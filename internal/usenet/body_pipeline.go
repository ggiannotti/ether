package usenet

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"time"

	"github.com/braedonsaunders/ether/internal/core"
	"github.com/braedonsaunders/ether/internal/yenc"
)

type bodyKey struct {
	id, group string
	prio      core.Priority
}
type bodyFlight struct {
	key         bodyKey
	ctx         context.Context
	cancel      context.CancelFunc
	done        chan struct{}
	waiters     int
	shared      bool
	retrySolo   bool // replay an unreceived tail without another predecessor
	started     bool // BODY window flushed; guarded by bodyMu
	liveWaiters int  // urgent callers sharing a transmitted speculative flight
	part        *yenc.Part
	err         error
}

// body coalesces requests at the same priority. Live requests may also join an
// already transmitted speculative BODY, but bypass queued speculative copies.
// Active sharing uses the connection's existing idle and command deadlines; it
// does not impose the stream layer's queued-work hedge delay on an active transfer.
// Each waiter can cancel independently; the last waiter cancels the work.
// Admission bounds queued metadata independently of connection count.
func (p *pool) body(ctx context.Context, id string, groups []string, prio core.Priority) (*yenc.Part, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	prio = clampPriority(prio)
	key := bodyKey{id: id, prio: prio}
	if p.o.UseGroup && len(groups) > 0 {
		key.group = groups[0]
	}
	// Joining transmitted work does not need another admission slot. In
	// particular, a speculative queue cannot delay this inexpensive join.
	p.bodyMu.Lock()
	if f := p.joinBodyLocked(key); f != nil {
		p.bodyMu.Unlock()
		return p.waitBody(ctx, key, f)
	}
	p.bodyMu.Unlock()
	if err := p.bodyAdmission.Acquire(ctx, prio); err != nil {
		return nil, err
	}
	p.bodyMu.Lock()
	f := p.joinBodyLocked(key)
	if f != nil {
		p.bodyAdmission.Release(prio)
	} else {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			p.bodyMu.Unlock()
			p.bodyAdmission.Release(prio)
			return nil, ErrPoolClosed
		}
		base := context.Background()
		if until := burstUntil(ctx); time.Now().Before(until) {
			base = context.WithValue(base, burstKey{}, until)
		}
		fctx, cancel := context.WithCancel(base)
		f = &bodyFlight{key: key, ctx: fctx, cancel: cancel, done: make(chan struct{}), waiters: 1}
		p.bodyFlights[key] = f
		p.bodyQueue[prio] = append(p.bodyQueue[prio], f)
		if p.bodyWorkers[prio] < p.targetLocked() {
			p.bodyWorkers[prio]++
			p.bodyWG.Add(1)
			go p.bodyWorker(prio)
		}
		p.mu.Unlock()
	}
	p.bodyMu.Unlock()
	return p.waitBody(ctx, key, f)
}

// joinBodyLocked never joins another priority until its commands are on the
// socket. A live join is retained if the speculative caller abandons its read.
func (p *pool) joinBodyLocked(key bodyKey) *bodyFlight {
	f := p.bodyFlights[key]
	if f == nil || f.ctx.Err() != nil {
		f = nil
		if key.prio == core.PriorityLive {
			for prio := core.PriorityReadahead; prio <= core.PriorityPrefetch; prio++ {
				candidate := p.bodyFlights[bodyKey{id: key.id, group: key.group, prio: prio}]
				if candidate != nil && candidate.started && candidate.ctx.Err() == nil {
					f = candidate
					break
				}
			}
		}
	}
	if f != nil {
		f.waiters++
		f.shared = true
		if key.prio == core.PriorityLive && f.key.prio != core.PriorityLive {
			f.liveWaiters++
		}
	}
	return f
}

func (p *pool) waitBody(ctx context.Context, key bodyKey, f *bodyFlight) (*yenc.Part, error) {
	select {
	case <-ctx.Done():
		p.bodyMu.Lock()
		f.waiters--
		if key.prio == core.PriorityLive && f.key.prio != core.PriorityLive {
			f.liveWaiters--
		}
		if f.waiters == 0 {
			f.cancel()
		}
		p.bodyMu.Unlock()
		return nil, ctx.Err()
	case <-f.done:
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if f.shared && f.part != nil {
			// Consumers historically own the returned bytes. Dedup must not introduce
			// aliasing between viewers, including one mutating while another reads.
			part := *f.part
			part.Data = bytes.Clone(part.Data)
			return &part, f.err
		}
		return f.part, f.err
	}
}

func (p *pool) completeBody(f *bodyFlight, part *yenc.Part, err error) {
	p.bodyMu.Lock()
	if p.bodyFlights[f.key] == f {
		delete(p.bodyFlights, f.key)
	}
	f.part, f.err = part, err
	f.cancel()
	close(f.done)
	p.bodyMu.Unlock()
	p.bodyAdmission.Release(f.key.prio)
}

func (p *pool) bodyWorker(prio core.Priority) {
	defer p.bodyWG.Done()
	claimed := false
	for {
		p.bodyMu.Lock()
		if claimed {
			p.bodyClaimed[prio]--
			claimed = false
		}
		q := p.bodyQueue[prio]
		if len(q) == 0 {
			p.bodyWorkers[prio]--
			p.bodyMu.Unlock()
			return
		}
		f := q[0]
		q[0] = nil
		p.bodyQueue[prio] = q[1:]
		p.bodyClaimed[prio]++
		claimed = true
		p.bodyMu.Unlock()
		if err := f.ctx.Err(); err != nil {
			p.completeBody(f, nil, err)
			continue
		}
		c, err := p.get(f.ctx, prio)
		if err != nil {
			p.completeBody(f, nil, err)
			continue
		}
		batch := []*bodyFlight{f}
		depth := p.o.BodyPipelineDepth
		if prio == core.PriorityPrefetch || c.recoveryProbe || f.retrySolo {
			depth = 1
		}
		// Never add speculative work to an occupied socket while more urgent work
		// waits. Already transmitted commands remain bounded and are not preempted.
		p.bodyMu.Lock()
		ss := p.sem.stats()
		if prio == core.PriorityReadahead {
			if !time.Now().Before(burstUntil(f.ctx)) {
				depth = min(depth, 2)
			}
			if ss.Size < 2 {
				depth = 1
			}
		}
		admission := p.bodyAdmission.stats()
		for higher := core.PriorityLive; higher < prio; higher++ {
			if len(p.bodyQueue[higher]) > 0 || ss.Waiting[higher] > 0 || admission.Waiting[higher] > 0 {
				depth = 1
			}
		}
		q = p.bodyQueue[prio]
		// Workers spawned but not yet holding a flight are pending siblings:
		// if a socket is free for one of them, leave queued flights for it to
		// serve in parallel rather than pipelining them onto this socket,
		// where they would only be answered serially behind this batch. Only
		// flights beyond that spare parallel capacity are pipelined here.
		spare := min(p.bodyWorkers[prio]-p.bodyClaimed[prio], p.sem.available(prio, burstUntil(f.ctx)))
		extra := len(q) - spare
		for i := 0; i < len(q) && len(batch) < depth && len(batch) <= extra; {
			if !q[i].retrySolo && q[i].key.group == f.key.group && q[i].ctx.Err() == nil {
				batch = append(batch, q[i])
				q = slices.Delete(q, i, i+1)
			} else {
				i++
			}
		}
		p.bodyQueue[prio] = q
		p.bodyMu.Unlock()
		p.runBodyBatch(c, batch, prio)
	}
}

func (p *pool) runBodyBatch(c *Conn, batch []*bodyFlight, prio core.Priority) {
	reused := c.uses > 0 || !c.idleSince.IsZero()
	c.uses += len(batch)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var alive atomic.Int32
	alive.Store(int32(len(batch)))
	stops := make([]func() bool, 0, len(batch))
	for _, f := range batch {
		stops = append(stops, context.AfterFunc(f.ctx, func() {
			if alive.Add(-1) == 0 {
				cancel()
			}
		}))
	}
	defer func() {
		for _, stop := range stops {
			stop()
		}
	}()
	start := time.Now()
	if group := batch[0].key.group; group != "" && c.CurrentGroup() != group {
		if err := c.Group(ctx, group); err != nil && !errors.Is(err, ErrNoSuchGroup) {
			err = p.failed(err, reused, ctx)
			p.put(c, prio)
			for _, f := range batch {
				p.completeBody(f, nil, err)
			}
			return
		}
	}
	ids := make([]string, len(batch))
	for i, f := range batch {
		ids[i] = f.key.id
	}
	p.bodyBatches.Add(1)
	if len(batch) > 1 {
		p.bodyPipelined.Add(int64(len(batch)))
	}
	// One broken transport counts once toward the breaker, however many BODY
	// replies it strands. Missing or corrupt articles are isolated per reply.
	networkRecorded := false
	var lastPart *yenc.Part
	var lastErr error
	var replay []*bodyFlight
	lastReplayed := false
	c.bodyPipelineStarted(ctx, ids, &yenc.DecodeOptions{MaxSize: p.o.MaxArticleSize}, func() {
		p.bodyMu.Lock()
		for _, f := range batch {
			f.started = true
		}
		p.bodyMu.Unlock()
	}, func(i int, part *yenc.Part, err error) {
		f := batch[i]
		if err != nil {
			p.bodyMu.Lock()
			f.started = false
			p.bodyMu.Unlock()
		}
		if errors.Is(err, errPipelineUnanswered) && !f.retrySolo && f.ctx.Err() == nil {
			f.retrySolo = true
			replay = append(replay, f)
			lastReplayed = i == len(batch)-1
			return
		}
		if err != nil {
			if errors.Is(err, ErrNoSuchArticle) {
				p.bodyMisses.Add(1)
				var pe *ProtocolError
				if errors.As(err, &pe) && pe.Code == 451 {
					p.bodyTakedowns.Add(1)
				}
			}
			network := isNetworkError(err) || errors.Is(err, ErrServiceUnavailable) || errors.Is(err, ErrConnBroken)
			if !network || !networkRecorded {
				// A mid-body drop says nothing about idle siblings. Only the
				// first reply on a reused socket can expose a stale idle pool.
				err = p.failed(err, reused && i == 0 && !c.bodyStarted, ctx)
				if network {
					networkRecorded = true
				}
			}
		} else {
			p.brk.success()
			p.articles.Add(1)
			p.latMu.Lock()
			p.latency.add(c.lastRTT)
			p.fetching.add(time.Since(start))
			p.latMu.Unlock()
		}
		if f.ctx.Err() != nil {
			err = f.ctx.Err()
			part = nil
		}
		if i == len(batch)-1 {
			lastPart, lastErr = part, err
		} else {
			p.completeBody(f, part, err)
		}
	})
	p.put(c, prio)
	if len(replay) > 0 {
		p.requeueBodies(replay, prio)
	}
	if !lastReplayed {
		p.completeBody(batch[len(batch)-1], lastPart, lastErr)
	}
}

// Resizing can expose more connection capacity without another incoming request.
// Wake existing queued work immediately rather than keeping it on the old number
// of socket workers until the next request happens to arrive.
func (p *pool) wakeBodyWorkers() {
	p.bodyMu.Lock()
	defer p.bodyMu.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.startBodyWorkersLocked()
}

// bodyMu and mu must be held; Add stays serialized with pool close/Wait.
func (p *pool) startBodyWorkersLocked() {
	for prio := core.PriorityLive; prio <= core.PriorityPrefetch; prio++ {
		n := min(len(p.bodyQueue[prio]), p.targetLocked()-p.bodyWorkers[prio])
		for i := 0; i < n; i++ {
			p.bodyWorkers[prio]++
			p.bodyWG.Add(1)
			go p.bodyWorker(prio)
		}
	}
}

func (p *pool) hasBodyWork() bool {
	p.bodyMu.Lock()
	defer p.bodyMu.Unlock()
	return len(p.bodyFlights) > 0 || p.bodyWorkers[0]+p.bodyWorkers[1]+p.bodyWorkers[2] > 0
}

func (p *pool) cancelBodies() {
	p.bodyMu.Lock()
	for _, f := range p.bodyFlights {
		f.cancel()
	}
	p.bodyMu.Unlock()
}

func (p *pool) abortBodies() {
	p.cancelBodies()
	p.bodyWG.Wait()
}

// A speculative tail that acquired a live waiter must replay at live priority;
// losing its predecessor cannot silently demote that viewer back into a queue.
func (p *pool) requeueBodies(replay []*bodyFlight, prio core.Priority) {
	p.bodyMu.Lock()
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		p.bodyMu.Unlock()
		for _, f := range replay {
			p.completeBody(f, nil, ErrPoolClosed)
		}
		return
	}
	var queues [numPriorities][]*bodyFlight
	for _, f := range replay {
		priority := prio
		if f.liveWaiters > 0 {
			priority = core.PriorityLive
		}
		queues[priority] = append(queues[priority], f)
	}
	for priority, q := range queues {
		if len(q) > 0 {
			p.bodyQueue[priority] = append(q, p.bodyQueue[priority]...)
		}
	}
	p.startBodyWorkersLocked()
	p.mu.Unlock()
	p.bodyMu.Unlock()
}
