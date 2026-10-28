package stream

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/braedonsaunders/ether/internal/core"
)

// flightKey isolates the three scheduling lanes for an article. A viewer must
// never inherit a queued speculative fetch's lower connection-pool priority.
// Completed bytes still share the ordinary hot/disk segment key.
type flightKey struct {
	segKey
	priority core.Priority
}

// flight is one in-progress segment fetch that same-priority requests join.
type flight struct {
	decoded chan struct{} // validated bytes ready; independent of cache persistence
	stored  chan struct{} // completed any cache persistence, for durable prefetch waiters
	cacheID string        // cache namespace owned by this flight
	done    chan struct{}
	seg     *segData
	err     error
	waiters int
	cancel  context.CancelFunc
}

// fetchOpts says where a fetched segment may be stored.
type fetchOpts struct {
	repairProgress func(*postedFile, int, core.RepairEvent) core.RepairWindow
	burstUntil     time.Time
	parallel       int         // independent bulk-reader window concurrency
	repairValid    func() bool // readahead cursor still needs this work
	deferObserve   bool        // readFile observes assembled segments in file order
	noRepair       bool        // dependency reads must never recursively enter repair
	cacheID        string      // release ID for the disk cache; "" = don't use it
	noHot          bool        // don't fill the hot buffer (background prefetch)
}

// segmentRaw returns slot i of pf: from the hot buffer, the disk cache, or
// Usenet, coalescing concurrent requests. The slot's index entry is
// updated from the article's yEnc header.
func (e *Engine) segmentRaw(ctx context.Context, rel *release, pf *postedFile, i int, prio core.Priority, fo fetchOpts) (*segData, error) {
	if core.IsBackgroundCheck(ctx) {
		prio = core.PriorityPrefetch
		fo.burstUntil = time.Time{}
	}
	if prio == core.PriorityLive {
		if seg, handled, err := e.hedgeSpeculative(ctx, rel, pf, i, fo); handled {
			return seg, err
		}
	}
	for {
		seg, abandoned, err := e.segmentOnce(ctx, rel, pf, i, prio, fo)
		if !abandoned {
			return seg, err
		}
		// The flight was abandoned by its other waiters just as this request
		// joined it; fetch again on our own behalf. segmentOnce reports
		// abandonment only after that flight ended, so each retry makes
		// progress; the abandoned flight's cancellation is not ours to report.
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
}

func (e *Engine) segmentOnce(ctx context.Context, rel *release, pf *postedFile, i int, prio core.Priority, fo fetchOpts) (_ *segData, abandoned bool, _ error) {
	id := pf.msgID(i)

	key := segKey{rel.digest, pf.segBase + i}
	if seg, ok := e.hot.get(key); ok {
		e.hotHits.Add(1)
		e.learnHot(rel, pf, i, seg)
		return seg, false, nil
	}

	if id == "" {
		if fo.cacheID != "" && e.cache != nil {
			if seg, ok := e.cachedSegment(fo.cacheID, key.seg); ok {
				e.learnHot(rel, pf, i, seg)
				return seg, false, nil
			}
		}

		e.missing.Add(1)
		return nil, false, fmt.Errorf("stream: %s part %d not in nzb: %w", pf.Name(), i+1, core.ErrArticleMissing)
	}
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil, false, ErrClosed
	}
	// A flight finishing between the lookup above and taking the lock has
	// already filled the hot buffer (it does so before leaving the map).
	if seg, ok := e.hot.get(key); ok {
		e.mu.Unlock()
		e.hotHits.Add(1)
		e.learnHot(rel, pf, i, seg)
		return seg, false, nil
	}
	// Bound even malformed callers to the three supported priority lanes.
	prio = min(max(prio, core.PriorityLive), core.PriorityPrefetch)
	fkey := flightKey{segKey: key, priority: prio}
	fl, joined := e.flights[fkey]
	// Speculation can safely share more urgent work. Prefer Live, then
	// Readahead; never launch a duplicate lower lane behind an active higher
	// lane. A durable prefetch needs that flight to write its cache namespace.
	for higher := core.PriorityLive; higher < prio; higher++ {
		hkey := flightKey{segKey: key, priority: higher}
		if candidate := e.flights[hkey]; candidate != nil && candidate.waiters > 0 &&
			(!fo.noHot || e.cache == nil || candidate.cacheID == fo.cacheID) {
			fkey, fl, joined = hkey, candidate, true
			break
		}
	}
	if joined && fl.waiters == 0 {
		// The last waiter canceled this lane. Keep its tombstone until the
		// worker exits: otherwise repeated cancels could spawn unbounded
		// overlapping work for one article while cancellation propagates.
		e.mu.Unlock()
		select {
		case <-fl.done:
			return nil, true, context.Canceled
		case <-ctx.Done():
			return nil, false, ctx.Err()
		}
	}
	if joined {
		fl.waiters++
		e.coalesced.Add(1)
	} else {
		fctx, cancel := context.WithCancel(core.InheritBackground(e.ctx, ctx))
		fl = &flight{decoded: make(chan struct{}), stored: make(chan struct{}), done: make(chan struct{}), cacheID: fo.cacheID, waiters: 1, cancel: cancel}
		e.flights[fkey] = fl
		e.wg.Add(1)
		go e.run(fctx, fl, fkey, rel, pf, i, id, prio, fo)
	}
	e.mu.Unlock()

	ready := fl.done
	if fo.noHot {
		ready = fl.stored
	}
	select {
	case <-ready:
		if fl.err != nil && errors.Is(fl.err, context.Canceled) && ctx.Err() == nil && e.ctx.Err() == nil {
			return nil, true, fl.err
		}
		return fl.seg, false, fl.err
	case <-ctx.Done():
		e.mu.Lock()
		fl.waiters--
		last := fl.waiters == 0
		if last {
			// Independent priority lanes remain alive for their own waiters.
			// run removes this canceled lane only after its worker exits.
			fl.cancel()
		}
		e.mu.Unlock()
		// A health call owns its canceled work through termination, so it
		// cannot spend bytes after reporting completion. Shared live work
		// and ordinary readers retain independent, prompt cancellation.
		if last && ctx.Value(healthWorkKey{}) == true {
			<-fl.done
		}
		return nil, false, ctx.Err()
	}
}

// liveHedgeDelay bounds the latency a live reader may inherit from an
// already queued speculative read. Healthy reads often finish inside this
// window, saving a duplicate article without sacrificing a reserved live lane.
const liveHedgeDelay = 250 * time.Millisecond

func (e *Engine) hedgeSpeculative(ctx context.Context, rel *release, pf *postedFile, i int, fo fetchOpts) (*segData, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, true, err
	}
	key := segKey{rel.digest, pf.segBase + i}
	if seg, ok := e.hot.get(key); ok {
		e.hotHits.Add(1)
		e.learnHot(rel, pf, i, seg)
		return seg, true, nil
	}
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil, true, ErrClosed
	}
	// Existing live work and cached data take precedence over speculation.
	if e.hot.has(key) || e.flights[flightKey{key, core.PriorityLive}] != nil {
		e.mu.Unlock()
		return nil, false, nil
	}
	var lower *flight
	for p := core.PriorityReadahead; p <= core.PriorityPrefetch; p++ {
		if fl := e.flights[flightKey{key, p}]; fl != nil && fl.waiters > 0 {
			lower = fl
			break
		}
	}
	if lower == nil {
		e.mu.Unlock()
		return nil, false, nil
	}
	lower.waiters++
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		lower.waiters--
		if lower.waiters == 0 {
			lower.cancel()
		}
		e.mu.Unlock()
	}()
	timer := time.NewTimer(e.hedgeDelay)
	defer timer.Stop()
	select {
	case <-lower.decoded:
	case <-timer.C:
		// Prefer decoded bytes if completion and the hedge deadline raced.
		select {
		case <-lower.decoded:
		default:
			return nil, false, nil
		}
	case <-ctx.Done():
		return nil, true, ctx.Err()
	case <-e.ctx.Done():
		return nil, true, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, true, err
	}
	if lower.err != nil {
		return nil, false, nil
	}
	// The decoded channel publishes an immutable result; persistence never
	// modifies it. noHot applies to the speculative owner, not a live consumer.
	if !fo.noHot {
		e.hot.put(key, lower.seg)
	}
	e.learnHot(rel, pf, i, lower.seg)
	e.coalesced.Add(1)
	return lower.seg, true, nil
}

// learnHot records a hot-buffer segment's exact range in pf. Usually the
// fetch that filled the buffer taught this release state already, but hot
// entries are keyed by NZB digest and can outlive the state that fetched
// them (a fetch finishing after its release was trimmed), so a fresh state
// for the same NZB would otherwise locate offsets from estimates forever.
func (e *Engine) learnHot(rel *release, pf *postedFile, i int, seg *segData) {
	if seg.repairedSpan || pf.slotKnown(i) {
		return
	}
	pf.learn(i, seg.begin, seg.end(), seg.fileSize, seg.total)
	rel.notePartSize(pf)
}

func (e *Engine) run(ctx context.Context, fl *flight, key flightKey, rel *release, pf *postedFile, i int, id string, prio core.Priority, fo fetchOpts) {
	defer e.wg.Done()
	defer close(fl.stored)
	defer fl.cancel()
	seg, fromCache, err := e.load(ctx, pf, i, id, key.seg, prio, fo)
	if err == nil {
		if !seg.repairedSpan {
			pf.learn(i, seg.begin, seg.end(), seg.fileSize, seg.total)
			rel.notePartSize(pf)
		}
		if !fo.noHot {
			e.hot.put(key.segKey, seg)
		}
	}
	// Publish normalized data before any slow disk write. A live hedge may
	// consume/promote these bytes while a noHot owner awaits durable storage.
	e.mu.Lock()
	fl.seg, fl.err = seg, err
	e.mu.Unlock()
	close(fl.decoded)
	persist := func() {
		if err == nil && !fromCache && fo.cacheID != "" && e.cache != nil {
			if perr := e.cache.Put(fo.cacheID, key.seg, encodeSegment(seg)); perr != nil {
				e.log.Debug("segment cache put failed", "release", fo.cacheID, "segment", key.seg, "err", perr)
			}
		}
	}
	if fo.noHot {
		// Background prefetch exists to fill the disk cache: finish the
		// write before reporting success.
		persist()
	}
	e.mu.Lock()
	if e.flights[key] == fl {
		delete(e.flights, key)
	}
	e.mu.Unlock()
	close(fl.done)
	if !fo.noHot {
		// Readers are not kept waiting for the disk.
		persist()
	}
}

// load reads a segment from the disk cache or fetches it, with retries for
// transient errors.
func (e *Engine) load(ctx context.Context, pf *postedFile, i int, id string, global int, prio core.Priority, fo fetchOpts) (*segData, bool, error) {
	if fo.cacheID != "" && e.cache != nil {
		if seg, ok := e.cachedSegment(fo.cacheID, global); ok {
			e.cacheHits.Add(1)
			return seg, true, nil
		}
	}
	var lastErr error
	for attempt := 0; attempt <= e.opts.Retries; attempt++ {
		if attempt > 0 {
			t := time.NewTimer(time.Duration(attempt) * 150 * time.Millisecond)
			select {
			case <-ctx.Done():
				t.Stop()
				return nil, false, ctx.Err()
			case <-t.C:
			}
		}
		var data []byte
		var part core.ArticlePart
		var err error
		if burst, ok := e.fetcher.(core.BurstArticleFetcher); ok && time.Now().Before(fo.burstUntil) {
			data, part, err = burst.BodyPartBurst(ctx, id, pf.groups, prio, fo.burstUntil)
		} else {
			data, part, err = e.fetcher.BodyPart(ctx, id, pf.groups, prio)
		}
		if err == nil {
			e.repairRate.add(int64(len(data)))
			e.fetches.Add(1)
			e.fetchedBytes.Add(int64(len(data)))
			seg, err := e.normalize(pf, i, data, part)
			return seg, false, err
		}
		if errors.Is(err, core.ErrBudgetExhausted) {
			return nil, false, err
		}
		if errors.Is(err, core.ErrArticleMissing) {
			e.missing.Add(1)
			return nil, false, fmt.Errorf("stream: %s part %d <%s>: %w", pf.Name(), i+1, id, err)
		}
		if ctx.Err() != nil {
			return nil, false, ctx.Err()
		}
		lastErr = err
		e.log.Debug("article fetch failed", "file", pf.Name(), "part", i+1, "attempt", attempt+1, "err", err)
	}
	return nil, false, fmt.Errorf("stream: %s part %d: %w", pf.Name(), i+1, lastErr)
}

// maxArticleBytes bounds plausible decoded article ranges.
// Real Usenet articles are well under a few MiB.
const maxArticleBytes = 32 << 20

// normalize places decoded article bytes in the posted file using the yEnc
// header, tolerating headers that are absent or inconsistent.
func (e *Engine) normalize(pf *postedFile, i int, data []byte, part core.ArticlePart) (*segData, error) {
	if part.Begin >= 1 && part.End >= part.Begin && part.End-part.Begin+1 > max(int64(len(data)), maxArticleBytes) {
		return nil, fmt.Errorf("stream: %s part %d implausible yEnc range %d-%d: %w", pf.Name(), i+1, part.Begin, part.End, ErrCorrupt)
	}
	seg := &segData{fileSize: part.FileSize, total: part.Total, name: part.Name}
	if part.Begin >= 1 && part.End >= part.Begin {
		seg.begin = part.Begin - 1
		want := part.End - part.Begin + 1
		if int64(len(data)) != want {
			return nil, fmt.Errorf("stream: %s part %d decoded length %d differs from yEnc range %d: %w", pf.Name(), i+1, len(data), want, ErrCorrupt)
		}
		seg.rangeExact = part.Part == i+1 && part.Total >= part.Part && part.FileSize >= part.End
	} else {
		// No part header: trust the index estimate.
		seg.begin, _, _ = pf.rangeOf(i)
		if seg.fileSize == 0 && pf.numSlots() == 1 {
			seg.fileSize = int64(len(data))
		}
	}
	seg.data = data
	return seg, nil
}

// Disk cache encoding: a small header so a cached segment restores its
// position in the posted file without the article.
//
//	"ETS2" | begin u64 | fileSize u64 | total u32 | nameLen u16 | flags u8 | name | data
//
// Multi-article repaired spans use ETS3 so older binaries ignore those entries.
const segMagic = "ETS2"

var (
	segMagicBytes  = []byte(segMagic)
	segMagic3Bytes = []byte("ETS3")
)

func encodeSegment(s *segData) []byte {
	name := s.name
	if len(name) > 1024 {
		name = name[:1024]
	}
	b := make([]byte, 4+8+8+4+2+1+len(name)+len(s.data))
	copy(b, segMagic)
	binary.LittleEndian.PutUint64(b[4:], uint64(s.begin))
	binary.LittleEndian.PutUint64(b[12:], uint64(s.fileSize))
	binary.LittleEndian.PutUint32(b[20:], uint32(s.total))
	binary.LittleEndian.PutUint16(b[24:], uint16(len(name)))
	if s.rangeExact {
		b[26] = 1
	}
	if s.repairedSpan {
		copy(b, "ETS3")
		b[26] |= 2
	}
	copy(b[27:], name)
	copy(b[27+len(name):], s.data)
	return b
}

// spanRefMagic marks a disk-cache record that points at the slot holding a
// repaired span, so a span covering k slots is stored once rather than k
// times. The reference carries the span's offset and length, so a later
// ordinary segment written over the target slot cannot be mistaken for it.
const spanRefMagic = "ETR1"

var spanRefMagicBytes = []byte(spanRefMagic)

func encodeSpanRef(target int, s *segData) []byte {
	b := make([]byte, 4+8+8+8)
	copy(b, spanRefMagic)
	binary.LittleEndian.PutUint64(b[4:], uint64(target))
	binary.LittleEndian.PutUint64(b[12:], uint64(s.begin))
	binary.LittleEndian.PutUint64(b[20:], uint64(len(s.data)))
	return b
}

// cachedSegment reads global from the disk cache, following a repaired-span
// reference to the slot that stores the span.
func (e *Engine) cachedSegment(cacheID string, global int) (*segData, bool) {
	b, ok := e.cache.Get(cacheID, global)
	if !ok {
		return nil, false
	}
	if len(b) != 28 || !bytes.Equal(b[:4], spanRefMagicBytes) {
		return decodeSegment(b)
	}
	target := int(binary.LittleEndian.Uint64(b[4:]))
	begin := int64(binary.LittleEndian.Uint64(b[12:]))
	n := int(binary.LittleEndian.Uint64(b[20:]))
	if target == global {
		return nil, false
	}
	if b, ok = e.cache.Get(cacheID, target); !ok {
		return nil, false
	}
	seg, ok := decodeSegment(b)
	if !ok || !seg.repairedSpan || seg.begin != begin || len(seg.data) != n {
		return nil, false
	}
	return seg, true
}

func decodeSegment(b []byte) (*segData, bool) {
	if len(b) < 27 || (!bytes.Equal(b[:4], segMagicBytes) && !bytes.Equal(b[:4], segMagic3Bytes)) {
		return nil, false
	}
	begin := int64(binary.LittleEndian.Uint64(b[4:]))
	size := int64(binary.LittleEndian.Uint64(b[12:]))
	total := int(binary.LittleEndian.Uint32(b[20:]))
	nl := int(binary.LittleEndian.Uint16(b[24:]))
	if begin < 0 || size < 0 || 27+nl > len(b) {
		return nil, false
	}
	return &segData{begin: begin, fileSize: size, total: total, name: string(b[27 : 27+nl]), data: b[27+nl:], rangeExact: b[26]&1 != 0, repairedSpan: b[26]&2 != 0}, true
}

// readFile fills p from posted file pf at off, fetching the covering
// segments in parallel. It returns the bytes copied; n < len(p) only with
// an error (io.EOF at the end of the file).
func (e *Engine) readFile(ctx context.Context, rel *release, pf *postedFile, p []byte, off int64, prio core.Priority, fo fetchOpts) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	ctx, cancel := context.WithCancel(ctx)
	var workers sync.WaitGroup
	// Join the bounded queue on every return, including health cancellation.
	// Unscheduled slots have no worker/result to drain after cancellation.
	defer func() { cancel(); workers.Wait() }()
	fo.deferObserve = true
	size := pf.Size()
	if off >= size && pf.SizeKnown() {
		return 0, errEOF
	}
	// Prefetch every covering slot in parallel; assembly below waits on them.
	slots := pf.slotsFor(off, int64(len(p)))
	type res struct {
		seg *segData
		err error
	}
	results := make(map[int]chan res, len(slots))
	if len(slots) > 1 {
		parallel := e.opts.ReadParallel
		if fo.parallel > 0 {
			parallel = fo.parallel
		}
		// Keep goroutine count bounded even for tiny/malformed NZB segments.
		for _, i := range slots {
			results[i] = make(chan res, 1)
		}
		jobs := make(chan int)
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer close(jobs)
			for _, i := range slots {
				select {
				case jobs <- i:
				case <-ctx.Done():
					return
				}
			}
		}()
		// Workers capture their channels before assembly starts deleting map
		// entries; the immutable queue avoids concurrent access to results.
		channels := make(map[int]chan res, len(results))
		for i, ch := range results {
			channels[i] = ch
		}
		for range min(parallel, len(slots)) {
			workers.Add(1)
			go func() {
				defer workers.Done()
				for i := range jobs {
					seg, err := e.segment(ctx, rel, pf, i, prio, fo)
					channels[i] <- res{seg, err}
				}
			}()
		}
	}
	get := func(i int) (*segData, error) {
		if ch, ok := results[i]; ok {
			delete(results, i)
			select {
			case r := <-ch:
				return r.seg, r.err
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return e.segment(ctx, rel, pf, i, prio, fo)
	}

	n := 0
	cur := off
	misses := 0
	for n < len(p) {
		if pf.SizeKnown() && cur >= pf.Size() {
			return n, errEOF
		}
		i := pf.locate(cur)
		seg, err := get(i)
		if err != nil {
			return n, err
		}
		if cur < seg.begin || cur >= seg.end() {
			// The estimate was wrong; the index has been corrected by the
			// fetch, so locate again.
			misses++
			if misses > 8 {
				return n, fmt.Errorf("stream: %s: offset %d not found in part %d [%d,%d): %w", pf.Name(), cur, i+1, seg.begin, seg.end(), ErrCorrupt)
			}
			if i == pf.numSlots()-1 && cur >= seg.end() {
				return n, errEOF
			}
			continue
		}
		if !fo.noRepair && e.repairOptions().Enabled {
			e.observeRepair(rel, pf, seg)
		}
		c := copy(p[n:], seg.data[cur-seg.begin:])
		n += c
		cur += int64(c)
	}
	return n, nil
}
