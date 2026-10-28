package usenet

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/braedonsaunders/ether/internal/config"
	"github.com/braedonsaunders/ether/internal/core"
)

// pool holds the connections of one provider. At most target() connections
// exist at any time (cfg.Connections, or this node's share of them in a
// cluster): every connection is either idle or held by the request that
// owns one of the priority semaphore's slots, and new connections are only
// dialled by a slot holder when no idle one is left. After the target
// shrinks, connections above it close as they are returned.
type pool struct {
	bodyMu        sync.Mutex
	bodyWG        sync.WaitGroup
	bodyQueue     [numPriorities][]*bodyFlight
	bodyWorkers   [numPriorities]int
	bodyClaimed   [numPriorities]int // workers holding a dequeued flight
	bodyFlights   map[bodyKey]*bodyFlight
	bodyAdmission *prioSem
	bodyBatches   atomic.Int64
	bodyPipelined atomic.Int64
	key           string // unique per pool instance (provider id + generation)
	cfg           config.UsenetProvider
	o             *Options
	log           *slog.Logger
	sem           *prioSem
	brk           *breaker

	mu     sync.Mutex
	warmMu sync.Mutex
	idle   []*Conn // LIFO: the most recently used connection is reused first
	live   int     // idle + in use + being dialled
	closed bool
	// size is the configured connection count; limitedUntil is set while
	// the semaphore runs smaller because the server refused connections.
	size         int
	limitedUntil time.Time
	// Warm-floor background redial state (guarded by mu): at most one
	// warmLoop runs at a time, and after a failed dial it waits until
	// warmNext (exponential backoff) before trying again.
	warmActive bool
	warmFails  int
	warmNext   time.Time
	// share, when positive, is this node's part of size in a cluster (the
	// account's connection limit is divided among live nodes).
	share int

	// minIdle is this pool's effective warm floor setting (per-provider
	// override or the fetcher-wide value); negative disables the floor.
	minIdle atomic.Int32

	ctx    context.Context // cancelled by close; parent of background dials
	cancel context.CancelFunc
	stop   chan struct{}
	wg     sync.WaitGroup

	bytes         atomic.Int64
	articles      atomic.Int64
	misses        atomic.Int64
	backfillSaves atomic.Int64
	bodyMisses    atomic.Int64
	bodyTakedowns atomic.Int64
	errs          atomic.Int64
	dials         atomic.Int64
	connSeq       atomic.Int64
	tcpTime       atomic.Int64
	tlsTime       atomic.Int64
	greetingTime  atomic.Int64
	authTime      atomic.Int64
	statBatches   atomic.Int64
	statBatchTime atomic.Int64
	bgRedials     atomic.Int64 // connections dialled by the floor janitor
	redialFails   atomic.Int64 // failed background redials

	latMu    sync.Mutex
	latency  ewma // command round trip
	fetching ewma // whole article
}

type ewma struct {
	v float64
	n int64
}

func (e *ewma) add(d time.Duration) {
	const alpha = 0.1
	if e.n == 0 {
		e.v = float64(d)
	} else {
		e.v += alpha * (float64(d) - e.v)
	}
	e.n++
}

// effectiveMinIdle resolves a provider's warm floor: the provider's own
// MinIdle when set (nonzero), else the fetcher-wide value.
func effectiveMinIdle(cfg config.UsenetProvider, global int) int {
	if cfg.MinIdle != 0 {
		return cfg.MinIdle
	}
	return global
}

func newPool(key string, cfg config.UsenetProvider, o *Options, log *slog.Logger, globalMinIdle int) *pool {
	size := cfg.Connections
	if size <= 0 {
		size = DefaultConnections
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &pool{
		size:          size,
		bodyFlights:   make(map[bodyKey]*bodyFlight),
		bodyAdmission: newPrioSem(max(64, size*16), max(16, size*4)),
		key:           key,
		cfg:           cfg,
		o:             o,
		log:           log.With("provider", providerLabel(cfg)),
		sem:           newPrioSem(size, prefetchSlots(size, o.PrefetchFraction)),
		brk:           newBreaker(o.BackoffMin, o.BackoffMax, o.FailThreshold, time.Now),
		ctx:           ctx,
		cancel:        cancel,
		stop:          make(chan struct{}),
	}
	p.sem.reserveLive = true
	if o.AuthRetryMin > 0 {
		p.brk.authMin = o.AuthRetryMin
	}
	if o.AuthRetryMax > 0 {
		p.brk.authMax = max(o.AuthRetryMax, p.brk.authMin)
	}
	p.minIdle.Store(int32(effectiveMinIdle(cfg, globalMinIdle)))
	p.wg.Add(1)
	go p.janitor()
	return p
}

// setGlobalMinIdle applies a changed fetcher-wide floor to this pool
// (honouring its per-provider override).
func (p *pool) setGlobalMinIdle(global int) {
	p.minIdle.Store(int32(effectiveMinIdle(p.cfg, global)))
}

func providerLabel(cfg config.UsenetProvider) string {
	if cfg.Name != "" {
		return cfg.Name
	}
	if cfg.ID != "" {
		return cfg.ID
	}
	return cfg.Host
}

func (p *pool) dialOptions() DialOptions {
	connID := p.connSeq.Add(1)
	return DialOptions{
		Host:            p.cfg.Host,
		Port:            p.cfg.Port,
		TLS:             p.cfg.TLS,
		Username:        p.cfg.Username,
		Password:        p.cfg.Password,
		TLSConfig:       p.o.TLSConfig,
		Timeout:         p.o.DialTimeout,
		CommandTimeout:  p.o.CommandTimeout,
		IdleReadTimeout: p.o.IdleReadTimeout,
		DialContext:     p.o.DialContext,
		BytesRead:       &p.bytes,
		OnPhase: func(phase string, d time.Duration) {
			switch phase {
			case "tcp":
				p.tcpTime.Add(int64(d))
			case "tls":
				p.tlsTime.Add(int64(d))
			case "greeting":
				p.greetingTime.Add(int64(d))
			case "auth":
				p.authTime.Add(int64(d))
			}
		},
		OnStatus: func(line string) {
			if p.o.TraceProtocol {
				p.log.Debug("nntp status", "connection", connID, "line", line)
			}
		},
		OnCommand: func(line string) {
			if p.o.TraceProtocol {
				p.log.Debug("nntp command", "connection", connID, "line", line)
			}
		},
	}
}

// get acquires a slot at prio and returns a connection for it. The caller
// must hand the connection back with put using the same prio.
func (p *pool) get(ctx context.Context, prio core.Priority) (*Conn, error) {
	for try := 0; ; try++ {
		if err := p.brk.allow(); err != nil {
			return nil, err
		}
		if err := p.sem.Acquire(ctx, prio); err != nil {
			return nil, err
		}
		// A waiter may have queued before a different command opened the
		// breaker. Recheck even when reusing an idle socket.
		probe, err := p.brk.admit()
		if err != nil {
			p.sem.Release(prio)
			return nil, err
		}
		c, err := p.conn(ctx)
		if err == nil {
			c.recoveryProbe = probe
			return c, nil
		}
		if probe {
			p.brk.releaseProbe()
		}
		p.sem.Release(prio)
		if !errors.Is(err, errAtLimit) || try >= 3 {
			return nil, err
		}
		// The pool just shrank to what the server allows: wait for one of
		// the connections that do exist instead of failing.
	}
}

// errAtLimit marks a dial refused because the account is at its server-side
// connection limit while other connections of this pool are working.
var errAtLimit = errors.New("usenet: provider connection limit reached")

// shrinkTo lowers the pool's effective size to what the server accepted,
// for LimitCooloff, after which the configured size is tried again.
func (p *pool) shrinkTo(n int) {
	n = max(n, 1)
	p.mu.Lock()
	p.limitedUntil = time.Now().Add(p.o.LimitCooloff)
	p.mu.Unlock()
	if p.sem.setSize(n, p.o.PrefetchFraction) {
		p.log.Info("usenet: server allows fewer connections than configured; adapting", "configured", p.target(), "using", n, "retry_after", p.o.LimitCooloff)
	}
}

func (p *pool) conn(ctx context.Context) (*Conn, error) {
	// The floor is read once, outside p.mu: idleFloor takes the semaphore
	// lock, and the lock order elsewhere is semaphore before p.mu.
	floor := p.idleFloor()
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, ErrPoolClosed
		}
		if n := len(p.idle); n > 0 {
			c := p.idle[n-1]
			p.idle[n-1] = nil
			p.idle = p.idle[:n-1]
			expire := time.Since(c.idleSince) > p.o.IdleTimeout && p.live > floor
			p.mu.Unlock()
			if expire {
				p.discard(c)
				continue
			}
			return c, nil
		}
		p.mu.Unlock()
		break
	}
	return p.dialNew(ctx)
}

// dialNew dials, negotiates TLS and authenticates one fresh connection,
// accounting it in live. The caller must hold a semaphore slot.
func (p *pool) dialNew(ctx context.Context) (*Conn, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrPoolClosed
	}
	p.live++
	p.mu.Unlock()
	// Re-check the breaker: another request may have tripped it while we
	// waited for a slot.
	if err := p.brk.allow(); err != nil {
		p.dropLive()
		return nil, err
	}
	c, err := Dial(ctx, p.dialOptions())
	if err != nil {
		p.dropLive()
		switch {
		case ctxErr(ctx) != nil:
			return nil, ctxErr(ctx)
		case errors.Is(err, ErrAuthFailed):
			p.errs.Add(1)
			d := p.brk.authFailure(err)
			p.log.Error("usenet: provider rejected credentials; disabled until a login probe succeeds or its settings change", "err", err, "retry_in", d)
		case errors.Is(err, ErrServiceUnavailable) && p.liveCount() > 0:
			// The server is up and serving our other connections; the
			// account is just at its real connection limit. Shrink to it
			// rather than taking the provider out of rotation.
			p.brk.noteError(err)
			p.shrinkTo(p.liveCount())
			return nil, fmt.Errorf("%w: %w", errAtLimit, err)
		default:
			p.errs.Add(1)
			if d := p.brk.connFailure(err); d > 0 {
				p.log.Warn("usenet: connect failed; backing off", "err", err, "backoff", d)
			} else {
				p.log.Warn("usenet: connect failed", "err", err)
			}
		}
		return nil, err
	}
	p.dials.Add(1)
	return c, nil
}

func (p *pool) liveCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.live
}

func (p *pool) dropLive() {
	p.mu.Lock()
	p.live--
	p.mu.Unlock()
}

// put returns a connection and its slot. Broken connections are closed.
func (p *pool) put(c *Conn, prio core.Priority) {
	probe := c.recoveryProbe
	c.recoveryProbe = false
	if c.Broken() {
		p.discard(c)
	} else {
		p.mu.Lock()
		if p.closed || p.live > p.targetLocked() {
			// Closed, or above a target that shrank while c was in use.
			p.live--
			p.mu.Unlock()
			go c.Quit()
		} else {
			c.idleSince = time.Now()
			p.idle = append(p.idle, c)
			p.mu.Unlock()
		}
	}
	if probe {
		p.brk.releaseProbe()
	}
	p.sem.Release(prio)
}

func (p *pool) discard(c *Conn) {
	_ = c.Close()
	p.dropLive()
}

// discardIdle closes every idle connection. Used when a reused connection
// turns out to have been dropped by the server: its idle siblings are very
// likely dead too, and redialling lazily is cheaper than failing on each.
func (p *pool) discardIdle() {
	p.mu.Lock()
	idle := p.idle
	p.idle = nil
	p.live -= len(idle)
	p.mu.Unlock()
	for _, c := range idle {
		_ = c.Close()
	}
	if len(idle) > 0 {
		p.maintainFloor()
	}
}

// janitor closes connections idle beyond IdleTimeout, pings ones idle
// beyond KeepAlive so servers do not drop them silently, and maintains
// the warm floor by redialling in the background when live connections
// have fallen below it.
func (p *pool) janitor() {
	defer p.wg.Done()
	every := p.o.IdleTimeout / 2
	if p.o.KeepAlive > 0 && p.o.KeepAlive/2 < every {
		every = p.o.KeepAlive / 2
	}
	every = max(every, 50*time.Millisecond)
	// Establish the floor right away so the first Play after startup (or
	// after a config Update created this pool) does not pay a cold dial.
	p.maintainFloor()
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-p.stop:
			return
		case <-t.C:
			p.sweep()
		}
	}
}

func (p *pool) sweep() {
	if p.brk.authProbeDue() {
		p.wg.Add(1) // janitor holds a count, so this cannot race close's Wait
		go p.authProbe()
	}
	now := time.Now()
	p.mu.Lock()
	restore := !p.limitedUntil.IsZero() && now.After(p.limitedUntil)
	if restore {
		p.limitedUntil = time.Time{}
	}
	target := p.targetLocked()
	p.mu.Unlock()
	if restore && p.sem.setSize(target, p.o.PrefetchFraction) {
		p.log.Info("usenet: trying the configured connection count again", "connections", target)
	}
	floor := p.idleFloor()
	var expired []*Conn
	p.mu.Lock()
	keep := p.idle[:0]
	for _, c := range p.idle {
		if now.Sub(c.idleSince) > p.o.IdleTimeout && p.live-len(expired) > floor {
			expired = append(expired, c)
		} else {
			keep = append(keep, c)
		}
	}
	clear(p.idle[len(keep):])
	p.idle = keep
	p.live -= len(expired)
	p.mu.Unlock()
	for _, c := range expired {
		_ = c.Quit()
	}
	if p.o.KeepAlive > 0 {
		for i := 0; i < 4; i++ {
			if !p.keepAliveOne(now) {
				break
			}
		}
	}
	// A failed keepalive above may have discarded a floor connection, or
	// the server may have dropped idle sockets since the last sweep.
	p.maintainFloor()
}

// maintainFloor starts a background redial when this primary pool has
// fewer live connections than its warm floor. It never blocks: at most
// one warmLoop runs, dials happen outside locks at prefetch priority,
// and a provider in circuit-breaker backoff or past a failed redial's
// backoff deadline is left alone.
func (p *pool) maintainFloor() {
	floor := p.idleFloor()
	if floor == 0 {
		return
	}
	if err := p.brk.allow(); err != nil {
		return
	}
	p.mu.Lock()
	if p.closed || p.warmActive || p.live >= floor || time.Now().Before(p.warmNext) {
		p.mu.Unlock()
		return
	}
	p.warmActive = true
	p.wg.Add(1) // under mu: close sets closed under mu before it Waits
	p.mu.Unlock()
	go p.warmLoop()
}

// warmLoop dials until the floor is reached, the pool gets busy (no free
// prefetch slot — live traffic will create its own connections), a dial
// fails (back off), or the pool closes.
func (p *pool) warmLoop() {
	defer p.wg.Done()
	defer func() {
		p.mu.Lock()
		p.warmActive = false
		p.mu.Unlock()
	}()
	for {
		floor := p.idleFloor()
		if floor == 0 {
			return
		}
		if err := p.brk.allow(); err != nil {
			return
		}
		p.mu.Lock()
		done := p.closed || p.live >= floor || time.Now().Before(p.warmNext)
		p.mu.Unlock()
		if done {
			return
		}
		if !p.sem.TryAcquire(core.PriorityPrefetch) {
			return
		}
		c, err := p.dialNew(p.ctx)
		if err != nil {
			p.sem.Release(core.PriorityPrefetch)
			if p.ctx.Err() != nil {
				return
			}
			p.redialFails.Add(1)
			p.mu.Lock()
			p.warmFails++
			p.warmNext = time.Now().Add(p.warmBackoff(p.warmFails))
			p.mu.Unlock()
			p.log.Debug("usenet: background warm redial failed; backing off", "err", err)
			return
		}
		p.bgRedials.Add(1)
		p.mu.Lock()
		p.warmFails = 0
		p.warmNext = time.Time{}
		p.mu.Unlock()
		p.put(c, core.PriorityPrefetch)
	}
}

// warmBackoff is the delay before the next background redial after fails
// consecutive failures: BackoffMin doubling up to BackoffMax, plus jitter.
func (p *pool) warmBackoff(fails int) time.Duration {
	d := p.o.BackoffMin
	for i := 1; i < fails && d < p.o.BackoffMax; i++ {
		d = cappedDouble(d, p.o.BackoffMin, p.o.BackoffMax)
	}
	d = min(d, p.o.BackoffMax)
	if d > 0 {
		d += time.Duration(rand.Float64() * float64(d) / 4)
	}
	return d
}

// keepAliveOne pings the longest-idle connection if it has been idle beyond
// KeepAlive (pings do not reset the idle timeout). A ping needs a slot like any other use of a connection, so the
// connection limit holds; when the pool is busy it is skipped (busy pools
// keep their connections alive with traffic). It reports whether it pinged.
func (p *pool) keepAliveOne(now time.Time) bool {
	if !p.sem.TryAcquire(core.PriorityReadahead) {
		return false
	}
	defer p.sem.Release(core.PriorityReadahead)
	p.mu.Lock()
	// Request recency and ping recency differ. Find the oldest wire activity
	// rather than repeatedly pinging the bottom of the request LIFO stack.
	oldest := -1
	for i, c := range p.idle {
		if now.Sub(c.used) > p.o.KeepAlive && (oldest < 0 || c.used.Before(p.idle[oldest].used)) {
			oldest = i
		}
	}
	if p.closed || oldest < 0 {
		p.mu.Unlock()
		return false
	}
	c := p.idle[oldest]
	p.idle = slices.Delete(p.idle, oldest, oldest+1)
	p.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), p.o.CommandTimeout)
	_, err := c.Date(ctx)
	cancel()
	var pe *ProtocolError
	if err != nil && errors.As(err, &pe) && !c.Broken() {
		err = nil // DATE unsupported; the round trip still kept it alive
	}
	if err != nil {
		p.log.Debug("usenet: keepalive failed; closing idle connection", "err", err)
		p.discard(c)
		return true
	}
	p.mu.Lock()
	if p.closed {
		p.live--
		p.mu.Unlock()
		_ = c.Close()
		return true
	}
	// Just pinged, but still the least recently used for requests: keep it
	// at the bottom of the stack.
	p.idle = slices.Insert(p.idle, 0, c)
	p.mu.Unlock()
	return true
}

// close stops the pool. Waiters fail with ErrPoolClosed, idle connections
// are closed now and busy ones when they are returned.
func (p *pool) close() { p.closeMode(true) }

// closeMode permits a provider configuration reload to finish active article
// windows; explicit fetcher shutdown cancels and joins them.
func (p *pool) closeMode(abort bool) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		if abort {
			p.abortBodies()
		}
		return
	}
	p.closed = true
	idle := p.idle
	p.idle = nil
	p.live -= len(idle)
	p.mu.Unlock()
	p.cancel() // interrupt background dials
	if abort {
		p.cancelBodies()
	}
	p.bodyAdmission.Close()
	p.sem.Close()
	close(p.stop)
	p.wg.Wait()
	if abort {
		p.bodyWG.Wait()
	}
	for _, c := range idle {
		go c.Quit()
	}
}

// stat runs STAT on one connection.
func (p *pool) stat(ctx context.Context, id string) (bool, error) {
	c, err := p.get(ctx, core.PriorityLive)
	if err != nil {
		return false, err
	}
	reused := c.uses > 0 || !c.idleSince.IsZero()
	c.uses++
	ok, err := c.Stat(ctx, id)
	rtt := c.lastRTT
	p.put(c, core.PriorityLive)
	if err != nil {
		return false, p.failed(err, reused, ctx)
	}
	p.brk.success()
	if !ok {
		p.misses.Add(1)
	}
	p.latMu.Lock()
	p.latency.add(rtt)
	p.latMu.Unlock()
	return ok, nil
}

func (p *pool) statMany(ctx context.Context, ids []string) (map[string]bool, error) {
	result := make(map[string]bool, len(ids))
	if len(ids) == 0 {
		return result, nil
	}
	c, err := p.get(ctx, core.PriorityLive)
	if err != nil {
		return nil, err
	}
	reused := c.uses > 0 || !c.idleSince.IsZero()
	c.uses++
	started := time.Now()
	result, err = c.StatMany(ctx, ids)
	p.statBatches.Add(1)
	p.statBatchTime.Add(int64(time.Since(started)))
	rtt := c.lastRTT
	p.put(c, core.PriorityLive)
	if err != nil {
		return nil, p.failed(err, reused, ctx)
	}
	p.brk.success()
	for _, ok := range result {
		if !ok {
			p.misses.Add(1)
		}
	}
	p.latMu.Lock()
	p.latency.add(rtt)
	p.latMu.Unlock()
	return result, nil
}

// authProbe tries one login on a provider disabled by an authentication
// failure. Success re-enables it; another rejection doubles the delay to
// the next probe; any other error retries after the minimum delay.
func (p *pool) authProbe() {
	defer p.wg.Done()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-p.stop:
			cancel()
		case <-ctx.Done():
		}
	}()
	c, err := Dial(ctx, p.dialOptions())
	switch {
	case err == nil:
		p.dials.Add(1)
		p.brk.authRecovered()
		p.log.Info("usenet: provider accepted credentials again; re-enabled")
		go c.Quit()
	case ctx.Err() != nil:
		p.brk.authProbeInconclusive(err)
	case errors.Is(err, ErrAuthFailed):
		p.errs.Add(1)
		d := p.brk.authFailure(err)
		p.log.Warn("usenet: provider still rejects credentials", "err", err, "retry_in", d)
	default:
		p.brk.authProbeInconclusive(err)
		p.log.Debug("usenet: auth probe could not reach provider", "err", err)
	}
}

// warm opens and authenticates one connection and returns it to the idle pool.
func (p *pool) warm(ctx context.Context, n int) error {
	// Multiple concurrent health checks may all request a warm pool. Serialize
	// each provider's warming pass so they share the same bounded idle set.
	p.warmMu.Lock()
	defer p.warmMu.Unlock()
	// Every warming goroutine holds its slot until all have one, so never
	// ask for more than the semaphore has (it runs below target while the
	// server limits us): the extra requests would wait for slots their
	// siblings hold, until ctx ends.
	n = min(n, 4, p.target(), p.sem.stats().Size)
	if n <= 0 {
		return nil
	}
	type acquired struct {
		c   *Conn
		err error
	}
	ready := make(chan acquired, n)
	release := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := p.get(ctx, core.PriorityLive)
			ready <- acquired{c: c, err: err}
			if c != nil {
				<-release
				p.put(c, core.PriorityLive)
			}
		}()
	}
	var first error
	for i := 0; i < n; i++ {
		got := <-ready
		if got.err != nil && first == nil {
			first = got.err
		}
	}
	close(release)
	wg.Wait()
	return first
}

// failed classifies and records an error from a request on this pool.
func (p *pool) failed(err error, reused bool, ctx context.Context) error {
	switch {
	case ctxErr(ctx) != nil:
		return ctxErr(ctx)
	case errors.Is(err, ErrNoSuchArticle):
		p.brk.success()
		p.misses.Add(1)
	case errors.Is(err, ErrInvalidMessageID):
	default:
		p.errs.Add(1)
		if isNetworkError(err) || errors.Is(err, ErrServiceUnavailable) {
			p.brk.connFailure(err)
		} else {
			p.brk.noteError(err)
		}
		// EOF or a reset on a connection that had been idle means the
		// server dropped it, and very likely its idle siblings with it. A
		// timeout is one slow or stalled command: the siblings are still
		// good, and closing them on a flaky link would redial on every
		// stall and trip the breaker.
		if reused && isNetworkError(err) && !isTimeoutError(err) {
			p.log.Debug("usenet: idle connection was dropped by the server; discarding idle connections", "err", err)
			p.discardIdle()
		}
		if errors.Is(err, ErrCorrupt) {
			p.log.Warn("usenet: corrupt article", "err", err)
		}
	}
	return err
}

// ProviderStats is a snapshot of one provider for the admin dashboard.
type ProviderStats struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Host     string `json:"host"`
	Priority int    `json:"priority"`
	Backfill bool   `json:"backfill"`

	// State is StateOK, StateBackoff or StateAuthFail.
	State        string    `json:"state"`
	BackoffUntil time.Time `json:"backoffUntil,omitempty"`
	LastError    string    `json:"lastError,omitempty"`
	LastErrorAt  time.Time `json:"lastErrorAt,omitempty"`

	// MaxConnections is the effective limit; it drops below
	// ConfiguredConnections for a while when the server refuses more.
	MaxConnections        int `json:"maxConnections"`
	ConfiguredConnections int `json:"configuredConnections"`
	LiveConnections       int `json:"liveConnections"`   // open sockets
	ActiveConnections     int `json:"activeConnections"` // slots in use
	IdleConnections       int `json:"idleConnections"`
	// MinIdle is the configured warm floor (0 = none for this pool);
	// WarmIdleConnections is how much of it is currently held idle.
	MinIdle             int   `json:"minIdle"`
	WarmIdleConnections int   `json:"warmIdleConnections"`
	BackgroundRedials   int64 `json:"backgroundRedials"` // floor janitor dials
	RedialFailures      int64 `json:"redialFailures"`
	PrefetchActive      int   `json:"prefetchActive"`
	WaitingLive         int   `json:"waitingLive"`
	WaitingReadahead    int   `json:"waitingReadahead"`
	WaitingPrefetch     int   `json:"waitingPrefetch"`
	// NodeShare is this node's part of ConfiguredConnections when the
	// account's connections are divided among cluster nodes (0 otherwise).
	NodeShare int `json:"nodeShare,omitempty"`

	Bytes         int64 `json:"bytes"` // received off the wire
	Articles      int64 `json:"articles"`
	Misses        int64 `json:"misses"`
	BodyMisses    int64 `json:"bodyMisses"`    // BODY responses unavailable on this provider
	BodyTakedowns int64 `json:"bodyTakedowns"` // subset of BodyMisses with status 451
	BackfillSaves int64 `json:"backfillSaves"` // body fetches saved after earlier provider misses
	Errors        int64 `json:"errors"`
	Dials         int64 `json:"dials"`

	AvgLatency       time.Duration `json:"avgLatency"`   // command round trip (EWMA)
	AvgFetchTime     time.Duration `json:"avgFetchTime"` // whole article (EWMA)
	TCPDialTime      time.Duration `json:"tcpDialTime"`
	TLSHandshakeTime time.Duration `json:"tlsHandshakeTime"`
	GreetingTime     time.Duration `json:"greetingTime"`
	AuthTime         time.Duration `json:"authTime"`
	BODYBatches      int64         `json:"bodyBatches"`
	BODYPipelined    int64         `json:"bodyPipelined"`
	STATBatches      int64         `json:"statBatches"`
	STATBatchTime    time.Duration `json:"statBatchTime"`
}

func (p *pool) stats() ProviderStats {
	st := ProviderStats{
		ID: p.cfg.ID, Name: p.cfg.Name, Host: p.cfg.Host,
		Priority: p.cfg.Priority, Backfill: p.cfg.Backfill,
	}
	state, until, lastErr, lastAt := p.brk.state()
	st.State = state
	if state == StateBackoff {
		st.BackoffUntil = until
	}
	if lastErr != nil {
		st.LastError, st.LastErrorAt = lastErr.Error(), lastAt
	}
	ss := p.sem.stats()
	st.MaxConnections = ss.Size
	st.ConfiguredConnections = p.size
	st.ActiveConnections = ss.InUse
	st.PrefetchActive = ss.Prefetching
	st.WaitingLive = ss.Waiting[core.PriorityLive]
	st.WaitingReadahead = ss.Waiting[core.PriorityReadahead]
	st.WaitingPrefetch = ss.Waiting[core.PriorityPrefetch]
	p.bodyMu.Lock()
	st.WaitingLive += len(p.bodyQueue[core.PriorityLive])
	st.WaitingReadahead += len(p.bodyQueue[core.PriorityReadahead])
	st.WaitingPrefetch += len(p.bodyQueue[core.PriorityPrefetch])
	p.bodyMu.Unlock()
	st.MinIdle = p.idleFloor()
	p.mu.Lock()
	if p.share > 0 {
		st.NodeShare = p.targetLocked()
	}
	st.LiveConnections = p.live
	st.IdleConnections = len(p.idle)
	if p.closed {
		st.State = StateClosed
	}
	p.mu.Unlock()
	st.WarmIdleConnections = min(st.IdleConnections, st.MinIdle)
	st.BackgroundRedials = p.bgRedials.Load()
	st.RedialFailures = p.redialFails.Load()
	st.Bytes = p.bytes.Load()
	st.Articles = p.articles.Load()
	st.Misses = p.misses.Load()
	st.BackfillSaves = p.backfillSaves.Load()
	st.BodyMisses = p.bodyMisses.Load()
	st.BodyTakedowns = p.bodyTakedowns.Load()
	st.Errors = p.errs.Load()
	st.Dials = p.dials.Load()
	st.TCPDialTime = time.Duration(p.tcpTime.Load())
	st.TLSHandshakeTime = time.Duration(p.tlsTime.Load())
	st.GreetingTime = time.Duration(p.greetingTime.Load())
	st.AuthTime = time.Duration(p.authTime.Load())
	st.BODYBatches = p.bodyBatches.Load()
	st.BODYPipelined = p.bodyPipelined.Load()
	st.STATBatches = p.statBatches.Load()
	st.STATBatchTime = time.Duration(p.statBatchTime.Load())
	p.latMu.Lock()
	st.AvgLatency = time.Duration(p.latency.v)
	st.AvgFetchTime = time.Duration(p.fetching.v)
	p.latMu.Unlock()
	return st
}

// idleFloor is how many live connections the janitor keeps warm for this
// pool: zero for backfill pools, when the floor is disabled, or when
// keepalive pings are off (unpinged connections would go stale anyway).
// It never exceeds the pool's effective (possibly server-limited) size.
func (p *pool) idleFloor() int {
	m := int(p.minIdle.Load())
	if p.cfg.Backfill || m < 0 || p.o.KeepAlive <= 0 {
		return 0
	}
	return min(p.target(), p.sem.stats().Size, m)
}

// targetLocked is the connection count this pool aims for: the configured
// size, or this node's share of it in a cluster. p.mu must be held.
func (p *pool) targetLocked() int {
	if p.share > 0 && p.share < p.size {
		return p.share
	}
	return p.size
}

func (p *pool) target() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.targetLocked()
}

// setShare sets this node's share of the configured connections (0 = all
// of them). Shrinking is graceful: requests holding connections finish
// (their connections close when returned while the pool is above its new
// target) and idle connections beyond the target close now. Growing lets
// waiting requests take the new slots at once. It reports whether the
// target changed.
func (p *pool) setShare(n int) bool {
	n = max(n, 0)
	p.mu.Lock()
	if p.closed || p.share == n {
		p.mu.Unlock()
		return false
	}
	before := p.targetLocked()
	p.share = n
	target := p.targetLocked()
	limited := !p.limitedUntil.IsZero()
	var drop []*Conn
	for p.live > target && len(p.idle) > 0 {
		drop = append(drop, p.idle[0]) // least recently used first
		p.idle[0] = nil
		p.idle = p.idle[1:]
		p.live--
	}
	p.mu.Unlock()
	for _, c := range drop {
		go c.Quit()
	}
	if before == target {
		return false
	}
	// While the server limits us below the configured count, keep the
	// smaller of the two; the cool-off restore uses the new target.
	if !limited || p.sem.stats().Size > target {
		p.sem.setSize(target, p.o.PrefetchFraction)
	}
	p.wakeBodyWorkers()
	p.log.Info("usenet: connection share changed", "configured", p.size, "from", before, "to", target, "closed_idle", len(drop))
	return true
}
