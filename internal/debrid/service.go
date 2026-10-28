package debrid

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/braedonsaunders/ether/internal/clusterbus"
	"github.com/braedonsaunders/ether/internal/config"
	"github.com/braedonsaunders/ether/internal/core"
	"github.com/braedonsaunders/ether/internal/store"
)

func init() {
	// Portable SQL (no INSERT OR REPLACE; 0/1 booleans).
	store.Register("debrid", 1, `
CREATE TABLE debrid_payloads (
  release_key TEXT PRIMARY KEY,
  payload     TEXT NOT NULL,
  updated_at  INTEGER NOT NULL
);
CREATE TABLE debrid_torrents (
  provider_id TEXT NOT NULL,
  info_hash   TEXT NOT NULL,
  torrent_id  TEXT NOT NULL DEFAULT '',
  cached      INTEGER NOT NULL DEFAULT 0,
  checked_at  INTEGER NOT NULL,
  PRIMARY KEY (provider_id, info_hash)
);`)
}

const (
	defaultPayloadTTL  = 7 * 24 * time.Hour
	defaultCachedTTL   = 12 * time.Hour
	defaultUncachedTTL = 30 * time.Minute
	providerOpTimeout  = 45 * time.Second
	providerParallel   = 4
	authBackoff        = 15 * time.Minute
	maxBackoff         = 5 * time.Minute
	// linkMargin is how long before expiry a link counts as stale.
	linkMargin = 2 * time.Minute
)

// Options configure a Service.
type Options struct {
	DB     *sql.DB // nil keeps state in memory
	Logger *slog.Logger
	Now    func() time.Time
	// Provider options (tests point them at httptest servers).
	ProviderOptions ProviderOptions
	// NewProvider overrides provider construction (tests).
	NewProvider func(config.DebridProvider, ProviderOptions) (Provider, error)
	PayloadTTL  time.Duration // stored payload reuse, 0 = 7 days
	CachedTTL   time.Duration // known-cached memory, 0 = 12h
	UncachedTTL time.Duration // known-uncached memory, 0 = 30m
}

// Service holds the configured debrid providers and applies priority,
// backoff, bounded concurrency and caching to every provider call.
type Service struct {
	opts Options
	log  *slog.Logger
	db   *sql.DB
	now  func() time.Time

	mu      sync.RWMutex
	cfg     config.Debrid
	entries []*entry // configured providers in priority order

	knownMu sync.Mutex
	known   map[string]knownState // providerID|hash

	// Cluster (cluster.go); all nil/zero on a standalone server.
	clusterMu sync.RWMutex
	node      clusterbus.Node
	inval     *clusterbus.Invalidator
	origin    string
	unsubs    []func()
	share     func(configured int) int // concurrency share; nil = configured
}

type knownState struct {
	cached bool
	until  time.Time
}

// entry is one configured provider with its breaker and counters.
type entry struct {
	cfg config.DebridProvider
	p   Provider
	err error // construction error (missing key, unknown kind)
	sem *limiter

	mu        sync.Mutex
	failures  int
	until     time.Time
	state     string // "", "backoff", "auth_failed"
	lastError string
	stats     ProviderStats
	// Add refusals (ErrAddRefused): consecutive count, the latest, and the
	// pause on new torrents they caused.
	addRefusals int
	lastRefusal time.Time
	addsUntil   time.Time
}

const (
	// addRefusedTTL is how long a refused add skips that hash: short, since
	// a refusal may be the account's add throttle rather than the torrent.
	addRefusedTTL = 5 * time.Minute
	addPauseMin   = 2 * time.Minute
	addPauseMax   = 15 * time.Minute
)

// addRefused records a refused add. A second refusal in a row pauses new
// torrents on the provider (existing torrents and links keep working),
// doubling while refusals continue, so Ether does not keep provoking an
// account-level throttle.
func (e *entry) addRefused(now time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if now.Sub(e.lastRefusal) > addPauseMax {
		e.addRefusals = 0
	}
	e.addRefusals++
	e.lastRefusal = now
	if e.addRefusals >= 2 {
		e.addsUntil = now.Add(min(addPauseMin<<min(e.addRefusals-2, 3), addPauseMax))
	}
}

// addAccepted ends a run of refusals.
func (e *entry) addAccepted() {
	e.mu.Lock()
	e.addRefusals = 0
	e.mu.Unlock()
}

// addsPaused reports a pause on new torrents as not cached for now: the
// release is skipped on this provider without marking it.
func (e *entry) addsPaused(now time.Time) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if now.Before(e.addsUntil) {
		return fmt.Errorf("debrid provider %q is not adding torrents until %s: %w", displayName(e.cfg), e.addsUntil.Format(time.Kitchen), errAddsPaused)
	}
	return nil
}

// errAddsPaused is a release skipped while a provider's adds are paused.
var errAddsPaused = fmt.Errorf("debrid: adds paused after refusals: %w", core.ErrNotCached)

// ProviderStats are one provider's counters and latency EWMAs.
type ProviderStats struct {
	ID             string     `json:"id"`
	Name           string     `json:"name"`
	Kind           string     `json:"kind"`
	Enabled        bool       `json:"enabled"`
	Priority       int        `json:"priority"`
	State          string     `json:"state"` // ok, backoff, auth_failed, disabled
	LastError      string     `json:"lastError,omitempty"`
	BackoffUntil   *time.Time `json:"backoffUntil,omitempty"`
	Requests       int64      `json:"requests"`
	Failures       int64      `json:"failures"`
	Cached         int64      `json:"cached"`
	NotCached      int64      `json:"notCached"`
	Blocked        int64      `json:"blocked"` // cached files the provider refused to serve
	LinksRefreshed int64      `json:"linksRefreshed"`
	Reused         int64      `json:"reused"`
	// Slots is how many API calls this node makes to the provider at once:
	// AccountSlots on a standalone server, this node's share of it in a
	// cluster.
	Slots        int        `json:"slots"`
	AccountSlots int        `json:"accountSlots"`
	LastSuccess  *time.Time `json:"lastSuccess,omitempty"`
	Latency      Latency    `json:"latency"`
}

// Latency holds exponentially weighted averages of a provider's timings:
// API resolution (add/select/poll), link generation, CDN connect (TCP+TLS)
// and time to first byte of a range request, plus read throughput.
type Latency struct {
	ResolveMs     float64 `json:"resolveMs"`
	LinkMs        float64 `json:"linkMs"`
	ConnectMs     float64 `json:"connectMs"`
	TTFBMs        float64 `json:"ttfbMs"`
	ThroughputBps float64 `json:"throughputBps"`
	Samples       int64   `json:"samples"`
}

const ewmaAlpha = 0.3

func ewma(cur, x float64, first bool) float64 {
	if first || cur == 0 {
		return x
	}
	return ewmaAlpha*x + (1-ewmaAlpha)*cur
}

// Status is the admin "debrid" status.
type Status struct {
	Enabled    bool            `json:"enabled"`
	CachedOnly bool            `json:"cachedOnly"`
	Providers  []ProviderStats `json:"providers"`
	Engine     *EngineStats    `json:"engine,omitempty"`
}

// NewService builds a Service from the debrid config section.
func NewService(cfg config.Debrid, opts Options) *Service {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.NewProvider == nil {
		opts.NewProvider = NewProvider
	}
	if opts.PayloadTTL <= 0 {
		opts.PayloadTTL = defaultPayloadTTL
	}
	if opts.CachedTTL <= 0 {
		opts.CachedTTL = defaultCachedTTL
	}
	if opts.UncachedTTL <= 0 {
		opts.UncachedTTL = defaultUncachedTTL
	}
	if opts.ProviderOptions.Now == nil {
		opts.ProviderOptions.Now = opts.Now
	}
	s := &Service{opts: opts, log: opts.Logger, db: opts.DB, now: opts.Now, known: map[string]knownState{}}
	s.UpdateConfig(cfg)
	return s
}

// UpdateConfig hot-applies an edited debrid section. Providers whose
// identity and key are unchanged keep their breaker state and counters.
func (s *Service) UpdateConfig(cfg config.Debrid) {
	s.mu.Lock()
	defer s.mu.Unlock()
	old := map[string]*entry{}
	for _, e := range s.entries {
		old[e.cfg.ID+"|"+e.cfg.Kind+"|"+e.cfg.APIKey] = e
	}
	next := make([]*entry, 0, len(cfg.Providers))
	for i, pc := range cfg.Providers {
		if pc.ID == "" {
			pc.ID = fmt.Sprintf("%s-%d", pc.Kind, i)
		}
		if e, ok := old[pc.ID+"|"+pc.Kind+"|"+pc.APIKey]; ok {
			e.mu.Lock()
			e.cfg = pc
			e.mu.Unlock()
			next = append(next, e)
			continue
		}
		e := &entry{cfg: pc, sem: newLimiter(s.slotsFor(providerParallel))}
		e.p, e.err = s.opts.NewProvider(pc, s.opts.ProviderOptions)
		next = append(next, e)
	}
	sort.SliceStable(next, func(i, j int) bool { return next[i].cfg.Priority < next[j].cfg.Priority })
	s.cfg = cfg
	s.entries = next
}

// Config returns the current debrid section.
func (s *Service) Config() config.Debrid {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cfg
}

// active returns enabled, constructible providers in priority order.
func (s *Service) active() []*entry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*entry
	for _, e := range s.entries {
		if e.cfg.Enabled && e.p != nil && e.err == nil {
			out = append(out, e)
		}
	}
	return out
}

func (s *Service) entryByID(id string) *entry {
	for _, e := range s.active() {
		if e.cfg.ID == id {
			return e
		}
	}
	return nil
}

// Enabled reports whether any provider can serve torrents.
func (s *Service) Enabled() bool { return len(s.active()) > 0 }

// ---------------------------------------------------------------------------
// Breaker and counters.

func (e *entry) blocked(now time.Time) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if now.Before(e.until) {
		if e.state == "auth_failed" {
			// Keep the cause classifiable: playback tells the viewer the
			// key was rejected rather than that nothing was found.
			return fmt.Errorf("debrid provider %q paused until %s (%s): %w", displayName(e.cfg), e.until.Format(time.Kitchen), e.lastError, ErrAuth)
		}
		return fmt.Errorf("debrid provider %q paused until %s (%s)", displayName(e.cfg), e.until.Format(time.Kitchen), e.lastError)
	}
	return nil
}

func (e *entry) acquire(ctx context.Context) error { return e.sem.acquire(ctx) }

func (e *entry) release() { e.sem.release() }

// observe updates the breaker after a provider call. It returns the new
// pause when the call put the provider into back-off, a clear event when a
// success ended an earlier pause, and nil otherwise (cluster.go broadcasts
// the events).
func (e *entry) observe(now time.Time, err error) *backoffEvent {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.stats.Requests++
	if err == nil || errors.Is(err, core.ErrNotCached) || errors.Is(err, ErrGone) || errors.Is(err, core.ErrReleaseRejected) {
		// The provider answered; release-level outcomes are not outages.
		was := e.state
		e.failures = 0
		if !now.Before(e.until) {
			// A call that was already in flight when a pause began does not
			// end it (nor blank its reported state) early.
			e.state = ""
		}
		if err == nil {
			t := now
			e.stats.LastSuccess = &t
		}
		if was != "" && !now.Before(e.until) {
			return &backoffEvent{Provider: e.cfg.ID}
		}
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, core.ErrBudgetExhausted) {
		return nil
	}
	e.stats.Failures++
	e.lastError = err.Error()
	var rl *rateLimitError
	switch {
	case errors.Is(err, ErrAuth):
		e.state, e.until = "auth_failed", now.Add(authBackoff)
	case errors.As(err, &rl) || errors.Is(err, ErrRateLimited):
		d := 30 * time.Second
		if rl != nil && rl.retryAfter > 0 {
			d = min(rl.retryAfter, maxBackoff)
		}
		e.state, e.until = "backoff", now.Add(d)
	default:
		e.failures++
		if e.failures < 3 {
			return nil
		}
		d := min(maxBackoff, 30*time.Second<<min(e.failures-3, 4))
		e.state, e.until = "backoff", now.Add(d)
	}
	return &backoffEvent{Provider: e.cfg.ID, State: e.state, Until: e.until.UnixMilli(), Error: shortError(e.lastError)}
}

func (e *entry) count(fn func(*ProviderStats)) {
	e.mu.Lock()
	fn(&e.stats)
	e.mu.Unlock()
}

// Stats reports every configured provider.
func (s *Service) Stats() []ProviderStats {
	s.mu.RLock()
	entries := append([]*entry(nil), s.entries...)
	s.mu.RUnlock()
	now := s.now()
	out := make([]ProviderStats, 0, len(entries))
	for _, e := range entries {
		e.mu.Lock()
		st := e.stats
		st.ID, st.Name, st.Kind, st.Enabled, st.Priority = e.cfg.ID, displayName(e.cfg), e.cfg.Kind, e.cfg.Enabled, e.cfg.Priority
		st.LastError = e.lastError
		st.Slots, st.AccountSlots = e.sem.size(), providerParallel
		switch {
		case !e.cfg.Enabled:
			st.State = "disabled"
		case e.err != nil:
			st.State, st.LastError = "auth_failed", e.err.Error()
		case now.Before(e.until):
			st.State = e.state
			u := e.until
			st.BackoffUntil = &u
		default:
			st.State = "ok"
		}
		e.mu.Unlock()
		out = append(out, st)
	}
	return out
}

// Latencies returns each enabled provider's latency EWMAs by provider id,
// for source routing.
func (s *Service) Latencies() map[string]Latency {
	out := map[string]Latency{}
	for _, e := range s.active() {
		e.mu.Lock()
		out[e.cfg.ID] = e.stats.Latency
		e.mu.Unlock()
	}
	return out
}

// Status is the admin status document (engine stats are filled in by the
// caller when an Engine is wired).
func (s *Service) Status() Status {
	cfg := s.Config()
	return Status{Enabled: s.Enabled(), CachedOnly: cfg.CachedOnly, Providers: s.Stats()}
}

func (s *Service) noteLatency(providerID string, fn func(*Latency)) {
	s.mu.RLock()
	var e *entry
	for _, x := range s.entries {
		if x.cfg.ID == providerID {
			e = x
		}
	}
	s.mu.RUnlock()
	if e != nil {
		e.count(func(st *ProviderStats) { fn(&st.Latency) })
	}
}

// RecordStream feeds CDN timings observed by the engine into the provider's
// EWMAs (connect and ttfb may be zero when unknown).
func (s *Service) RecordStream(providerID string, connect, ttfb time.Duration, bytes int64, elapsed time.Duration) {
	s.noteLatency(providerID, func(l *Latency) {
		first := l.Samples == 0
		if connect > 0 {
			l.ConnectMs = ewma(l.ConnectMs, ms(connect), first)
		}
		if ttfb > 0 {
			l.TTFBMs = ewma(l.TTFBMs, ms(ttfb), first)
		}
		if bytes >= 256<<10 && elapsed > 0 {
			l.ThroughputBps = ewma(l.ThroughputBps, float64(bytes)/elapsed.Seconds(), first)
		}
		l.Samples++
	})
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

// ---------------------------------------------------------------------------
// Known cached / uncached hashes.

// setKnown remembers a provider's answer for a hash and tells the other
// cluster nodes to re-read it.
func (s *Service) setKnown(ctx context.Context, providerID, hash, torrentID string, cached bool) {
	s.notifyKnown(s.recordKnown(ctx, providerID, hash, torrentID, cached))
}

// recordKnown is setKnown without the cluster notification (bulk callers
// notify once for many hashes); it returns the hash's known-map key.
func (s *Service) recordKnown(ctx context.Context, providerID, hash, torrentID string, cached bool) string {
	hash = strings.ToLower(hash)
	ttl := s.opts.UncachedTTL
	if cached {
		ttl = s.opts.CachedTTL
	}
	now := s.now()
	key := knownKey(providerID, hash)
	s.knownMu.Lock()
	s.known[key] = knownState{cached: cached, until: now.Add(ttl)}
	s.knownMu.Unlock()
	if s.db == nil {
		return key
	}
	cachedInt := 0
	if cached {
		cachedInt = 1
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO debrid_torrents (provider_id, info_hash, torrent_id, cached, checked_at)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT (provider_id, info_hash) DO UPDATE SET
  torrent_id = CASE WHEN excluded.torrent_id <> '' THEN excluded.torrent_id ELSE debrid_torrents.torrent_id END,
  cached = excluded.cached,
  checked_at = excluded.checked_at`, providerID, hash, torrentID, cachedInt, now.Unix())
	if err != nil {
		s.log.Debug("debrid: record torrent", "err", err)
	}
	return key
}

// knownFor reports a remembered answer for provider+hash.
func (s *Service) knownFor(ctx context.Context, providerID, hash string) (cached, ok bool, torrentID string) {
	hash = strings.ToLower(hash)
	now := s.now()
	s.knownMu.Lock()
	k, hit := s.known[knownKey(providerID, hash)]
	s.knownMu.Unlock()
	if hit && now.Before(k.until) {
		cached, ok = k.cached, true
	}
	if s.db != nil {
		var tid string
		var c int
		var at int64
		if err := s.db.QueryRowContext(ctx, `SELECT torrent_id, cached, checked_at FROM debrid_torrents WHERE provider_id = ? AND info_hash = ?`,
			providerID, hash).Scan(&tid, &c, &at); err == nil {
			torrentID = tid
			if !ok {
				ttl := s.opts.UncachedTTL
				if c == 1 {
					ttl = s.opts.CachedTTL
				}
				if now.Sub(time.Unix(at, 0)) < ttl {
					cached, ok = c == 1, true
				}
			}
		}
	}
	return cached, ok, torrentID
}

// hintTTL bounds how long a source's cached marker stands in for a
// provider's own answer.
const hintTTL = 30 * time.Minute

// ApplyHints records the cached markers sources put on torrent releases
// (core.Release.DebridCached and DebridUncached) for providers that have
// no answer of their own about the hash. A provider the source reported as
// holding the torrent is tried ahead of unknown ones; with CachedOnly, one
// it reported as not holding it is skipped without adding the torrent.
// Hints live in memory only and never replace a provider's answer.
func (s *Service) ApplyHints(ctx context.Context, rels []core.Release) {
	var entries []*entry
	cachedOnly := false
	for _, r := range rels {
		if r.Protocol != core.ProtocolTorrent || r.InfoHash == "" || len(r.DebridCached)+len(r.DebridUncached) == 0 {
			continue
		}
		if entries == nil {
			if entries = s.active(); len(entries) == 0 {
				return
			}
			cachedOnly = s.Config().CachedOnly
		}
		hash := strings.ToLower(r.InfoHash)
		for _, e := range entries {
			cached := slices.Contains(r.DebridCached, e.cfg.Kind)
			if !cached && (!cachedOnly || !slices.Contains(r.DebridUncached, e.cfg.Kind)) {
				continue
			}
			if _, ok, _ := s.knownFor(ctx, e.cfg.ID, hash); ok {
				continue
			}
			s.knownMu.Lock()
			s.known[knownKey(e.cfg.ID, hash)] = knownState{cached: cached, until: s.now().Add(hintTTL)}
			s.knownMu.Unlock()
		}
	}
}

// KnownCached reports, without any API call, whether an enabled provider
// recently reported the hash as cached.
func (s *Service) KnownCached(hash string) bool {
	hash = strings.ToLower(hash)
	now := s.now()
	entries := s.active()
	s.knownMu.Lock()
	defer s.knownMu.Unlock()
	for _, e := range entries {
		if k, ok := s.known[knownKey(e.cfg.ID, hash)]; ok && k.cached && now.Before(k.until) {
			return true
		}
	}
	return false
}

// CheckCached asks providers with bulk availability checks about hashes,
// remembering the answers. It returns hash -> cached on some provider, and
// definitive=true when every enabled provider could answer (so a false
// entry means no provider has the torrent).
func (s *Service) CheckCached(ctx context.Context, hashes []string) (map[string]bool, bool) {
	out := map[string]bool{}
	if len(hashes) == 0 {
		return out, false
	}
	definitive := true
	for _, e := range s.active() {
		if e.blocked(s.now()) != nil {
			definitive = false
			continue
		}
		var ask []string
		for _, h := range hashes {
			if c, ok, _ := s.knownFor(ctx, e.cfg.ID, h); ok {
				out[h] = out[h] || c
			} else {
				ask = append(ask, h)
			}
		}
		if len(ask) == 0 {
			continue
		}
		if err := e.acquire(ctx); err != nil {
			return out, false
		}
		cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		res, err := e.p.CheckCached(cctx, ask)
		cancel()
		e.release()
		if errors.Is(err, ErrUnsupported) {
			definitive = false
			continue
		}
		if err != nil && ctx.Err() != nil {
			// A caller's short discovery budget says nothing about the
			// provider's availability. Do not open its circuit for that.
			return out, false
		}
		s.observe(e, err)
		if err != nil {
			definitive = false
			continue
		}
		keys := make([]string, 0, len(ask))
		for _, h := range ask {
			c := res[strings.ToLower(h)]
			out[h] = out[h] || c
			keys = append(keys, s.recordKnown(ctx, e.cfg.ID, h, "", c))
		}
		s.notifyKnown(keys...)
	}
	return out, definitive
}

// ---------------------------------------------------------------------------
// Payload persistence.

func payloadKey(rel core.Release) string {
	k := rel.ID
	if rel.Target != nil {
		k += fmt.Sprintf("|s%de%d", rel.Target.Season, rel.Target.Episode)
	}
	return k
}

func (s *Service) storedPayload(ctx context.Context, key string) (Payload, bool) {
	if s.db == nil || key == "" {
		return Payload{}, false
	}
	var raw string
	var at int64
	if err := s.db.QueryRowContext(ctx, `SELECT payload, updated_at FROM debrid_payloads WHERE release_key = ?`, key).Scan(&raw, &at); err != nil {
		return Payload{}, false
	}
	if s.now().Sub(time.Unix(at, 0)) > s.opts.PayloadTTL {
		return Payload{}, false
	}
	p, err := DecodePayload([]byte(raw))
	return p, err == nil
}

func (s *Service) savePayload(ctx context.Context, p Payload) {
	if s.db == nil || p.Key == "" {
		return
	}
	_, err := s.db.ExecContext(context.WithoutCancel(ctx), `INSERT INTO debrid_payloads (release_key, payload, updated_at) VALUES (?, ?, ?)
ON CONFLICT (release_key) DO UPDATE SET payload = excluded.payload, updated_at = excluded.updated_at`,
		p.Key, string(p.Encode()), s.now().Unix())
	if err != nil {
		s.log.Debug("debrid: save payload", "err", err)
	}
}

// ---------------------------------------------------------------------------
// Acquire and refresh.

// Acquire makes a torrent release available on the first provider (by
// priority) that has it cached, and returns its payload. A payload stored
// for the same release within PayloadTTL is reused without API calls. The
// error wraps core.ErrNotCached when every provider answered "not cached",
// errNoVideo (core.ErrReleaseRejected) when the torrent has no playable
// file, and is a plain (transient) error otherwise.
func (s *Service) Acquire(ctx context.Context, rel core.Release) ([]byte, error) {
	entries := s.active()
	if len(entries) == 0 {
		return nil, ErrNoProvider
	}
	hash := strings.ToLower(rel.InfoHash)
	if hash == "" {
		return nil, fmt.Errorf("debrid: torrent %q has no info hash: %w", rel.Name, core.ErrReleaseRejected)
	}
	key := payloadKey(rel)
	if p, ok := s.reusablePayload(ctx, rel); ok {
		return p.Encode(), nil
	}
	magnet := rel.MagnetURL
	if magnet == "" {
		magnet = "magnet:?xt=urn:btih:" + hash
	}
	base := Payload{Key: key, ReleaseID: rel.ID, InfoHash: hash, Magnet: magnet, Target: rel.Target, Release: rel.Name}
	choose := func(files []File) (File, error) { return ChooseFile(files, rel.Target) }
	var errs []error
	notCached := 0
	for _, e := range entries {
		if err := e.blocked(s.now()); err != nil {
			errs = append(errs, err)
			continue
		}
		if cached, ok, _ := s.knownFor(ctx, e.cfg.ID, hash); ok && !cached {
			notCached++
			continue
		}
		p, err := s.acquireOn(ctx, e, base, choose)
		if err == nil {
			s.savePayload(ctx, p)
			s.log.Info("debrid release ready", "release", rel.Name, "provider", displayName(e.cfg), "file", p.Name())
			return p.Encode(), nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		switch {
		case errors.Is(err, core.ErrNotCached):
			notCached++
		case errors.Is(err, core.ErrReleaseRejected):
			return nil, err // the torrent itself has no playable file
		default:
			errs = append(errs, fmt.Errorf("%s: %w", displayName(e.cfg), err))
		}
	}
	if len(errs) == 0 {
		return nil, fmt.Errorf("%q: %w", rel.Name, core.ErrNotCached)
	}
	if notCached > 0 {
		return nil, fmt.Errorf("%q: %w (%v)", rel.Name, core.ErrNotCached, errors.Join(errs...))
	}
	return nil, errors.Join(errs...)
}

// acquireOn resolves a torrent on one provider: reuse an existing torrent
// by hash, else add it; then generate a direct link.
func (s *Service) acquireOn(ctx context.Context, e *entry, base Payload, choose func([]File) (File, error)) (Payload, error) {
	wallStarted := time.Now()
	var queueDuration, findDuration, readyDuration, linkDuration time.Duration
	defer func() {
		s.log.Info("debrid acquisition timings", "provider", e.cfg.ID,
			"queue", queueDuration, "find", findDuration, "ready", readyDuration,
			"link", linkDuration, "took", time.Since(wallStarted))
	}()
	if err := e.acquire(ctx); err != nil {
		return Payload{}, err
	}
	queueDuration = time.Since(wallStarted)
	defer e.release()
	// A caller's own budget (a resolver candidate check, say) ending says
	// nothing about the provider; only the provider's outcomes, including
	// its own providerOpTimeout, move the breaker.
	parent := ctx
	observe := func(err error) {
		if err == nil || parent.Err() == nil {
			s.observe(e, err)
		}
	}
	ctx, cancel := context.WithTimeout(ctx, providerOpTimeout)
	defer cancel()
	cfg := s.Config()
	hash := base.InfoHash

	started := s.now()
	findStarted := time.Now()
	_, _, torrentID := s.knownFor(ctx, e.cfg.ID, hash)
	if torrentID == "" {
		if fast, ok := e.p.(knownTorrentFinder); ok {
			// Providers with atomic cached-only create + duplicate recovery
			// can reuse known IDs without listing the whole account first.
			torrentID = fast.findKnown(hash)
		} else if id, err := e.p.Find(ctx, hash); err == nil {
			torrentID = id
		} else if !errors.Is(err, ErrUnsupported) {
			observe(err)
			if errors.Is(err, ErrAuth) {
				return Payload{}, err
			}
		}
	}
	// Background validation never creates torrents or starts downloads. An
	// existing account item can be inspected; unknown remains unknown.
	if core.IsBackgroundCheck(ctx) && torrentID == "" {
		return Payload{}, ErrUnsupported
	}
	findDuration = time.Since(findStarted)
	readyStarted := time.Now()
	if torrentID == "" {
		if err := e.addsPaused(s.now()); err != nil {
			return Payload{}, err
		}
		// Adding to the account: in a cluster one node adds, the others
		// wait for its torrent id (no-op standalone).
		id, release, err := s.claimAdd(ctx, e, hash)
		if err != nil {
			return Payload{}, err
		}
		defer release()
		torrentID = id
	}
	req := AcquireRequest{InfoHash: hash, Magnet: base.Magnet, TorrentID: torrentID, Choose: choose, CachedOnly: cfg.CachedOnly || core.IsBackgroundCheck(ctx),
		RemoveUncached: cfg.RemoveUncached, MayRemove: s.mayRemove(e.cfg.ID)}
	acquire := func() (*Acquired, error) {
		if req.TorrentID == "" {
			if err := e.addsPaused(s.now()); err != nil {
				return nil, err
			}
		}
		acq, err := e.p.Acquire(ctx, req)
		switch {
		case errors.Is(err, ErrAddRefused):
			e.addRefused(s.now())
		case err == nil && acq.Added:
			e.addAccepted()
		}
		return acq, err
	}
	acq, err := acquire()
	if err != nil && !core.IsBackgroundCheck(ctx) && torrentID != "" && (errors.Is(err, ErrGone) || errors.Is(err, core.ErrNotCached)) && !errors.Is(err, ErrAddRefused) {
		// The remembered torrent is gone or lacks the wanted file: add anew.
		req.TorrentID = ""
		id, release, cerr := s.reclaimAdd(ctx, e, hash, torrentID)
		if cerr == nil {
			defer release()
			req.TorrentID = id
			acq, err = acquire()
		} else {
			err = cerr
		}
	}
	readyDuration = time.Since(readyStarted)
	if errors.Is(err, errAddsPaused) {
		// Skipped without a provider call: says nothing about the torrent.
		return Payload{}, err
	}
	observe(err)
	if errors.Is(err, ErrAddRefused) {
		// Not evidence about the torrent: skip it briefly, in memory only.
		e.count(func(st *ProviderStats) { st.Blocked++ })
		s.knownMu.Lock()
		s.known[knownKey(e.cfg.ID, hash)] = knownState{until: s.now().Add(addRefusedTTL)}
		s.knownMu.Unlock()
		return Payload{}, err
	}
	if err != nil {
		if errors.Is(err, core.ErrNotCached) {
			e.count(func(st *ProviderStats) { st.NotCached++ })
			s.setKnown(ctx, e.cfg.ID, hash, "", false)
		}
		return Payload{}, err
	}
	resolved := s.now().Sub(started)
	if req.TorrentID != "" {
		e.count(func(st *ProviderStats) { st.Reused++ })
	}
	e.count(func(st *ProviderStats) { st.Cached++ })
	s.setKnown(ctx, e.cfg.ID, hash, acq.TorrentID, true)

	p := base
	p.Provider, p.Kind = e.cfg.ID, e.cfg.Kind
	p.TorrentID, p.FileID, p.Path, p.Size, p.Link = acq.TorrentID, acq.File.ID, acq.File.Path, acq.File.Size, acq.File.Link
	linkStart := s.now()
	linkWallStarted := time.Now()
	dl, err := e.p.Link(ctx, p.handle())
	linkDuration = time.Since(linkWallStarted)
	observe(err)
	if err != nil {
		if errors.Is(err, ErrBlocked) {
			s.blockedFile(ctx, e, hash, acq.TorrentID, acq.Added && cfg.RemoveUncached, req.MayRemove)
		}
		return Payload{}, err
	}
	linked := s.now().Sub(linkStart)
	s.noteLatency(e.cfg.ID, func(l *Latency) {
		first := l.Samples == 0
		l.ResolveMs = ewma(l.ResolveMs, ms(resolved), first)
		l.LinkMs = ewma(l.LinkMs, ms(linked), first)
		l.Samples++
	})
	p.URL, p.Expires = dl.URL, dl.Expires
	if dl.Size > 0 && p.Size <= 0 {
		p.Size = dl.Size
	}
	if p.Size <= 0 {
		return Payload{}, fmt.Errorf("debrid: %s reported no size for %s: %w", displayName(e.cfg), p.Name(), errNoVideo)
	}
	return p, nil
}

// fresh reports whether a payload's direct link is usable now.
func (s *Service) fresh(p Payload) bool {
	return p.URL != "" && (p.Expires.IsZero() || s.now().Add(linkMargin).Before(p.Expires))
}

// Refresh returns p with a usable direct link. Unless force is set, a
// fresher link stored by another process path is reused without an API
// call. The same provider regenerates the link (re-adding the torrent by
// magnet when it was removed); otherwise another provider holding the same
// file takes over. Errors wrap core.ErrSourceUnavailable for transient or
// no-longer-cached outcomes (failover without marking the release bad) and
// core.ErrArticleMissing when the file itself is gone from the torrent.
func (s *Service) Refresh(ctx context.Context, p Payload, force bool) (Payload, error) {
	if !force {
		if stored, ok := s.storedPayload(ctx, p.Key); ok && s.fresh(stored) && stored.Path == p.Path && stored.Size == p.Size {
			return stored, nil
		}
		if s.fresh(p) {
			return p, nil
		}
	}
	var errs []error
	gone, missing := 0, 0
	note := func(err error) {
		switch {
		case errors.Is(err, errFileMissing):
			missing++
		case errors.Is(err, ErrGone) || errors.Is(err, core.ErrNotCached):
			gone++
		}
		errs = append(errs, err)
	}
	if e := s.entryByID(p.Provider); e != nil {
		if err := e.blocked(s.now()); err != nil {
			errs = append(errs, err)
		} else {
			out, err := s.relink(ctx, e, p)
			if err == nil {
				return out, nil
			}
			if ctx.Err() != nil {
				return p, ctx.Err()
			}
			note(err)
		}
	}
	// Another provider may hold the same torrent.
	for _, e := range s.active() {
		if e.cfg.ID == p.Provider || e.blocked(s.now()) != nil {
			continue
		}
		base := Payload{Key: p.Key, ReleaseID: p.ReleaseID, InfoHash: p.InfoHash, Magnet: p.Magnet, Target: p.Target, Release: p.Release}
		out, err := s.acquireOn(ctx, e, base, sameFile(p))
		if err == nil {
			e.count(func(st *ProviderStats) { st.LinksRefreshed++ })
			s.savePayload(ctx, out)
			s.log.Info("debrid release moved to another provider", "release", p.Release, "provider", displayName(e.cfg))
			return out, nil
		}
		if ctx.Err() != nil {
			return p, ctx.Err()
		}
		note(err)
	}
	joined := errors.Join(errs...)
	if joined == nil {
		joined = ErrNoProvider
	}
	switch {
	case missing > 0 && missing+gone == len(errs):
		return p, fmt.Errorf("debrid: %s is gone: %w (%v)", p.Name(), core.ErrArticleMissing, joined)
	case gone > 0 && gone+missing == len(errs):
		return p, fmt.Errorf("debrid: %s: %w: %w", p.Name(), core.ErrSourceUnavailable, core.ErrNotCached)
	}
	return p, fmt.Errorf("debrid: refresh link for %s: %w (%v)", p.Name(), core.ErrSourceUnavailable, joined)
}

// relink regenerates a link on the payload's own provider.
func (s *Service) relink(ctx context.Context, e *entry, p Payload) (Payload, error) {
	if err := e.acquire(ctx); err != nil {
		return p, err
	}
	lctx, cancel := context.WithTimeout(ctx, providerOpTimeout)
	start := s.now()
	dl, err := e.p.Link(lctx, p.handle())
	if err == nil || ctx.Err() == nil { // the caller's budget is not a provider failure
		s.observe(e, err)
	}
	cancel()
	e.release()
	if err == nil {
		s.noteLatency(e.cfg.ID, func(l *Latency) { l.LinkMs = ewma(l.LinkMs, ms(s.now().Sub(start)), l.Samples == 0) })
		p.URL, p.Expires = dl.URL, dl.Expires
		e.count(func(st *ProviderStats) { st.LinksRefreshed++ })
		s.savePayload(ctx, p)
		return p, nil
	}
	if errors.Is(err, ErrBlocked) {
		s.blockedFile(ctx, e, p.InfoHash, p.TorrentID, false, nil)
		return p, err
	}
	if !errors.Is(err, ErrGone) {
		return p, err
	}
	// The torrent was removed from the account: re-add it by magnet.
	base := Payload{Key: p.Key, ReleaseID: p.ReleaseID, InfoHash: p.InfoHash, Magnet: p.Magnet, Target: p.Target, Release: p.Release}
	s.forgetTorrent(ctx, e.cfg.ID, p.InfoHash)
	out, err := s.acquireOn(ctx, e, base, sameFile(p))
	if err != nil {
		return p, err
	}
	e.count(func(st *ProviderStats) { st.LinksRefreshed++ })
	s.savePayload(ctx, out)
	return out, nil
}

// blockedFile records a file the provider refuses to serve as not cached
// there, so ranking stops promoting it and the next Play skips it without
// API calls. A torrent Ether added for this attempt is removed again when
// remove is set and no node streams from it.
func (s *Service) blockedFile(ctx context.Context, e *entry, hash, torrentID string, remove bool, mayRemove func(context.Context, string) bool) {
	e.count(func(st *ProviderStats) { st.Blocked++ })
	if remove && torrentID != "" {
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		if mayRemove == nil || mayRemove(rctx, torrentID) {
			if err := e.p.Remove(rctx, torrentID); err == nil {
				s.forgetTorrent(rctx, e.cfg.ID, hash)
			}
		}
		cancel()
	}
	s.setKnown(ctx, e.cfg.ID, hash, "", false)
	s.log.Info("debrid provider blocks file", "provider", displayName(e.cfg), "hash", hash)
}

func (s *Service) forgetTorrent(ctx context.Context, providerID, hash string) {
	if s.db != nil {
		if _, err := s.db.ExecContext(ctx, `UPDATE debrid_torrents SET torrent_id = '' WHERE provider_id = ? AND info_hash = ?`, providerID, strings.ToLower(hash)); err != nil {
			s.log.Debug("debrid: forget torrent", "err", err)
		}
	}
}

// Test checks one provider config for the admin "test" button: the key
// works and the account can use the API.
func (s *Service) Test(ctx context.Context, cfg config.DebridProvider) error {
	p, err := s.opts.NewProvider(cfg, s.opts.ProviderOptions)
	if err != nil {
		return err
	}
	a, err := p.Account(ctx)
	if err != nil {
		if errors.Is(err, ErrAuth) {
			return fmt.Errorf("%s rejected the API key", displayName(cfg))
		}
		return err
	}
	if !a.Premium {
		return fmt.Errorf("%s account %q has no active premium subscription", displayName(cfg), a.User)
	}
	return nil
}
