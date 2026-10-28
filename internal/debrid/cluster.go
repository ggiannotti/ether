package debrid

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/braedonsaunders/ether/internal/clusterbus"
	"github.com/braedonsaunders/ether/internal/core"
	"github.com/braedonsaunders/ether/internal/store"
)

// Debrid accounts in a cluster.
//
// Several nodes share one debrid account, so account-wide behaviour is
// coordinated through the shared database and the cluster bus:
//
//   - Concurrency: each provider allows providerParallel API calls at once
//     per account; a clustered node uses its weighted share (SetConcurrencyShare,
//     recomputed by internal/app on membership changes).
//   - Back-off: a pause one node observes (rate limit, rejected key,
//     repeated failures) is broadcast on "debrid:backoff" and applied by
//     every node; a success after the pause broadcasts its end.
//   - Adding torrents: before adding a magnet a node claims (provider, hash)
//     in debrid_torrents; the others wait for the winner's torrent id, so
//     the account never holds the same torrent twice.
//   - Removal: a node streaming a torrent holds a lease in debrid_leases;
//     a torrent is removed from the account only when nobody holds one.
//   - Known-cached answers: a node that learns whether a hash is cached
//     publishes "invalidate:debrid" with the providerID|hash keys, and the
//     other nodes re-read those rows.
//
// A standalone server never calls SetCluster and keeps every one of these
// in-process, exactly as before.

func init() {
	store.Register("debrid", 2, `
ALTER TABLE debrid_torrents ADD COLUMN claimed_by TEXT NOT NULL DEFAULT '';
ALTER TABLE debrid_torrents ADD COLUMN claimed_at INTEGER NOT NULL DEFAULT 0;
CREATE TABLE debrid_leases (
  provider_id TEXT NOT NULL,
  torrent_id  TEXT NOT NULL,
  node_id     TEXT NOT NULL,
  expires_at  INTEGER NOT NULL,
  PRIMARY KEY (provider_id, torrent_id, node_id)
);
CREATE INDEX debrid_leases_node ON debrid_leases(node_id);`)
}

// TopicBackoff carries provider pauses between nodes.
const TopicBackoff = "debrid:backoff"

var (
	// claimStale is when another node may take over an add claim whose
	// holder never finished (crashed mid-add).
	claimStale = 2 * time.Minute
	// claimWait bounds how long a node waits for another node's add.
	claimWait = 60 * time.Second
	// claimPoll is how often a waiting node re-reads the claim.
	claimPoll = 250 * time.Millisecond
	// leaseRefresh and leaseTTL keep a streaming node's leases alive; a
	// crashed node's leases lapse after leaseTTL.
	leaseRefresh = 30 * time.Second
	leaseTTL     = 90 * time.Second
)

// SetCluster connects the cluster bus (nil disconnects). A standalone
// server never calls it.
func (s *Service) SetCluster(node clusterbus.Node) {
	s.clusterMu.Lock()
	old, unsubs := s.inval, s.unsubs
	s.inval, s.unsubs, s.node = nil, nil, node
	if node != nil {
		s.origin = clusterbus.NewOrigin()
		s.inval = clusterbus.NewInvalidator(node, "debrid", s.log, s.applyKnown)
		s.unsubs = []func(){node.Subscribe(TopicBackoff, s.onBackoff)}
	}
	s.clusterMu.Unlock()
	old.Close()
	for _, u := range unsubs {
		u()
	}
}

// clusterNode is the cluster surface, nil standalone.
func (s *Service) clusterNode() clusterbus.Node {
	s.clusterMu.RLock()
	defer s.clusterMu.RUnlock()
	return s.node
}

// ---------------------------------------------------------------------------
// Concurrency share.

// SetConcurrencyShare divides each provider's account-wide API concurrency
// among cluster nodes: share receives the account limit and returns this
// node's part, clamped to 1..limit. Nil restores the account limit. Calls
// in flight finish; the new limit applies to new calls.
func (s *Service) SetConcurrencyShare(share func(configured int) int) {
	s.clusterMu.Lock()
	s.share = share
	s.clusterMu.Unlock()
	s.mu.RLock()
	entries := append([]*entry(nil), s.entries...)
	s.mu.RUnlock()
	n := s.slotsFor(providerParallel)
	for _, e := range entries {
		e.sem.resize(n)
	}
}

// slotsFor is this node's share of an account-wide limit.
func (s *Service) slotsFor(configured int) int {
	s.clusterMu.RLock()
	share := s.share
	s.clusterMu.RUnlock()
	if share == nil {
		return configured
	}
	return min(max(share(configured), 1), configured)
}

// limiter is a counting semaphore whose size can change while in use.
type limiter struct {
	mu    sync.Mutex
	limit int
	used  int
	wake  chan struct{} // closed and replaced whenever a slot may be free
}

func newLimiter(n int) *limiter { return &limiter{limit: max(n, 1), wake: make(chan struct{})} }

func (l *limiter) acquire(ctx context.Context) error {
	for {
		l.mu.Lock()
		if l.used < l.limit {
			l.used++
			l.mu.Unlock()
			return nil
		}
		w := l.wake
		l.mu.Unlock()
		select {
		case <-w:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// reacquire is acquire for a caller that must end up holding a slot (its
// release is deferred): when ctx ends first the slot is taken anyway, over
// the limit, and ctx's error returned.
func (l *limiter) reacquire(ctx context.Context) error {
	err := l.acquire(ctx)
	if err != nil {
		l.mu.Lock()
		l.used++
		l.mu.Unlock()
	}
	return err
}

func (l *limiter) release() {
	l.mu.Lock()
	l.used--
	l.signalLocked()
	l.mu.Unlock()
}

func (l *limiter) resize(n int) {
	l.mu.Lock()
	l.limit = max(n, 1)
	l.signalLocked()
	l.mu.Unlock()
}

func (l *limiter) size() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.limit
}

func (l *limiter) signalLocked() {
	close(l.wake)
	l.wake = make(chan struct{})
}

// ---------------------------------------------------------------------------
// Back-off broadcast.

// backoffEvent is a provider pause (State "backoff" or "auth_failed", Until
// in Unix milliseconds) or, with State empty, the end of one.
type backoffEvent struct {
	Provider string `json:"provider"`
	State    string `json:"state,omitempty"`
	Until    int64  `json:"until,omitempty"`
	Error    string `json:"error,omitempty"`
}

// shortError trims a provider error for the bus and status pages.
func shortError(msg string) string {
	const limit = 200
	if len(msg) <= limit {
		return msg
	}
	return msg[:limit] + "…"
}

// observe updates e's breaker after a call and broadcasts a pause or its
// end to the other nodes.
func (s *Service) observe(e *entry, err error) {
	ev := e.observe(s.now(), err)
	if ev == nil {
		return
	}
	node := s.clusterNode()
	if node == nil {
		return
	}
	s.clusterMu.RLock()
	origin := s.origin
	s.clusterMu.RUnlock()
	raw, _ := json.Marshal(ev)
	ctx, cancel := context.WithTimeout(context.Background(), clusterbus.PublishTimeout)
	defer cancel()
	if err := node.Publish(ctx, TopicBackoff, clusterbus.Tag(origin, raw)); err != nil {
		s.log.Warn("debrid: publish provider back-off", "provider", ev.Provider, "err", err)
	}
}

func (s *Service) onBackoff(msg []byte) {
	origin, raw := clusterbus.Untag(msg)
	s.clusterMu.RLock()
	own := origin == s.origin
	s.clusterMu.RUnlock()
	if own {
		return
	}
	var ev backoffEvent
	if json.Unmarshal(raw, &ev) != nil || ev.Provider == "" {
		return
	}
	s.mu.RLock()
	var e *entry
	for _, x := range s.entries {
		if x.cfg.ID == ev.Provider {
			e = x
		}
	}
	s.mu.RUnlock()
	if e != nil {
		e.applyRemote(s.now(), ev)
	}
}

// applyRemote applies another node's pause, never beyond the longest pause
// this node would set itself and never shortening a longer local one; an
// end-of-pause event lifts the pause.
func (e *entry) applyRemote(now time.Time, ev backoffEvent) {
	e.mu.Lock()
	defer e.mu.Unlock()
	switch ev.State {
	case "":
		if e.state != "" || now.Before(e.until) {
			e.failures, e.state, e.until = 0, "", time.Time{}
		}
	case "backoff", "auth_failed":
		limit := maxBackoff
		if ev.State == "auth_failed" {
			limit = authBackoff
		}
		until := time.UnixMilli(ev.Until)
		if until.After(now.Add(limit)) {
			until = now.Add(limit)
		}
		if !until.After(now) || !until.After(e.until) {
			return
		}
		e.state, e.until = ev.State, until
		if ev.Error != "" {
			e.lastError = "another node: " + ev.Error
		}
	}
}

// ---------------------------------------------------------------------------
// Known-cached invalidation.

func knownKey(providerID, hash string) string { return providerID + "|" + hash }

// notifyKnown tells the other nodes these known-map keys changed.
func (s *Service) notifyKnown(keys ...string) {
	if len(keys) == 0 {
		return
	}
	s.clusterMu.RLock()
	iv := s.inval
	s.clusterMu.RUnlock()
	iv.Notify(strings.Join(keys, ","))
}

// applyKnown re-reads the keys another node changed (all: forget every
// remembered answer; knownFor reads the database again).
func (s *Service) applyKnown(ctx context.Context, hints []string, all bool) {
	if all {
		s.knownMu.Lock()
		s.known = map[string]knownState{}
		s.knownMu.Unlock()
		return
	}
	for _, h := range hints {
		for _, key := range strings.Split(h, ",") {
			provider, hash, ok := strings.Cut(key, "|")
			if ok && provider != "" && hash != "" {
				s.reloadKnown(ctx, provider, hash)
			}
		}
	}
}

func (s *Service) reloadKnown(ctx context.Context, providerID, hash string) {
	key := knownKey(providerID, hash)
	var cached int
	var at int64
	err := sql.ErrNoRows
	if s.db != nil {
		err = s.db.QueryRowContext(ctx, `SELECT cached, checked_at FROM debrid_torrents WHERE provider_id = ? AND info_hash = ?`,
			providerID, hash).Scan(&cached, &at)
	}
	s.knownMu.Lock()
	defer s.knownMu.Unlock()
	if err != nil || at <= 0 {
		delete(s.known, key)
		return
	}
	ttl := s.opts.UncachedTTL
	if cached == 1 {
		ttl = s.opts.CachedTTL
	}
	until := time.Unix(at, 0).Add(ttl)
	if !s.now().Before(until) {
		delete(s.known, key)
		return
	}
	s.known[key] = knownState{cached: cached == 1, until: until}
}

// ---------------------------------------------------------------------------
// Add claims.

// errClaimBusy is returned when another node's add did not finish within
// claimWait (a transient failure: the release is tried again later).
var errClaimBusy = errors.New("debrid: another node is still adding this torrent")

// errNotCachedElsewhere is another node's add finding the torrent uncached.
var errNotCachedElsewhere = fmt.Errorf("torrent is not cached (checked by another node): %w", core.ErrNotCached)

// claimAdd claims adding (provider, hash) to the account; the caller holds
// one of e's slots (released while waiting). It returns the
// torrent id another node added meanwhile (use it; nothing to add), or ""
// with release (call it when done) when this node won the claim. Without a
// cluster it returns at once. A database error lets the add proceed
// unclaimed: a duplicate torrent is better than no playback.
func (s *Service) claimAdd(ctx context.Context, e *entry, hash string) (string, func(), error) {
	noop := func() {}
	node := s.clusterNode()
	if node == nil || s.db == nil {
		return "", noop, nil
	}
	token := node.NodeID() + "/" + clusterbus.NewOrigin()
	deadline := s.now().Add(claimWait)
	for {
		tid, won, recentUncached, err := s.tryClaim(ctx, e.cfg.ID, hash, token)
		switch {
		case err != nil:
			if ctx.Err() != nil {
				return "", noop, ctx.Err()
			}
			s.log.Warn("debrid: claim torrent add", "provider", displayName(e.cfg), "err", err)
			return "", noop, nil
		case tid != "":
			return tid, noop, nil
		case won:
			return "", func() { s.releaseClaim(e.cfg.ID, hash, token) }, nil
		case recentUncached:
			// Another node's add just found the torrent uncached.
			return "", noop, fmt.Errorf("debrid: %s: %w", displayName(e.cfg), errNotCachedElsewhere)
		}
		if !s.now().Before(deadline) {
			return "", noop, errClaimBusy
		}
		// Wait without holding one of this node's provider slots: with a
		// small share, a waiting add must not stall every other call.
		e.release()
		err = sleep(ctx, claimPoll)
		if aerr := e.sem.reacquire(ctx); err == nil {
			err = aerr
		}
		if err != nil {
			return "", noop, err
		}
	}
}

// reclaimAdd claims re-adding a torrent whose remembered id stale turned
// out gone: the stale id is forgotten (unless another node already
// replaced it) and the add is claimed as in claimAdd.
func (s *Service) reclaimAdd(ctx context.Context, e *entry, hash, stale string) (string, func(), error) {
	if s.clusterNode() == nil || s.db == nil {
		return "", func() {}, nil
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE debrid_torrents SET torrent_id = '' WHERE provider_id = ? AND info_hash = ? AND torrent_id = ?`,
		e.cfg.ID, hash, stale); err != nil {
		s.log.Debug("debrid: forget stale torrent", "err", err)
	}
	id, release, err := s.claimAdd(ctx, e, hash)
	if id == stale {
		id = "" // nobody re-added it after all
	}
	return id, release, err
}

// tryClaim takes the add claim when nobody recorded a torrent id, no fresh
// claim is held and the torrent was not just found uncached; otherwise it reports the recorded torrent id and
// whether the row says the torrent was recently found uncached.
func (s *Service) tryClaim(ctx context.Context, providerID, hash, token string) (tid string, won, recentUncached bool, err error) {
	now := s.now()
	// checked_at 0 on a new row: a claim is not an availability answer.
	res, err := s.db.ExecContext(ctx, `INSERT INTO debrid_torrents (provider_id, info_hash, torrent_id, cached, checked_at, claimed_by, claimed_at)
VALUES (?, ?, '', 0, 0, ?, ?)
ON CONFLICT (provider_id, info_hash) DO UPDATE SET claimed_by = excluded.claimed_by, claimed_at = excluded.claimed_at
WHERE debrid_torrents.torrent_id = '' AND (debrid_torrents.claimed_by = '' OR debrid_torrents.claimed_at < ?)
  AND NOT (debrid_torrents.cached = 0 AND debrid_torrents.checked_at > ?)`,
		providerID, hash, token, now.Unix(), now.Add(-claimStale).Unix(), now.Add(-s.opts.UncachedTTL).Unix())
	if err != nil {
		return "", false, false, err
	}
	if n, err := res.RowsAffected(); err != nil {
		return "", false, false, err
	} else if n == 1 {
		return "", true, false, nil
	}
	var cached int
	var at int64
	err = s.db.QueryRowContext(ctx, `SELECT torrent_id, cached, checked_at FROM debrid_torrents WHERE provider_id = ? AND info_hash = ?`,
		providerID, hash).Scan(&tid, &cached, &at)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, false, nil // deleted meanwhile: claim again
	}
	if err != nil {
		return "", false, false, err
	}
	recentUncached = cached == 0 && at > 0 && now.Sub(time.Unix(at, 0)) < s.opts.UncachedTTL && tid == ""
	return tid, false, recentUncached, nil
}

func (s *Service) releaseClaim(providerID, hash, token string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := s.db.ExecContext(ctx, `UPDATE debrid_torrents SET claimed_by = '', claimed_at = 0 WHERE provider_id = ? AND info_hash = ? AND claimed_by = ?`,
		providerID, hash, token); err != nil {
		s.log.Debug("debrid: release torrent add claim", "err", err)
	}
}

// ---------------------------------------------------------------------------
// Streaming leases.

// mayRemove is the AcquireRequest.MayRemove check for a provider: false
// while any node holds an unexpired lease on the torrent (or the leases
// cannot be read). Nil standalone.
func (s *Service) mayRemove(providerID string) func(context.Context, string) bool {
	if s.clusterNode() == nil || s.db == nil {
		return nil
	}
	return func(ctx context.Context, torrentID string) bool {
		var n int
		err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM debrid_leases WHERE provider_id = ? AND torrent_id = ? AND expires_at > ?`,
			providerID, torrentID, s.now().Unix()).Scan(&n)
		if err != nil {
			s.log.Warn("debrid: torrent kept on the account (streaming leases unreadable)", "provider", providerID, "err", err)
			return false
		}
		if n > 0 {
			s.log.Info("debrid: torrent kept on the account: a node is streaming from it", "provider", providerID, "torrent", torrentID)
		}
		return n == 0
	}
}

type leaseKey struct{ provider, torrent string }

// kickLeases asks the lease worker to bring this node's leases in line with
// its open files (clustered only; the worker starts on first use).
func (e *Engine) kickLeases() {
	node := e.svc.clusterNode()
	if node == nil || e.svc.db == nil {
		return
	}
	e.leaseOnce.Do(func() {
		e.leaseKick = make(chan struct{}, 1)
		e.leaseDone = make(chan struct{})
		go e.runLeases(node.NodeID())
	})
	select {
	case e.leaseKick <- struct{}{}:
	default:
	}
}

// RefreshLeases renews this node's streaming leases now (clustered only),
// e.g. as soon as the database is reachable again after an outage longer
// than the lease lifetime, so no node removes a torrent this node still
// streams from.
func (e *Engine) RefreshLeases() { e.kickLeases() }

func (e *Engine) runLeases(self string) {
	defer close(e.leaseDone)
	t := time.NewTicker(leaseRefresh)
	defer t.Stop()
	for {
		select {
		case <-e.ctx.Done():
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if _, err := e.svc.db.ExecContext(ctx, `DELETE FROM debrid_leases WHERE node_id = ?`, self); err != nil {
				e.log.Debug("debrid: delete streaming leases", "err", err)
			}
			cancel()
			return
		case <-e.leaseKick:
		case <-t.C:
		}
		e.syncLeases(self)
	}
}

// openTorrents is the set of torrents this node's open files stream from.
func (e *Engine) openTorrents() map[leaseKey]bool {
	e.mu.Lock()
	files := make([]*remoteFile, 0, len(e.files))
	for _, f := range e.files {
		if f.refs > 0 {
			files = append(files, f)
		}
	}
	e.mu.Unlock()
	out := map[leaseKey]bool{}
	for _, f := range files {
		p, _ := f.payload()
		if p.TorrentID != "" {
			out[leaseKey{p.Provider, p.TorrentID}] = true
		}
	}
	return out
}

// syncLeases refreshes a lease for every open torrent and drops this node's
// leases on torrents it no longer streams.
func (e *Engine) syncLeases(self string) {
	ctx, cancel := context.WithTimeout(e.ctx, 10*time.Second)
	defer cancel()
	db := e.svc.db
	open := e.openTorrents()
	expires := e.svc.now().Add(leaseTTL).Unix()
	for k := range open {
		if _, err := db.ExecContext(ctx, `INSERT INTO debrid_leases (provider_id, torrent_id, node_id, expires_at) VALUES (?, ?, ?, ?)
ON CONFLICT (provider_id, torrent_id, node_id) DO UPDATE SET expires_at = excluded.expires_at`, k.provider, k.torrent, self, expires); err != nil {
			e.log.Debug("debrid: refresh streaming lease", "err", err)
		}
	}
	rows, err := db.QueryContext(ctx, `SELECT provider_id, torrent_id FROM debrid_leases WHERE node_id = ?`, self)
	if err != nil {
		return
	}
	var stale []leaseKey
	for rows.Next() {
		var k leaseKey
		if rows.Scan(&k.provider, &k.torrent) == nil && !open[k] {
			stale = append(stale, k)
		}
	}
	if err := rows.Err(); err != nil {
		e.log.Debug("debrid: list streaming leases", "err", err)
	}
	rows.Close()
	for _, k := range stale {
		if _, err := db.ExecContext(ctx, `DELETE FROM debrid_leases WHERE provider_id = ? AND torrent_id = ? AND node_id = ?`, k.provider, k.torrent, self); err != nil {
			e.log.Debug("debrid: delete stale streaming lease", "err", err)
		}
	}
}
