// Package availability discovers releases incrementally and spends a bounded
// background allowance validating useful candidates. Recent releases require
// evidence for discovery; older catalogue entries need no background search.
package availability

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/braedonsaunders/ether/internal/config"
	"github.com/braedonsaunders/ether/internal/core"
	"github.com/braedonsaunders/ether/internal/schedule"
)

type Feeds interface {
	FeedSources() []core.ReleaseFeed
	Recent(context.Context, string, int, int) ([]core.Release, error)
}
type Catalog interface {
	ReleaseMatches(context.Context, []core.Release) (map[string][]core.Title, error)
	CachedTitle(context.Context, core.RatingKey) (*core.Title, error)
	CachedTitles(context.Context, []core.RatingKey) ([]core.Title, error)
}
type Validator interface {
	FeedCandidate(context.Context, *core.Title, core.Release) (core.Release, string, error)
	VerifyFeedCandidate(context.Context, *core.Title, core.Release) error
}
type Deps struct {
	DB         *sql.DB
	Config     func() config.Config
	Feeds      Feeds
	Catalog    Catalog
	Validator  Validator
	Seal       func(string) (string, error)
	Open       func(string) (string, error)
	CacheCheck func(context.Context, []string) (map[string]bool, bool)
	Active     func() bool
	Now        func() time.Time
	Logger     *slog.Logger
}
type Service struct {
	visibilityMu       sync.Mutex
	visibilityRevision uint64
	visibilityCache    map[string]visibilityEntry
	db                 *sql.DB
	cfg                func() config.Config
	d                  Deps
	now                func() time.Time
	log                *slog.Logger
	runMu              sync.Mutex
	cacheSeeded        bool
}

func New(d Deps) (*Service, error) {
	if d.DB == nil || d.Config == nil || d.Feeds == nil || d.Catalog == nil || d.Validator == nil {
		return nil, errors.New("availability: missing dependencies")
	}
	if d.Now == nil {
		d.Now = time.Now
	}
	if d.Logger == nil {
		d.Logger = slog.Default()
	}
	return &Service{db: d.DB, cfg: d.Config, d: d, now: d.Now, log: d.Logger}, nil
}
func (s *Service) encode(r core.Release) (string, error) {
	b, err := json.Marshal(r)
	if err != nil {
		return "", err
	}
	if s.d.Seal != nil {
		return s.d.Seal(string(b))
	}
	return string(b), nil
}
func (s *Service) decode(raw string) (core.Release, error) {
	var r core.Release
	var err error
	if s.d.Open != nil {
		raw, err = s.d.Open(raw)
		if err != nil {
			return r, err
		}
	}
	err = json.Unmarshal([]byte(raw), &r)
	return r, err
}
func (s *Service) policy() string {
	c := s.cfg()
	b, _ := json.Marshal([]any{"source-suitability-v2", c.Quality, c.Resolver, c.Usenet, c.Debrid, c.Indexers})
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
func selected(ids []string, id string) bool {
	if len(ids) == 0 {
		return true
	}
	for _, x := range ids {
		if x == id {
			return true
		}
	}
	return false
}
func (s *Service) usable(protocol string) bool {
	c := s.cfg()
	if protocol == core.ProtocolTorrent {
		if !c.Availability.Debrid {
			return false
		}
		for _, p := range c.Debrid.Providers {
			if p.Enabled && p.APIKey != "" {
				return true
			}
		}
	} else {
		for _, p := range c.Usenet.Providers {
			if p.Enabled && p.Host != "" {
				return true
			}
		}
	}
	return false
}
func (s *Service) Run(ctx context.Context) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if err := s.Cycle(ctx); err != nil && !benign(err) {
				s.log.Warn("availability cycle failed", "error", err)
			}
			timer.Reset(time.Minute)
		}
	}
}
func (s *Service) Cycle(ctx context.Context) error {
	if !s.runMu.TryLock() {
		return nil
	}
	defer s.runMu.Unlock()
	cfg := s.cfg().Availability
	if !s.cacheSeeded {
		if err := s.seedCached(ctx); err != nil {
			return err
		}
		s.cacheSeeded = true
	}
	if cfg.Monitor {
		feeds := s.d.Feeds.FeedSources()
		type due struct {
			f  core.ReleaseFeed
			at int64
		}
		var work []due
		for _, f := range feeds {
			if !f.Supported || !selected(cfg.IndexerIDs, f.ID) {
				continue
			}
			_, at, _, err := s.feedState(ctx, f)
			if err != nil {
				return err
			}
			if at == 0 || schedule.Due(s.now(), time.Unix(at, 0), time.Duration(max(5, cfg.PollMinutes))*time.Minute) {
				work = append(work, due{f, at})
			}
		}
		sort.SliceStable(work, func(i, j int) bool { return work[i].at < work[j].at })
		for _, w := range work {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if !s.cfg().Availability.Monitor {
				break
			}
			if err := s.syncFeed(ctx, w.f); err != nil {
				if benign(err) {
					continue // A paused indexer must not starve other feeds.
				}
				s.log.Warn("release feed sync failed", "source", w.f.Name, "error", err)
			}
		}
	}
	if cfg.Verify && (s.d.Active == nil || !s.d.Active()) {
		if err := s.verify(ctx); err != nil && !benign(err) {
			return err
		}
	}
	return s.prune(ctx)
}

type queued struct {
	id                              string
	key                             core.RatingKey
	source, generation, raw, policy string
	attempts                        int
	revision                        int64
}

func (s *Service) verify(parent context.Context) error {
	defer s.clearVisibility()
	policy := s.policy()
	now := s.now().Unix()
	sources := map[string]core.ReleaseFeed{}
	var allowed []string
	args := []any{now, policy}
	for _, f := range s.d.Feeds.FeedSources() {
		sources[f.ID] = f
		if selected(s.cfg().Availability.IndexerIDs, f.ID) && s.usable(f.Protocol) {
			allowed = append(allowed, "(source_id=? AND generation=?)")
			args = append(args, f.ID, f.Generation)
		}
	}
	if len(allowed) == 0 {
		return nil
	}
	// A config change reconsiders retained releases without querying an indexer.
	rows, err := s.db.QueryContext(parent, `SELECT id,rating_key,source_id,generation,payload,policy,attempts,revision FROM (
 SELECT id,rating_key,source_id,generation,payload,policy,attempts,revision,priority,first_seen,
 ROW_NUMBER() OVER (PARTITION BY rating_key ORDER BY CASE WHEN verified_at=0 THEN 0 ELSE 1 END,last_seen DESC,id) AS position
 FROM availability_releases WHERE (next_check<=? OR policy<>?) AND (`+strings.Join(allowed, " OR ")+`)
 ) AS candidates WHERE position=1 ORDER BY priority DESC,first_seen DESC LIMIT 256`, args...)
	if err != nil {
		return err
	}
	var work []queued
	for rows.Next() {
		var q queued
		if err = rows.Scan(&q.id, &q.key, &q.source, &q.generation, &q.raw, &q.policy, &q.attempts, &q.revision); err != nil {
			rows.Close()
			return err
		}
		work = append(work, q)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	// Preflight locally before spending any provider calls. Deferred rows move
	// out of the queue so disabled sources cannot starve useful candidates.
	var eligible []queued
	hashes := []string{}
	cfg := s.cfg().Availability
	fresh := map[core.RatingKey]bool{}
	for _, k := range s.verifiedKeys(parent) {
		fresh[k] = true
	}
	for _, q := range work {
		if fresh[q.key] {
			_, e := s.db.ExecContext(parent, `UPDATE availability_releases SET next_check=?,policy=? WHERE rating_key=?`, now+int64((6*time.Hour).Seconds()), policy, q.key)
			if e != nil {
				return e
			}
			continue
		}
		f, ok := sources[q.source]
		r, e := s.decode(q.raw)
		if !ok || f.Generation != q.generation || !selected(cfg.IndexerIDs, q.source) || e != nil || !s.usable(r.Protocol) {
			_, e = s.db.ExecContext(parent, `UPDATE availability_releases SET next_check=?,policy=? WHERE id=? AND rating_key=? AND source_id=?`, now+3600, policy, q.id, q.key, q.source)
			if e != nil {
				return e
			}
			continue
		}
		t, e := s.d.Catalog.CachedTitle(parent, q.key)
		if e != nil || t == nil || !t.IsReleased(s.now()) {
			_, err := s.db.ExecContext(parent, `UPDATE availability_releases SET next_check=?,policy=? WHERE id=? AND rating_key=? AND source_id=?`, now+3600, policy, q.id, q.key, q.source)
			if err != nil {
				return err
			}
			continue
		}
		_, reason, e := s.d.Validator.FeedCandidate(parent, t, r)
		if e != nil {
			return e
		}
		if reason != "" {
			_, e = s.db.ExecContext(parent, `UPDATE availability_releases SET state='policy',reason=?,verified_at=0,policy=?,next_check=? WHERE id=? AND rating_key=? AND source_id=?`, reason, policy, now+86400, q.id, q.key, q.source)
			if e != nil {
				return e
			}
			continue
		}
		eligible = append(eligible, q)
		if r.InfoHash != "" {
			hashes = append(hashes, r.InfoHash)
		}
		if len(eligible) >= 24 {
			break
		}
	}
	work = eligible
	// Do not issue cache lookups once the daily validation allowance is spent.
	var used int64
	_ = s.db.QueryRowContext(parent, `SELECT used FROM availability_budget WHERE day=? AND resource='checks'`, s.now().UTC().Format("2006-01-02")).Scan(&used)
	if cfg.Checks <= 0 || used >= int64(cfg.Checks) {
		return core.ErrBudgetExhausted
	}
	if len(hashes) > 0 && s.d.CacheCheck != nil {
		bctx, cancel := context.WithTimeout(s.budgetContext(parent, false), 15*time.Second)
		cached, definitive := s.d.CacheCheck(bctx, hashes)
		cancel()
		kept := work[:0]
		preferred := map[string]bool{}
		for _, q := range work {
			r, _ := s.decode(q.raw)
			if r.Protocol == core.ProtocolTorrent && r.InfoHash != "" && definitive && !cached[r.InfoHash] {
				_, e := s.db.ExecContext(parent, `UPDATE availability_releases SET state='unavailable',reason='Not cached by the configured debrid providers',verified_at=0,policy=?,next_check=? WHERE id=? AND rating_key=? AND source_id=?`, policy, now+3600, q.id, q.key, q.source)
				if e != nil {
					return e
				}
				continue
			}
			preferred[q.id] = cached[r.InfoHash]
			kept = append(kept, q)
		}
		work = kept
		sort.SliceStable(work, func(i, j int) bool { return preferred[work[i].id] && !preferred[work[j].id] })
	}
	for _, q := range work {
		c := s.cfg().Availability
		if parent.Err() != nil {
			return parent.Err()
		}
		if !c.Verify || (s.d.Active != nil && s.d.Active()) {
			return nil
		}
		f, ok := sources[q.source]
		if !ok || f.Generation != q.generation || !selected(c.IndexerIDs, q.source) {
			continue
		}
		r, e := s.decode(q.raw)
		if e != nil {
			return e
		}
		if !s.usable(r.Protocol) {
			continue
		}
		t, e := s.d.Catalog.CachedTitle(parent, q.key)
		if e != nil || t == nil || !t.IsReleased(s.now()) {
			_, _ = s.db.ExecContext(parent, `UPDATE availability_releases SET next_check=?,policy=? WHERE id=? AND rating_key=? AND source_id=?`, now+3600, policy, q.id, q.key, q.source)
			continue
		}
		r, reason, e := s.d.Validator.FeedCandidate(parent, t, r)
		state := "eligible"
		verified := int64(0)
		retry := s.now().Add(6 * time.Hour).Unix()
		if e != nil {
			return e
		}
		if reason != "" {
			state = "policy"
			retry = s.now().Add(24 * time.Hour).Unix()
		} else {
			if e = s.spend(parent, "checks", 1, int64(c.Checks)); e != nil {
				return e
			}
			ctx, stop := context.WithTimeout(s.budgetContext(parent, false), 20*time.Second)
			e = s.d.Validator.VerifyFeedCandidate(ctx, t, r)
			stop()
			if parent.Err() != nil {
				return parent.Err()
			}
			switch {
			case e == nil:
				state = "verified"
				verified = s.now().Unix()
				reason = "Source health checked for this title and quality policy"
			case errors.Is(e, core.ErrBudgetExhausted):
				state, reason = "unknown", safeReason(e)
				retry = s.now().Add(time.Hour).Unix()
			default:
				state = "unknown"
				reason = safeReason(e)
				if errors.Is(e, core.ErrArticleMissing) || errors.Is(e, core.ErrReleaseRejected) || errors.Is(e, core.ErrNotCached) {
					state = "unavailable"
				}
				// Recheck only discovered candidates, backing off repeated outcomes.
				retry = s.now().Add(time.Duration(min(360, 5*(1<<min(q.attempts, 6)))) * time.Minute).Unix()
			}
		}
		_, err = s.db.ExecContext(parent, `UPDATE availability_releases SET state=?,reason=?,verified_at=?,checked_id=?,
 first_verified=CASE WHEN first_verified=0 AND ?>0 THEN ? ELSE first_verified END,
 policy=?,next_check=?,attempts=attempts+1 WHERE id=? AND rating_key=? AND source_id=? AND generation=? AND revision=?`, state, reason, verified, r.ID, verified, verified, policy, retry, q.id, q.key, q.source, q.generation, q.revision)
		if err != nil {
			return err
		}
	}
	return nil
}

// Observe reuses an actual successful resolver health check. Optimistic starts
// and prepared sessions without health evidence do not call this method.
func (s *Service) Observe(ctx context.Context, t *core.Title, r core.Release) {
	defer s.clearVisibility()
	if t == nil || !t.IsReleased(s.now()) {
		return
	}
	for _, f := range s.d.Feeds.FeedSources() {
		if f.Name == r.Indexer {
			policy := s.policy()
			if err := s.remember(ctx, f, *t, r, policy); err != nil {
				return
			}
			var revision int64
			if err := s.db.QueryRowContext(ctx, `SELECT revision FROM availability_releases WHERE id=? AND rating_key=? AND source_id=?`, r.ID, t.Key, f.ID).Scan(&revision); err != nil {
				return
			}
			_, reason, err := s.d.Validator.FeedCandidate(ctx, t, r)
			if err != nil || reason != "" {
				return
			}
			now := s.now().Unix()
			_, err = s.db.ExecContext(ctx, `UPDATE availability_releases SET state='verified',reason='Health check reused from playback resolution',verified_at=?,first_verified=CASE WHEN first_verified=0 THEN ? ELSE first_verified END,policy=?,next_check=? WHERE id=? AND rating_key=? AND source_id=? AND revision=? AND generation=?`, now, now, policy, now+int64((6*time.Hour).Seconds()), r.ID, t.Key, f.ID, revision, f.Generation)
			if err != nil {
				s.log.Debug("availability observation could not be saved", "error", err)
			}
			return
		}
	}
}

// Candidates serves remembered descriptors locally; playback still applies its
// exact viewer/device policy and validates the selected source.
func (s *Service) Candidates(ctx context.Context, t *core.Title) []core.Release {
	rows, err := s.db.QueryContext(ctx, `SELECT source_id,generation,payload FROM availability_releases WHERE rating_key=? ORDER BY verified_at DESC,last_seen DESC LIMIT 24`, t.Key)
	if err != nil {
		return nil
	}
	defer rows.Close()
	current := map[string]string{}
	for _, f := range s.d.Feeds.FeedSources() {
		current[f.ID] = f.Generation
	}
	var out []core.Release
	for rows.Next() {
		var id, gen, raw string
		if rows.Scan(&id, &gen, &raw) != nil {
			continue
		}
		if current[id] != gen {
			continue
		}
		if r, e := s.decode(raw); e == nil {
			out = append(out, r)
		}
	}
	if err := rows.Err(); err != nil {
		s.log.Warn("availability candidates not fully loaded", "err", err)
	}
	return out
}

// Invalidate retracts positive evidence immediately when playback or the owner
// invalidates a release. The title and its other sources remain discoverable.
func (s *Service) Invalidate(ctx context.Context, id string) {
	defer s.clearVisibility()
	_, err := s.db.ExecContext(ctx, `UPDATE availability_releases SET revision=revision+1,verified_at=0,state='unknown',reason='Source health invalidated; awaiting a fresh check',next_check=0 WHERE id=? OR checked_id=?`, id, id)
	if err != nil {
		s.log.Debug("availability invalidation failed", "error", err)
	}
}
