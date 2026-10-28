package availability

import (
	"context"
	"database/sql"
	"errors"
	"github.com/braedonsaunders/ether/internal/core"
	"github.com/braedonsaunders/ether/internal/store"
	"time"
)

func init() {
	store.Register("availability", 3, `
 ALTER TABLE availability_feeds ADD COLUMN catchup_target TEXT NOT NULL DEFAULT '';
 ALTER TABLE availability_feeds ADD COLUMN catchup_tail TEXT NOT NULL DEFAULT '';
 ALTER TABLE availability_feeds ADD COLUMN catchup_offset INTEGER NOT NULL DEFAULT 0;
 -- Earlier monitors only seeded the newest page and discarded gap boundaries.
 -- Recover their recent history once, within the normal catch-up allowance.
 UPDATE availability_feeds SET catchup_target='__ether_recent_history__',gap=1,
 message='Recent feed history is queued for bounded catch-up';
 `)
	store.Register("availability", 2, `
 CREATE TABLE availability_feed_items (
 id TEXT NOT NULL, source_id TEXT NOT NULL, generation TEXT NOT NULL,
 match_title TEXT NOT NULL, payload TEXT NOT NULL, seen_at INTEGER NOT NULL,
 PRIMARY KEY(id,source_id));
 CREATE INDEX availability_feed_match ON availability_feed_items(match_title,seen_at);
 `)

	store.Register("availability", 1, `
CREATE TABLE availability_feeds (
 source_id TEXT PRIMARY KEY, generation TEXT NOT NULL, anchor TEXT NOT NULL DEFAULT '',
 checked_at INTEGER NOT NULL DEFAULT 0, synced_at INTEGER NOT NULL DEFAULT 0,
 gap INTEGER NOT NULL DEFAULT 0, message TEXT NOT NULL DEFAULT '', discovered INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE availability_releases (
 id TEXT NOT NULL, rating_key INTEGER NOT NULL, source_id TEXT NOT NULL, generation TEXT NOT NULL,
 payload TEXT NOT NULL, title TEXT NOT NULL, name TEXT NOT NULL, protocol TEXT NOT NULL,
 first_seen INTEGER NOT NULL, last_seen INTEGER NOT NULL, first_verified INTEGER NOT NULL DEFAULT 0,
 verified_at INTEGER NOT NULL DEFAULT 0, policy TEXT NOT NULL DEFAULT '', state TEXT NOT NULL DEFAULT 'observed',
 reason TEXT NOT NULL DEFAULT '', checked_id TEXT NOT NULL DEFAULT '', next_check INTEGER NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0,
 revision INTEGER NOT NULL DEFAULT 0, priority REAL NOT NULL DEFAULT 0, PRIMARY KEY(id,rating_key,source_id)
);
CREATE INDEX availability_queue ON availability_releases(next_check,priority);
CREATE INDEX availability_title ON availability_releases(rating_key,verified_at);
CREATE TABLE availability_budget(day TEXT NOT NULL, resource TEXT NOT NULL, used INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(day,resource));
`)
}
func (s *Service) spend(ctx context.Context, kind string, n, limit int64) error {
	if n <= 0 {
		return nil
	}
	if limit <= 0 {
		return core.ErrBudgetExhausted
	}
	day := s.now().UTC().Format("2006-01-02")
	if _, err := s.db.ExecContext(ctx, `INSERT INTO availability_budget(day,resource) VALUES(?,?) ON CONFLICT(day,resource) DO NOTHING`, day, kind); err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx, `UPDATE availability_budget SET used=used+? WHERE day=? AND resource=? AND used<=?`, n, day, kind, limit-n)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return core.ErrBudgetExhausted
	}
	return nil
}
func (s *Service) budgetContext(ctx context.Context, feed bool) context.Context {
	cfg := s.cfg().Availability
	ctx = core.WithForegroundReserve(ctx, cfg.ReservePercent)
	return core.WithBackgroundBudget(ctx, func(ctx context.Context, kind, source string, n int64) error {
		live := s.cfg().Availability
		if s.d.Active != nil && s.d.Active() && !feed {
			return context.Canceled
		}
		if (feed && !live.Monitor) || (!feed && !live.Verify) {
			return context.Canceled
		}
		switch kind {
		case "bytes":
			return s.spend(ctx, "bytes", n, live.CheckBytes)
		case "grab":
			return s.spend(ctx, "grabs", n, int64(live.Grabs))
		case "debrid":
			if !live.Debrid {
				return core.ErrBudgetExhausted
			}
		}
		bucket, limit := "checkRequests", live.CheckRequests
		if feed {
			bucket, limit = "feedRequests", live.FeedRequests
		}
		return s.spend(ctx, bucket, n, int64(limit))
	})
}
func (s *Service) remember(ctx context.Context, f core.ReleaseFeed, t core.Title, r core.Release, policy string) error {
	defer s.clearVisibility()
	raw, err := s.encode(r)
	if err != nil {
		return err
	}
	now := s.now().Unix()
	_, err = s.db.ExecContext(ctx, `INSERT INTO availability_releases(id,rating_key,source_id,generation,payload,title,name,protocol,first_seen,last_seen,priority,policy) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
 ON CONFLICT(id,rating_key,source_id) DO UPDATE SET last_seen=excluded.last_seen,payload=excluded.payload,priority=excluded.priority,
 verified_at=CASE WHEN availability_releases.generation<>excluded.generation THEN 0 ELSE availability_releases.verified_at END,
 first_verified=CASE WHEN availability_releases.generation<>excluded.generation THEN 0 ELSE availability_releases.first_verified END,
 next_check=CASE WHEN availability_releases.generation<>excluded.generation THEN 0 ELSE availability_releases.next_check END,
 state=CASE WHEN availability_releases.generation<>excluded.generation THEN 'observed' ELSE availability_releases.state END,
 generation=excluded.generation`, r.ID, t.Key, f.ID, f.Generation, raw, t.Title, r.Name, r.Protocol, now, now, t.Popularity, policy)
	return err
}
func (s *Service) prune(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM availability_feed_items WHERE seen_at<?`, s.now().Add(-61*24*time.Hour).Unix()); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM availability_feed_items WHERE (id,source_id) IN (SELECT id,source_id FROM availability_feed_items ORDER BY seen_at DESC LIMIT 2147483647 OFFSET 30000)`); err != nil {
		return err
	}

	cutoff := s.now().Add(-61 * 24 * time.Hour).Unix()
	if _, err := s.db.ExecContext(ctx, `DELETE FROM availability_releases WHERE last_seen<?`, cutoff); err != nil {
		return err
	}
	// Bound even very busy feeds; retaining recent/high-priority work is preferable
	// to letting monitoring consume an unbounded database.
	if _, err := s.db.ExecContext(ctx, `DELETE FROM availability_releases WHERE (id,rating_key,source_id) IN (SELECT id,rating_key,source_id FROM availability_releases ORDER BY last_seen DESC LIMIT 2147483647 OFFSET 30000)`); err != nil {
		// Retention failures are surfaced rather than silently growing the table.
		return err
	}
	_, err := s.db.ExecContext(ctx, `DELETE FROM availability_budget WHERE day<?`, s.now().Add(-35*24*time.Hour).UTC().Format("2006-01-02"))
	return err
}
func benign(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, core.ErrBudgetExhausted)
}
func (s *Service) feedState(ctx context.Context, f core.ReleaseFeed) (string, int64, bool, error) {
	var anchor string
	var checked int64
	var gap int
	err := s.db.QueryRowContext(ctx, `SELECT anchor,checked_at,gap FROM availability_feeds WHERE source_id=? AND generation=?`, f.ID, f.Generation).Scan(&anchor, &checked, &gap)
	if errors.Is(err, sql.ErrNoRows) {
		return "", 0, false, nil
	}
	return anchor, checked, gap != 0, err
}
func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
func safeReason(err error) string {
	switch {
	case errors.Is(err, core.ErrBudgetExhausted):
		return "Background allowance reached; playback capacity is reserved"
	case errors.Is(err, core.ErrArticleMissing):
		return "Sampled articles are missing on the configured providers"
	case errors.Is(err, core.ErrNotCached):
		return "Not cached by the configured debrid providers"
	case errors.Is(err, core.ErrReleaseRejected):
		return "Source was rejected by media validation"
	case errors.Is(err, context.DeadlineExceeded):
		return "Validation timed out; availability remains unknown"
	default:
		return "Source could not be checked; it will be retried"
	}
}
