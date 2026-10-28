package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/braedonsaunders/ether/internal/core"
	"github.com/braedonsaunders/ether/internal/schedule"
	"github.com/braedonsaunders/ether/internal/store"
)

// One durable global chart per window and media type, shared by every viewer.
// Never substitute popularity or a production-country filter for audience trends.
const trendPoolSize = 200
const trendRefresh = time.Hour
const trendRetry = 5 * time.Minute
const trendMaxAge = 7 * 24 * time.Hour

func init() {
	store.Register("catalog", 8, `CREATE TABLE catalog_trends (
 name TEXT NOT NULL, media_type TEXT NOT NULL, items TEXT NOT NULL DEFAULT '[]',
 fetched_at INTEGER NOT NULL DEFAULT 0, attempted_at INTEGER NOT NULL DEFAULT 0,
 failed INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(name,media_type));`)
}

type TrendStatus struct {
	Source      string         `json:"source"`
	Scope       string         `json:"scope"`
	Window      string         `json:"window"`
	MediaType   core.MediaType `json:"mediaType"`
	Titles      int            `json:"titles"`
	FetchedAt   *time.Time     `json:"fetchedAt,omitempty"`
	AttemptedAt *time.Time     `json:"attemptedAt,omitempty"`
	State       string         `json:"state"`
}

type trendSnapshot struct {
	items              []core.Title
	fetched, attempted time.Time
	failed             bool
}

func (c *Catalog) loadTrend(ctx context.Context, name string, typ core.MediaType) trendSnapshot {
	var raw string
	var fetched, attempted int64
	var failed int
	if c.db.QueryRowContext(ctx, `SELECT items,fetched_at,attempted_at,failed FROM catalog_trends WHERE name=? AND media_type=?`, name, string(typ)).Scan(&raw, &fetched, &attempted, &failed) != nil {
		return trendSnapshot{}
	}
	s := trendSnapshot{failed: failed != 0}
	if json.Unmarshal([]byte(raw), &s.items) != nil {
		return trendSnapshot{}
	}
	if fetched > 0 {
		s.fetched = time.Unix(fetched, 0)
	}
	if attempted > 0 {
		s.attempted = time.Unix(attempted, 0)
	}
	return s
}
func (c *Catalog) trendDue(s trendSnapshot) bool {
	if s.failed {
		return schedule.Due(c.now(), s.attempted, trendRetry)
	}
	return s.fetched.IsZero() || schedule.Due(c.now(), s.fetched, trendRefresh)
}
func (c *Catalog) refreshTrend(ctx context.Context, name string, typ core.MediaType) (trendSnapshot, error) {
	return c.trendFlights.Do(ctx, name+string(typ), func(ctx context.Context) (trendSnapshot, error) {
		old := c.loadTrend(ctx, name, typ)
		if !c.trendDue(old) {
			return old, nil
		}
		if c.client == nil {
			return old, errors.New("TMDB is not configured")
		}
		parent := ctx
		ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
		defer cancel()
		items, err := c.fetchList(ctx, name, typ, trendPoolSize)
		if err == nil && len(items) == 0 {
			err = errors.New("empty trending response")
		}
		if err == nil {
			err = c.upsertSummaries(ctx, items, false)
		}
		if err != nil {
			// Keep the entire last successful chart, not a partial page or an empty outage.
			if parent.Err() == nil {
				if _, merr := c.db.ExecContext(parent, `INSERT INTO catalog_trends(name,media_type,attempted_at,failed) VALUES(?,?,?,1) ON CONFLICT(name,media_type) DO UPDATE SET attempted_at=excluded.attempted_at,failed=1`, name, string(typ), c.now().Unix()); merr != nil {
					c.log.Debug("catalog: mark trend refresh failed", "name", name, "type", typ, "err", merr)
				}
			}
			return old, err
		}
		raw, err := json.Marshal(items)
		if err != nil {
			return old, err
		}
		at := c.now()
		_, err = c.db.ExecContext(ctx, `INSERT INTO catalog_trends(name,media_type,items,fetched_at,attempted_at,failed) VALUES(?,?,?,?,?,0) ON CONFLICT(name,media_type) DO UPDATE SET items=excluded.items,fetched_at=excluded.fetched_at,attempted_at=excluded.attempted_at,failed=0`, name, string(typ), string(raw), at.Unix(), at.Unix())
		if err != nil {
			return old, err
		}
		return trendSnapshot{items: items, fetched: at, attempted: at}, nil
	})
}
func (c *Catalog) trendingList(ctx context.Context, name string, typ core.MediaType, limit int) ([]core.Title, error) {
	if typ == "" {
		a, ea := c.trendingList(ctx, name, core.TypeMovie, limit)
		b, eb := c.trendingList(ctx, name, core.TypeShow, limit)
		out := make([]core.Title, 0, len(a)+len(b))
		for i := 0; i < len(a) || i < len(b); i++ {
			if i < len(a) {
				out = append(out, a[i])
			}
			if i < len(b) {
				out = append(out, b[i])
			}
		}
		if len(out) == 0 {
			return nil, errors.Join(ea, eb)
		}
		return limitTitles(out, limit), nil
	}
	s := c.loadTrend(ctx, name, typ)
	var err error
	if s.fetched.IsZero() {
		s, err = c.refreshTrend(ctx, name, typ)
	}
	if s.fetched.IsZero() || c.now().Sub(s.fetched) > trendMaxAge {
		return nil, err
	}
	// Availability and viewer policy are applied by the ordinary callers, never bypassed.
	keys := make([]core.RatingKey, 0, len(s.items))
	for _, t := range s.items {
		keys = append(keys, t.Key)
	}
	hydrated, loadErr := c.titlesByKeys(ctx, keys)
	if loadErr != nil {
		return nil, loadErr
	}
	byKey := map[core.RatingKey]core.Title{}
	for _, t := range hydrated {
		byKey[t.Key] = t
	}
	ordered := make([]core.Title, 0, len(s.items))
	for _, t := range s.items {
		if full, ok := byKey[t.Key]; ok {
			ordered = append(ordered, full)
		}
	}
	return limitTitles(c.releasedTitles(ordered), limit), nil
}

// RunTrends has an independent schedule: a slow catalogue crawl cannot delay
// charts. App lifetime owns and joins it, and only the cluster leader polls.
func (c *Catalog) RunTrends(ctx context.Context) {
	for ctx.Err() == nil {
		for _, typ := range []core.MediaType{core.TypeMovie, core.TypeShow} {
			for _, name := range []string{"trending_day", "trending_week"} {
				if ctx.Err() != nil {
					return
				}
				if _, err := c.refreshTrend(ctx, name, typ); err != nil && ctx.Err() == nil {
					c.log.Warn("catalog: trending refresh unavailable; retaining previous chart", "media", typ, "window", name)
				}
			}
		}
		if !schedule.Wait(ctx, c.now, time.Minute) {
			return
		}
	}
}
func (c *Catalog) TrendStatus(ctx context.Context) []TrendStatus {
	out := []TrendStatus{}
	for _, typ := range []core.MediaType{core.TypeMovie, core.TypeShow} {
		for _, window := range []string{"day", "week"} {
			s := c.loadTrend(ctx, "trending_"+window, typ)
			st := TrendStatus{Source: "TMDB", Scope: "Global", Window: window, MediaType: typ, Titles: len(s.items), State: "waiting"}
			if !s.fetched.IsZero() {
				st.FetchedAt = &s.fetched
				st.State = "fresh"
				if c.trendDue(s) || s.failed {
					st.State = "stale"
				}
				if c.now().Sub(s.fetched) > trendMaxAge {
					st.State = "expired"
				}
			}
			if !s.attempted.IsZero() {
				st.AttemptedAt = &s.attempted
			}
			if c.client == nil {
				st.State = "unconfigured"
			}
			out = append(out, st)
		}
	}
	return out
}
