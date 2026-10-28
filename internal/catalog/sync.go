package catalog

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/braedonsaunders/ether/internal/core"
	"github.com/braedonsaunders/ether/internal/schedule"
	"github.com/braedonsaunders/ether/internal/tmdb"
)

// SyncProgress describes a running or last-finished catalogue sync.
type SyncProgress struct {
	Running     bool      `json:"running"`
	Phase       string    `json:"phase,omitempty"` // e.g. "movie/popularity", "tv/votes/1994"
	PagesDone   int       `json:"pagesDone"`
	TitlesSeen  int       `json:"titlesSeen"`
	TitlesAdded int       `json:"titlesAdded"`
	StartedAt   time.Time `json:"startedAt,omitempty"`
	FinishedAt  time.Time `json:"finishedAt,omitempty"`
	LastError   string    `json:"lastError,omitempty"`
}

// Stats is the admin-UI snapshot of the mirror.
type Stats struct {
	Trending        []TrendStatus `json:"trending"`
	Movies          int           `json:"movies"`
	Shows           int           `json:"shows"`
	Seasons         int           `json:"seasons"`
	Episodes        int           `json:"episodes"`
	BrowsableMovies int           `json:"browsableMovies"`
	BrowsableShows  int           `json:"browsableShows"`
	Collections     int           `json:"collections"`
	LastSync        time.Time     `json:"lastSync,omitempty"`
	Sync            SyncProgress  `json:"sync"`
}

// Stats returns mirror counts, last sync time and current sync progress.
func (c *Catalog) Stats(ctx context.Context) (Stats, error) {
	var s Stats
	s.Trending = c.TrendStatus(ctx)
	count := func(q string, dst *int, args ...any) error {
		return c.db.QueryRowContext(ctx, q, args...).Scan(dst)
	}
	if err := count(`SELECT COUNT(*) FROM catalog_titles WHERE type='movie'`, &s.Movies); err != nil {
		return s, err
	}
	if err := count(`SELECT COUNT(*) FROM catalog_titles WHERE type='show'`, &s.Shows); err != nil {
		return s, err
	}
	if err := count(`SELECT COUNT(*) FROM catalog_titles WHERE type='season'`, &s.Seasons); err != nil {
		return s, err
	}
	if err := count(`SELECT COUNT(*) FROM catalog_titles WHERE type='episode'`, &s.Episodes); err != nil {
		return s, err
	}
	if err := count(`SELECT COUNT(*) FROM catalog_titles WHERE type='movie' AND browsable=1`, &s.BrowsableMovies); err != nil {
		return s, err
	}
	if err := count(`SELECT COUNT(*) FROM catalog_titles WHERE type='show' AND browsable=1`, &s.BrowsableShows); err != nil {
		return s, err
	}
	if err := count(`SELECT COUNT(*) FROM catalog_collections`, &s.Collections); err != nil {
		return s, err
	}
	if v := c.getMeta(ctx, "last_sync"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			s.LastSync = time.Unix(n, 0).UTC()
		}
	}
	c.progressMu.RLock()
	s.Sync = c.progress
	c.progressMu.RUnlock()
	s.Sync.Running = c.syncing.Load()
	return s, nil
}

func (c *Catalog) setProgress(fn func(*SyncProgress)) {
	c.progressMu.Lock()
	fn(&c.progress)
	c.progressMu.Unlock()
}

// SyncProgress returns the current sync progress snapshot.
func (c *Catalog) SyncProgress() SyncProgress {
	c.progressMu.RLock()
	defer c.progressMu.RUnlock()
	p := c.progress
	p.Running = c.syncing.Load()
	return p
}

// Run performs an initial sync and then refreshes on RefreshInterval until ctx ends.
// Intended to be run in its own goroutine by the app wiring. With
// WithResumeSchedule the initial sync waits until the interval has passed
// since the last recorded sync.
func (c *Catalog) Run(ctx context.Context) {
	if wait := c.untilDue(ctx); wait > 0 {
		c.log.Info("catalog: resuming the sync schedule", "next_sync_in", wait.Round(time.Second))
		if !schedule.Wait(ctx, c.now, wait) {
			return
		}
	}
	full := schedule.New(c.now(), 0)
	warm := schedule.New(c.now(), 90*time.Second)
	recent := schedule.New(c.now(), 4*time.Hour)
	for ctx.Err() == nil {
		if full.Remaining(c.now()) == 0 {
			err := c.Sync(ctx)
			delay := c.refreshInterval()
			switch {
			case err == nil || ctx.Err() != nil:
			case errors.Is(err, tmdb.ErrNoKey):
				// A fresh install before setup: expected, not an error.
				c.log.Info("catalog: waiting for a TMDB API key (add it in setup or Integrations)")
				delay = min(delay, 5*time.Minute)
			default:
				c.log.Error("catalog: refresh sync", "err", err)
				delay = min(delay, 5*time.Minute)
			}
			full = schedule.New(c.now(), delay)
		}
		if ctx.Err() != nil {
			return
		}
		if recent.Remaining(c.now()) == 0 {
			if !c.syncing.Load() {
				c.syncNewReleases(ctx)
			}
			recent = schedule.New(c.now(), 4*time.Hour)
		}
		if ctx.Err() != nil {
			return
		}
		if warm.Remaining(c.now()) == 0 {
			if !c.syncing.Load() {
				_, _ = c.WarmDetails(ctx, 25)
			}
			warm = schedule.New(c.now(), 90*time.Second)
		}
		delay := min(schedule.MaxWait, full.Remaining(c.now()), warm.Remaining(c.now()), recent.Remaining(c.now()))
		if !schedule.Wait(ctx, c.now, delay) {
			return
		}
	}
}

func (c *Catalog) refreshInterval() time.Duration {
	if d := c.config().RefreshInterval.D(); d > 0 {
		return d
	}
	return 24 * time.Hour
}

// untilDue is how long Run waits before its first sync: zero unless
// resuming the recorded schedule and the last sync is recent.
func (c *Catalog) untilDue(ctx context.Context) time.Duration {
	if !c.resumeSchedule {
		return 0
	}
	n, err := strconv.ParseInt(c.getMeta(ctx, "last_sync"), 10, 64)
	if err != nil || n <= 0 {
		return 0 // never synced: sync now
	}
	return min(c.refreshInterval(), max(time.Unix(n, 0).Add(c.refreshInterval()).Sub(c.now().Round(0)), 0))
}

// Sync (re)builds the browsable set from TMDB discover. A second call while
// one is running returns ErrSyncInProgress; use SyncProgress to poll.
var ErrSyncInProgress = fmt.Errorf("catalog: sync already in progress")

func (c *Catalog) Sync(ctx context.Context) error {
	if c.client == nil {
		return fmt.Errorf("catalog: sync: no TMDB client")
	}
	if !c.syncing.CompareAndSwap(false, true) {
		return ErrSyncInProgress
	}
	defer c.syncing.Store(false)
	c.setProgress(func(p *SyncProgress) {
		*p = SyncProgress{Running: true, StartedAt: c.now()}
	})
	err := c.syncLocked(ctx)
	c.setProgress(func(p *SyncProgress) {
		p.Running = false
		p.FinishedAt = c.now()
		p.Phase = ""
		if err != nil {
			p.LastError = err.Error()
		}
	})
	if err == nil {
		_ = c.setMeta(ctx, "last_sync", strconv.FormatInt(c.now().Unix(), 10))
	}
	return err
}

func (c *Catalog) syncLocked(ctx context.Context) error {
	for _, tv := range []bool{false, true} {
		// Pass 1: popularity-sorted; stop once popularity drops below threshold
		// AND the item also fails the vote bar (sorted desc, so all later fail pop).
		if err := c.syncPass(ctx, tv, "popularity"); err != nil {
			return err
		}
		// Pass 2: vote-count with server-side vote_count.gte catches long-tail
		// classics whose current popularity is low.
		if c.config().MinVotes > 0 {
			if err := c.syncPass(ctx, tv, "votes"); err != nil {
				return err
			}
		}
	}
	c.syncNewReleases(ctx)
	// Demote browsable titles that no longer meet the (possibly raised) threshold?
	// Deliberately not done: searched titles joined on demand and owners raising
	// the bar expect new syncs to stop adding, not to hide known titles. The
	// browsable flag is only ever added by sync/search.
	return nil
}

func (c *Catalog) syncPass(ctx context.Context, tv bool, pass string) error {
	cfg := c.config()
	typ := "movie"
	sortBy := "popularity.desc"
	if tv {
		typ = "tv"
	}
	params := tmdb.DiscoverParams{SortBy: sortBy, IncludeAdult: cfg.IncludeAdult}
	if pass == "votes" {
		params.SortBy = "vote_count.desc"
		params.VoteCountGTE = cfg.MinVotes
	}
	// Probe page 1 unsliced; if TMDB's 500-page cap would bite, slice by year.
	totalPages, err := c.syncSlice(ctx, tv, params, pass, 0, true)
	if err != nil {
		return err
	}
	c.setProgress(func(p *SyncProgress) { p.Phase = typ + "/" + pass })
	// The probe only read total_pages (the real fetch hits the TMDB cache).
	if totalPages <= 500 {
		_, err := c.syncSlice(ctx, tv, params, pass, 0, false)
		return err
	}
	from, to := c.syncYears[0], c.syncYears[1]
	if from <= 0 {
		from = 1900
	}
	if to <= 0 {
		to = c.now().Year() + 2
	}
	for year := to; year >= from; year-- {
		if err := ctx.Err(); err != nil {
			return err
		}
		p := params
		p.Year = year
		c.setProgress(func(pr *SyncProgress) { pr.Phase = fmt.Sprintf("%s/%s/%d", typ, pass, year) })
		if _, err := c.syncSlice(ctx, tv, p, pass, year, false); err != nil {
			return err
		}
	}
	return nil
}

// syncSlice pages one discover slice. In probe mode it fetches only page 1 and
// returns total_pages without storing anything.
func (c *Catalog) syncSlice(ctx context.Context, tv bool, params tmdb.DiscoverParams, pass string, year int, probe bool) (int, error) {
	cfg := c.config()
	for page := 1; page <= 500; page++ {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		p := params
		p.Page = page
		var titles []core.Title
		totalPages := 0
		if tv {
			res, err := c.client.DiscoverTV(ctx, p)
			if err != nil {
				return 0, err
			}
			totalPages = res.TotalPages
			if probe {
				return totalPages, nil
			}
			for _, s := range res.Results {
				titles = append(titles, c.tvToTitle(ctx, s))
			}
		} else {
			res, err := c.client.DiscoverMovies(ctx, p)
			if err != nil {
				return 0, err
			}
			totalPages = res.TotalPages
			if probe {
				return totalPages, nil
			}
			for _, m := range res.Results {
				titles = append(titles, c.movieToTitle(ctx, m))
			}
		}
		added := 0
		stop := false
		for _, t := range titles {
			if pass == "popularity" && cfg.MinPopularity > 0 && t.Popularity < cfg.MinPopularity && (cfg.MinVotes <= 0 || t.VoteCount < cfg.MinVotes) {
				// Popularity-sorted desc: nothing later on this slice qualifies on pop,
				// but vote-qualifiers are still possible deeper. Only stop early when
				// there is no vote pass (MinVotes unset) — otherwise finish the slice
				// cheaply is wrong for huge slices, so stop and let the votes pass catch them.
				stop = true
			}
			if c.meetsThreshold(t) {
				if err := c.upsert(ctx, t, upsertOpts{browsable: true}); err != nil {
					return totalPages, err
				}
				added++
			}
		}
		c.setProgress(func(pr *SyncProgress) {
			pr.PagesDone++
			pr.TitlesSeen += len(titles)
			pr.TitlesAdded += added
		})
		if stop || page >= totalPages || len(titles) == 0 {
			return totalPages, nil
		}
	}
	return 500, nil
}

// qualifiesNewRelease admits a recently released title (last 45 days) that
// already has real viewer traction even if its cumulative vote count is still
// building toward the long-term MinVotes bar.
func (c *Catalog) qualifiesNewRelease(t core.Title) bool {
	if c.meetsThreshold(t) {
		return true
	}
	now := c.now()
	if (t.Type != core.TypeMovie && t.Type != core.TypeShow) || t.Adult || !t.IsReleased(now) {
		return false
	}
	if now.Sub(t.Released) > 45*24*time.Hour {
		return false
	}
	cfg := c.config()
	minPop := math.Max(2.5, cfg.MinPopularity*0.6)
	minVotes := max(10, cfg.MinVotes/5)
	return t.Popularity >= minPop && t.VoteCount >= minVotes
}

// syncNewReleases pulls TMDB's live release feeds (now_playing, airing_today,
// on_the_air, trending_day, trending_week) so brand-new theatrical, streaming
// and broadcast releases enter the browsable mirror without waiting for
// long-term vote counts to accumulate. Errors on optional endpoints are
// ignored so minimal test fixtures are unaffected.
func (c *Catalog) syncNewReleases(ctx context.Context) {
	if c.client == nil || ctx.Err() != nil {
		return
	}
	var batch []core.Title
	seen := map[core.RatingKey]bool{}
	collect := func(ts []core.Title) {
		for _, t := range ts {
			if t.Key == 0 || seen[t.Key] || !c.qualifiesNewRelease(t) {
				continue
			}
			seen[t.Key] = true
			batch = append(batch, t)
		}
	}
	for _, list := range []string{"now_playing", "trending_day", "trending_week"} {
		if items, err := c.fetchList(ctx, list, core.TypeMovie, 40); err == nil {
			collect(items)
		}
	}
	for _, list := range []string{"airing_today", "on_the_air", "trending_day", "trending_week"} {
		if items, err := c.fetchList(ctx, list, core.TypeShow, 40); err == nil {
			collect(items)
		}
	}
	if len(batch) > 0 {
		_ = c.upsertMany(ctx, batch, upsertOpts{browsable: true})
	}
}

// WarmDetails enriches up to limit browsable titles whose detailed metadata
// (credits, crew, keywords, studios, runtime, content rating, seasons and
// crowd recommendations) has not been fetched yet, plus returning series
// whose detail TTL has expired. New releases (<120 days old) are warmed first,
// followed by the most popular and most-voted titles. Settled titles are
// marked detail=1 and never re-queried.
func (c *Catalog) WarmDetails(ctx context.Context, limit int) (int, error) {
	if c.client == nil || limit <= 0 {
		return 0, nil
	}
	now := c.now()
	recentCutoff := now.Add(-120 * 24 * time.Hour).Unix()
	staleCutoff := now.Add(-c.staleAfter).Unix()
	rows, err := c.db.QueryContext(ctx, `
		SELECT key FROM catalog_titles
		WHERE browsable=1 AND adult=0 AND type IN ('movie','show')
		  AND released_ts<>0 AND released_ts<=?
		  AND (detail=0 OR (type='show' AND status='Returning Series' AND detail_at>0 AND detail_at<?))
		ORDER BY CASE WHEN detail=0 AND released_ts>=? THEN 0 WHEN detail=0 THEN 1 ELSE 2 END ASC,
		         popularity DESC, vote_count DESC, key ASC
		LIMIT ?`, now.Unix(), staleCutoff, recentCutoff, limit)
	if err != nil {
		return 0, err
	}
	var keys []core.RatingKey
	for rows.Next() {
		var k int64
		if err := rows.Scan(&k); err != nil {
			rows.Close()
			return 0, err
		}
		keys = append(keys, core.RatingKey(k))
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	warmed := 0
	for _, k := range keys {
		if ctx.Err() != nil {
			return warmed, ctx.Err()
		}
		if _, err := c.Get(ctx, k); err == nil {
			warmed++
		}
	}
	return warmed, nil
}

type personCreditsTMDB interface {
	PersonCombinedCredits(ctx context.Context, id int64) (*tmdb.PersonCombinedCredits, error)
}

// ensurePersonCredits hydrates a person's filmography from TMDB's
// /person/{id}/combined_credits on first browse when the local mirror holds
// only a few detail-enriched titles for them.
func (c *Catalog) ensurePersonCredits(ctx context.Context, personID int64) {
	if personID <= 0 || c.client == nil {
		return
	}
	pc, ok := c.client.(personCreditsTMDB)
	if !ok {
		return
	}
	now := c.now()
	c.personMu.Lock()
	if at, seen := c.personHydrated[personID]; seen && now.Sub(at) < 24*time.Hour {
		c.personMu.Unlock()
		return
	}
	if len(c.personHydrated) >= 2048 {
		c.personHydrated = map[int64]time.Time{}
	}
	c.personHydrated[personID] = now
	c.personMu.Unlock()

	var have int
	if err := c.db.QueryRowContext(ctx, `SELECT COUNT(DISTINCT key) FROM catalog_credits WHERE person_tmdb=?`, personID).Scan(&have); err == nil && have >= 8 {
		return
	}
	bctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	credits, err := pc.PersonCombinedCredits(bctx, personID)
	if err != nil || credits == nil {
		return
	}
	var personName, personThumb string
	_ = c.db.QueryRowContext(ctx, `SELECT name, thumb FROM catalog_credits WHERE person_tmdb=? AND name<>'' LIMIT 1`, personID).Scan(&personName, &personThumb)
	if personName == "" {
		if pd, ok := c.client.(personDetailsSource); ok {
			if p, err := pd.PersonDetails(bctx, personID); err == nil && p != nil {
				personName, personThumb = p.Name, p.ProfilePath
			}
		}
	}
	type creditLink struct {
		key  core.RatingKey
		role string
		kind string
		ord  int
	}
	var summaries []core.Title
	seenTitle := map[core.RatingKey]bool{}
	var links []creditLink
	addWork := func(w tmdb.PersonCredit, kind, role string, ord int) {
		var t core.Title
		switch w.MediaType {
		case "movie":
			t = c.movieToTitle(ctx, w.AsMovie())
		case "tv":
			t = c.tvToTitle(ctx, w.AsTV())
		default:
			return
		}
		if t.Key == 0 || t.Adult || !t.IsReleased(now) || !c.meetsThreshold(t) {
			return
		}
		if !seenTitle[t.Key] {
			seenTitle[t.Key] = true
			summaries = append(summaries, t)
		}
		links = append(links, creditLink{key: t.Key, role: role, kind: kind, ord: ord})
	}
	for i, w := range credits.Cast {
		addWork(w, "cast", w.Character, i)
	}
	for i, w := range credits.Crew {
		job := strings.ToLower(strings.TrimSpace(w.Job))
		switch job {
		case "director":
			addWork(w, "director", w.Job, i)
		case "writer", "screenplay", "author", "novel", "story", "teleplay":
			addWork(w, "writer", w.Job, i)
		case "director of photography", "cinematography":
			addWork(w, "crew", "Cinematographer", i)
		case "original music composer", "music", "composer":
			addWork(w, "crew", "Composer", i)
		case "creator", "series composition", "showrunner":
			addWork(w, "crew", "Creator", i)
		}
	}
	if len(summaries) > 0 {
		_ = c.upsertMany(ctx, summaries, upsertOpts{browsable: true})
	}
	if len(links) > 0 {
		tx, err := c.db.BeginTx(ctx, nil)
		if err != nil {
			c.log.Debug("catalog: begin person credits", "person", personID, "err", err)
		} else {
			defer tx.Rollback() // after Commit this returns ErrTxDone, ignorable
			var insertErr error
			for _, l := range links {
				if _, err := tx.ExecContext(ctx, `INSERT INTO catalog_credits(key,person_tmdb,name,role,kind,ord,thumb) VALUES(?,?,?,?,?,?,?) ON CONFLICT DO NOTHING`,
					int64(l.key), personID, personName, l.role, l.kind, l.ord, personThumb); err != nil {
					insertErr = err
					break
				}
			}
			if insertErr != nil {
				c.log.Debug("catalog: insert person credits", "person", personID, "err", insertErr)
			} else if err := tx.Commit(); err != nil {
				c.log.Debug("catalog: commit person credits", "person", personID, "err", err)
			} else {
				c.writeGen.Add(1)
			}
		}
	}
}
