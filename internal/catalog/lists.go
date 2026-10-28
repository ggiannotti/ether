package catalog

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/braedonsaunders/ether/internal/core"
	"github.com/braedonsaunders/ether/internal/store"
	"github.com/braedonsaunders/ether/internal/tmdb"
)

func (c *Catalog) upsertSummaries(ctx context.Context, titles []core.Title, browsable bool) error {
	if browsable {
		return c.upsertMany(ctx, titles, upsertOpts{browsable: true})
	}
	// One transaction for the list, while retaining each title's threshold.
	// A transaction per summary adds two network round trips per tile in HA.
	if len(titles) == 0 {
		return nil
	}
	// Concurrent nodes may refresh overlapping lists in different rank order.
	// Take row locks in key order, while the caller retains its display order.
	titles = append([]core.Title(nil), titles...)
	sort.SliceStable(titles, func(i, j int) bool { return titles[i].Key < titles[j].Key })
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := c.now().Unix()
	for _, t := range titles {
		if err := upsertOne(ctx, tx, t, upsertOpts{browsable: c.meetsThreshold(t)}, now); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	c.writeGen.Add(1)
	return nil
}

func limitTitles(items []core.Title, limit int) []core.Title {
	if limit > 0 && len(items) > limit {
		return items[:limit]
	}
	if items == nil {
		return []core.Title{}
	}
	return items
}

// Search reaches all of TMDB; matched titles join the browsable set.
func (c *Catalog) Search(ctx context.Context, query string, types []core.MediaType, limit int) ([]core.Title, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return []core.Title{}, nil
	}
	if limit <= 0 {
		limit = 20
	}
	wantMovie, wantTV := true, true
	if len(types) > 0 {
		wantMovie, wantTV = false, false
		for _, t := range types {
			if t == core.TypeMovie {
				wantMovie = true
			}
			if t == core.TypeShow {
				wantTV = true
			}
		}
	}
	seen := map[core.RatingKey]bool{}
	var out []core.Title
	add := func(ts []core.Title) {
		for _, t := range ts {
			if t.Adult || !t.IsReleased(c.now()) || t.Key == 0 || seen[t.Key] {
				continue
			}
			seen[t.Key] = true
			out = append(out, t)
		}
	}
	// Local first (instant, works offline).
	like := store.Like(store.DialectOf(c.db))
	local, err := c.queryTitles(ctx, `SELECT `+titleCols+` FROM catalog_titles WHERE type IN ('movie','show')
		AND (title `+like+` ? ESCAPE '\' OR original_title `+like+` ? ESCAPE '\') ORDER BY popularity DESC LIMIT ?`,
		"%"+escapeLike(query)+"%", "%"+escapeLike(query)+"%", limit*2)
	if err == nil {
		var filtered []core.Title
		for _, t := range local {
			if (t.Type == core.TypeMovie && wantMovie) || (t.Type == core.TypeShow && wantTV) {
				filtered = append(filtered, t)
			}
		}
		add(filtered)
	}
	if c.client == nil {
		return limitTitles(c.FilterVisibility(ctx, out), limit), nil
	}
	var remote []core.Title
	// Page through TMDB until we have enough or run out (max 3 pages).
	if wantMovie && wantTV {
		for page := 1; page <= 3 && len(remote) < limit*2; page++ {
			res, err := c.client.SearchMulti(ctx, query, page)
			if err != nil {
				if page == 1 && len(out) == 0 {
					return nil, err
				}
				break
			}
			for _, r := range res.Results {
				switch r.MediaType {
				case "movie":
					remote = append(remote, c.movieToTitle(ctx, r.AsMovie()))
				case "tv":
					remote = append(remote, c.tvToTitle(ctx, r.AsTV()))
				}
			}
			if page >= res.TotalPages {
				break
			}
		}
	} else if wantMovie {
		res, err := c.client.SearchMovies(ctx, query, 1)
		if err != nil && len(out) == 0 {
			return nil, err
		}
		if err == nil {
			for _, m := range res.Results {
				remote = append(remote, c.movieToTitle(ctx, m))
			}
		}
	} else if wantTV {
		res, err := c.client.SearchTV(ctx, query, 1)
		if err != nil && len(out) == 0 {
			return nil, err
		}
		if err == nil {
			for _, s := range res.Results {
				remote = append(remote, c.tvToTitle(ctx, s))
			}
		}
	}
	if len(remote) > 0 {
		if err := c.upsertMany(ctx, remote, upsertOpts{browsable: true}); err != nil {
			return nil, err
		}
		// Reload so callers get hydrated/genres-preserved rows.
		for _, t := range remote {
			if st, err := c.loadByKey(ctx, t.Key); err == nil {
				add([]core.Title{st.t})
			} else {
				add([]core.Title{t})
			}
		}
	}
	// Remote (TMDB relevance) order is preserved after local exact-prefix matches:
	// keep simple stable order: exact title first, then popularity.
	sortTitlesForSearch(out, query)
	return limitTitles(c.FilterVisibility(ctx, out), limit), nil
}

func sortTitlesForSearch(items []core.Title, q string) {
	lq := strings.ToLower(q)
	score := func(t core.Title) int {
		lt := strings.ToLower(t.Title)
		switch {
		case lt == lq:
			return 0
		case strings.HasPrefix(lt, lq):
			return 1
		case strings.Contains(lt, lq):
			return 2
		default:
			return 3
		}
	}
	sort.SliceStable(items, func(i, j int) bool {
		si, sj := score(items[i]), score(items[j])
		if si != sj {
			return si < sj
		}
		return items[i].Popularity > items[j].Popularity
	})
}

// List returns TMDB curated/trending lists with a short in-memory TTL.
func (c *Catalog) List(ctx context.Context, list string, t core.MediaType, limit int) ([]core.Title, error) {
	if limit <= 0 {
		limit = 20
	}
	if list == "trending_day" || list == "trending_week" {
		return c.trendingList(ctx, list, t, limit)
	}
	cacheKey := list + "|" + string(t) + "|" + strconv.Itoa(limit)
	c.listMu.Lock()
	if e, ok := c.listCache[cacheKey]; ok && c.now().Before(e.expires) {
		items := append([]core.Title(nil), e.items...)
		c.listMu.Unlock()
		return c.releasedTitles(c.FilterVisibility(ctx, items)), nil
	}
	c.listMu.Unlock()
	if c.client == nil {
		// Offline fallback: best-effort browse by popularity/rating.
		sortOrder := core.SortPopularity
		if list == "top_rated" {
			sortOrder = core.SortRating
		}
		page, err := c.Browse(ctx, core.BrowseQuery{Filter: core.Filter{Type: t}, Sort: sortOrder, Desc: true, Limit: limit})
		if err != nil {
			return nil, err
		}
		return page.Items, nil
	}
	items, err := c.fetchList(ctx, list, t, limit)
	if err != nil {
		return nil, err
	}
	if err := c.upsertSummaries(ctx, items, false); err != nil {
		return nil, err
	}
	// Hydrate the list together: the facet queries are already batch-aware.
	keys := make([]core.RatingKey, 0, len(items))
	for _, it := range items {
		keys = append(keys, it.Key)
	}
	hydrated, loadErr := c.titlesByKeys(ctx, keys)
	byKey := make(map[core.RatingKey]core.Title, len(hydrated))
	for _, t := range hydrated {
		byKey[t.Key] = t
	}
	out := make([]core.Title, 0, len(items))
	for _, it := range items {
		if t, ok := byKey[it.Key]; ok {
			out = append(out, t)
		} else if loadErr != nil {
			if st, err := c.loadByKey(ctx, it.Key); err == nil {
				out = append(out, st.t)
			} else {
				out = append(out, it)
			}
		} else {
			out = append(out, it)
		}
	}
	out = limitTitles(c.releasedTitles(out), limit)
	c.listMu.Lock()
	c.listCache[cacheKey] = listEntry{items: append([]core.Title(nil), out...), expires: c.now().Add(c.listTTL)}
	c.listMu.Unlock()
	return out, nil
}

func (c *Catalog) fetchList(ctx context.Context, list string, t core.MediaType, limit int) ([]core.Title, error) {
	pages := (limit + 19) / 20
	if pages < 1 {
		pages = 1
	}
	maxPages := 5
	if list == "trending_day" || list == "trending_week" {
		maxPages = trendPoolSize / 20
	}
	if pages > maxPages {
		pages = maxPages
	}
	collectMovies := func(fn func(page int) (*tmdb.Paged[tmdb.Movie], error)) ([]core.Title, error) {
		var out []core.Title
		for p := 1; p <= pages; p++ {
			res, err := fn(p)
			if err != nil {
				return out, err
			}
			for _, m := range res.Results {
				out = append(out, c.movieToTitle(ctx, m))
			}
			if p >= res.TotalPages || len(out) >= limit {
				break
			}
		}
		return out, nil
	}
	collectTV := func(fn func(page int) (*tmdb.Paged[tmdb.TVShow], error)) ([]core.Title, error) {
		var out []core.Title
		for p := 1; p <= pages; p++ {
			res, err := fn(p)
			if err != nil {
				return out, err
			}
			for _, s := range res.Results {
				out = append(out, c.tvToTitle(ctx, s))
			}
			if p >= res.TotalPages || len(out) >= limit {
				break
			}
		}
		return out, nil
	}
	switch list {
	case "trending_day", "trending_week":
		window := "week"
		if list == "trending_day" {
			window = "day"
		}
		switch t {
		case core.TypeMovie:
			return collectMovies(func(p int) (*tmdb.Paged[tmdb.Movie], error) { return c.client.TrendingMovies(ctx, window, p) })
		case core.TypeShow:
			return collectTV(func(p int) (*tmdb.Paged[tmdb.TVShow], error) { return c.client.TrendingTV(ctx, window, p) })
		default:
			var out []core.Title
			for p := 1; p <= pages; p++ {
				res, err := c.client.TrendingAll(ctx, window, p)
				if err != nil {
					return out, err
				}
				for _, r := range res.Results {
					if r.MediaType == "movie" {
						out = append(out, c.movieToTitle(ctx, r.AsMovie()))
					} else if r.MediaType == "tv" {
						out = append(out, c.tvToTitle(ctx, r.AsTV()))
					}
				}
				if p >= res.TotalPages || len(out) >= limit {
					break
				}
			}
			return out, nil
		}
	case "popular", "top_rated", "now_playing", "upcoming", "airing_today", "on_the_air":
		if t == core.TypeShow {
			l := list
			if l == "now_playing" {
				l = "airing_today"
			}
			if l == "upcoming" {
				l = "on_the_air"
			}
			return collectTV(func(p int) (*tmdb.Paged[tmdb.TVShow], error) { return c.client.TVList(ctx, l, p) })
		}
		l := list
		if l == "airing_today" || l == "on_the_air" {
			l = "now_playing"
		}
		return collectMovies(func(p int) (*tmdb.Paged[tmdb.Movie], error) { return c.client.MovieList(ctx, l, p) })
	default:
		return nil, fmt.Errorf("catalog: unknown list %q", list)
	}
}

// Similar returns TMDB recommendations ∪ similar (recommendations first).
func (c *Catalog) Similar(ctx context.Context, key core.RatingKey, limit int) ([]core.Title, error) {
	if limit <= 0 {
		limit = 20
	}
	// Resolve seasons/episodes to their show.
	kind := key.Kind()
	id := key.TMDB()
	var out []core.Title
	seen := map[core.RatingKey]bool{key: true}
	if kind == core.KindSeason || kind == core.KindEpisode {
		seen[core.ShowKey(id)] = true
	}
	add := func(ts []core.Title) {
		for _, t := range ts {
			if !seen[t.Key] {
				seen[t.Key] = true
				out = append(out, t)
			}
		}
	}
	if c.client == nil {
		// Offline: same genres via browse is the recommender's job; return nothing.
		return []core.Title{}, nil
	}
	switch kind {
	case core.KindMovie:
		if res, err := c.client.MovieRecommendations(ctx, id, 1); err == nil {
			for _, m := range res.Results {
				add([]core.Title{c.movieToTitle(ctx, m)})
			}
		} else if res2, err2 := c.client.MovieSimilar(ctx, id, 1); err2 != nil {
			return nil, err
		} else {
			for _, m := range res2.Results {
				add([]core.Title{c.movieToTitle(ctx, m)})
			}
			break
		}
		if res, err := c.client.MovieSimilar(ctx, id, 1); err == nil {
			for _, m := range res.Results {
				add([]core.Title{c.movieToTitle(ctx, m)})
			}
		}
	case core.KindShow, core.KindSeason, core.KindEpisode:
		if res, err := c.client.TVRecommendations(ctx, id, 1); err == nil {
			for _, s := range res.Results {
				add([]core.Title{c.tvToTitle(ctx, s)})
			}
		} else if res2, err2 := c.client.TVSimilar(ctx, id, 1); err2 != nil {
			return nil, err
		} else {
			for _, s := range res2.Results {
				add([]core.Title{c.tvToTitle(ctx, s)})
			}
			break
		}
		if res, err := c.client.TVSimilar(ctx, id, 1); err == nil {
			for _, s := range res.Results {
				add([]core.Title{c.tvToTitle(ctx, s)})
			}
		}
	default:
		return nil, fmt.Errorf("%w: key %s", core.ErrNotFound, key)
	}
	out = limitTitles(c.releasedTitles(out), limit)
	if err := c.upsertSummaries(ctx, out, false); err != nil {
		return nil, err
	}
	reloaded := make([]core.Title, 0, len(out))
	for _, it := range out {
		if st, err := c.loadByKey(ctx, it.Key); err == nil {
			reloaded = append(reloaded, st.t)
		} else {
			reloaded = append(reloaded, it)
		}
	}
	return c.releasedTitles(reloaded), nil
}

// Collection returns a TMDB franchise and its parts sorted by release date.
func (c *Catalog) Collection(ctx context.Context, tmdbCollection int64) (string, []core.Title, error) {
	if tmdbCollection <= 0 {
		return "", nil, core.ErrNotFound
	}
	if c.client != nil {
		d, err := c.client.CollectionDetails(ctx, tmdbCollection)
		if err == nil {
			if _, err := c.db.ExecContext(ctx, `INSERT INTO catalog_collections(tmdb_id,name,summary,poster,backdrop,fetched_at) VALUES(?,?,?,?,?,?)
				ON CONFLICT(tmdb_id) DO UPDATE SET name=excluded.name,summary=excluded.summary,poster=excluded.poster,backdrop=excluded.backdrop,fetched_at=excluded.fetched_at`,
				d.ID, d.Name, d.Overview, d.PosterPath, d.BackdropPath, c.now().Unix()); err != nil {
				c.log.Debug("catalog: cache collection", "tmdb", d.ID, "err", err)
			}
			var items []core.Title
			for _, m := range d.Parts {
				t := c.movieToTitle(ctx, m)
				t.CollectionID = d.ID
				t.CollectionName = d.Name
				items = append(items, t)
			}
			sort.Slice(items, func(i, j int) bool { return items[i].Released.Before(items[j].Released) })
			if err := c.upsertSummaries(ctx, items, false); err != nil {
				return d.Name, nil, err
			}
			out := make([]core.Title, 0, len(items))
			for _, it := range items {
				if st, err := c.loadByKey(ctx, it.Key); err == nil {
					out = append(out, st.t)
				} else {
					out = append(out, it)
				}
			}
			return d.Name, c.releasedTitles(out), nil
		} else if isNotFound(err) {
			return "", nil, err
		}
		// fall through to local on transient errors
	}
	var name string
	_ = c.db.QueryRowContext(ctx, `SELECT name FROM catalog_collections WHERE tmdb_id=?`, tmdbCollection).Scan(&name)
	items, err := c.queryTitles(ctx, `SELECT `+titleCols+` FROM catalog_titles WHERE collection_id=? AND type='movie' ORDER BY released_ts ASC`, tmdbCollection)
	if err != nil {
		return "", nil, err
	}
	if len(items) == 0 {
		return "", nil, core.ErrNotFound
	}
	if name == "" && len(items) > 0 {
		name = items[0].CollectionName
	}
	return name, items, nil
}

func isNotFound(err error) bool {
	return err != nil && (strings.Contains(err.Error(), "not found") || err == core.ErrNotFound)
}

// ByExternalID looks up by imdb ("tt123") or tvdb id.
func (c *Catalog) ByExternalID(ctx context.Context, source, id string) (*core.Title, error) {
	source = strings.ToLower(strings.TrimSpace(source))
	id = strings.TrimSpace(id)
	if id == "" {
		return nil, core.ErrNotFound
	}
	// Local first.
	var items []core.Title
	var err error
	switch source {
	case "imdb", "imdb_id":
		items, err = c.queryTitles(ctx, `SELECT `+titleCols+` FROM catalog_titles WHERE imdb=? AND type IN ('movie','show','episode') ORDER BY CASE type WHEN 'episode' THEN 1 ELSE 0 END LIMIT 1`, id)
	case "tvdb", "tvdb_id":
		n, perr := strconv.ParseInt(id, 10, 64)
		if perr != nil {
			return nil, fmt.Errorf("catalog: tvdb id %q: %w", id, perr)
		}
		items, err = c.queryTitles(ctx, `SELECT `+titleCols+` FROM catalog_titles WHERE tvdb=? AND type IN ('movie','show') LIMIT 1`, n)
	default:
		return nil, fmt.Errorf("catalog: unsupported external source %q", source)
	}
	if err != nil {
		return nil, err
	}
	if len(items) > 0 {
		// Return the detail-enriched version.
		return c.Get(ctx, items[0].Key)
	}
	if c.client == nil {
		return nil, core.ErrNotFound
	}
	found, err := c.client.FindByExternalID(ctx, source, id)
	if err != nil {
		return nil, err
	}
	// Prefer episode results for imdb episode lookups? Titles (movie/show) are the common case.
	if len(found.MovieResults) > 0 {
		t := c.movieToTitle(ctx, found.MovieResults[0])
		if err := c.upsertSummaries(ctx, []core.Title{t}, false); err != nil {
			return nil, err
		}
		return c.Get(ctx, t.Key)
	}
	if len(found.TVResults) > 0 {
		t := c.tvToTitle(ctx, found.TVResults[0])
		if err := c.upsertSummaries(ctx, []core.Title{t}, false); err != nil {
			return nil, err
		}
		return c.Get(ctx, t.Key)
	}
	if len(found.TVEpisodeResults) > 0 {
		e := found.TVEpisodeResults[0]
		if e.ShowID != 0 {
			return c.Get(ctx, core.EpisodeKey(e.ShowID, e.SeasonNumber, e.EpisodeNumber))
		}
	}
	return nil, core.ErrNotFound
}

// Apply the same release rule to remote lists and cached responses as SQL reads.
func (c *Catalog) releasedTitles(items []core.Title) []core.Title {
	out := make([]core.Title, 0, len(items))
	for _, t := range items {
		if !t.Adult && t.IsReleased(c.now()) && c.Visible(&t) {
			out = append(out, t)
		}
	}
	return out
}
