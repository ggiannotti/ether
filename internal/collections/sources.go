package collections

import (
	"context"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/braedonsaunders/ether/internal/core"
)

// sourceResult is what one source contributed: titles in the source's own
// order (full titles when they came from TMDB, so they can be joined to the
// catalogue), plus problems worth showing the owner.
type sourceResult struct {
	titles   []core.Title
	keys     []core.RatingKey // keys known to be in the catalogue already
	all      bool             // the whole catalogue
	warnings []string
	// assured lists, per title, the rules TMDB guaranteed when the rules
	// were pushed into this source's discover query.
	assured map[core.RatingKey][]*rule
	// resolved fills in a source's id/name once looked up, so saved specs
	// show what a label resolved to.
	resolvedID    int64
	resolvedLabel string
}

// kindsFor maps a media type to TMDB endpoint kinds.
func kindsFor(mediaType string) []string {
	switch mediaType {
	case MediaMovie:
		return []string{"movie"}
	case MediaShow:
		return []string{"tv"}
	}
	return []string{"movie", "tv"}
}

func coreType(kind string) core.MediaType {
	if kind == "tv" {
		return core.TypeShow
	}
	return core.TypeMovie
}

// want is how many candidates to pull from a ranked external source.
func wantFor(sp *core.CollectionSpec) int {
	if sp.Limit > 0 {
		w := sp.Limit * 3
		if w < 60 {
			w = 60
		}
		if w > 400 {
			w = 400
		}
		return w
	}
	return 200
}

func pagesFor(want int) int {
	p := (want + 19) / 20
	if p < 1 {
		p = 1
	}
	if p > 20 {
		p = 20
	}
	return p
}

func (s *Service) region(src string) string {
	if src != "" {
		return strings.ToUpper(src)
	}
	if s.catCfg != nil {
		if r := strings.ToUpper(strings.TrimSpace(s.catCfg().Region)); r != "" {
			return r
		}
	}
	return "US"
}

// discoverInto runs a discover-backed source for one kind. With match
// "all", the spec's rules are added to the query where TMDB can express
// them, so the source returns the titles that match (not just the most
// popular ones) and the rules are guaranteed for what comes back.
func (s *Service) discoverInto(ctx context.Context, sp *core.CollectionSpec, kind string, q url.Values, rules []*rule, want int, res *sourceResult, what string) {
	var covered []*rule
	if sp.Match == MatchAll {
		for _, r := range rules {
			extra := url.Values{}
			if s.translate(ctx, r, kind, extra) && mergeParams(q, extra) {
				covered = append(covered, r)
			}
		}
	}
	if q.Get("sort_by") == "" {
		q.Set("sort_by", discoverSort(sp.Sort, kind))
	}
	items, _, err := s.api.discover(ctx, kind, q, want, pagesFor(want))
	if err != nil {
		res.warnings = append(res.warnings, fmt.Sprintf("TMDB %s: %v", what, err))
		return
	}
	res.titles = append(res.titles, items...)
	if len(covered) == 0 {
		return
	}
	if res.assured == nil {
		res.assured = map[core.RatingKey][]*rule{}
	}
	for _, t := range items {
		res.assured[t.Key] = append(res.assured[t.Key], covered...)
	}
}

// mergeParams ANDs extra discover parameters into q. It reports false (and
// changes nothing) when the combination cannot be expressed: an OR list
// meeting another value of the same parameter, or a conflicting scalar.
func mergeParams(q, extra url.Values) bool {
	merged := map[string]string{}
	for k := range extra {
		v := extra.Get(k)
		cur := q.Get(k)
		switch {
		case cur == "" || cur == v:
			merged[k] = v
		case strings.HasSuffix(k, ".gte") || strings.HasSuffix(k, ".lte"):
			merged[k] = stricter(k, cur, v)
		case (strings.HasPrefix(k, "with_") || strings.HasPrefix(k, "without_")) && listParam(k):
			if strings.Contains(cur, "|") || strings.Contains(v, "|") {
				if !strings.HasPrefix(k, "without_") {
					return false
				}
			}
			merged[k] = cur + "," + v
		default:
			return false
		}
	}
	for k, v := range merged {
		q.Set(k, v)
	}
	return true
}

func listParam(k string) bool {
	switch k {
	case "with_genres", "without_genres", "with_keywords", "without_keywords", "with_companies", "without_companies",
		"with_people", "with_cast", "with_crew", "with_networks":
		return true
	}
	return false
}

// stricter keeps the tighter of two bounds (numbers or YYYY-MM-DD dates).
func stricter(k, a, b string) string {
	fa, ea := strconv.ParseFloat(a, 64)
	fb, eb := strconv.ParseFloat(b, 64)
	gte := strings.HasSuffix(k, ".gte")
	if ea == nil && eb == nil {
		if (gte && fb > fa) || (!gte && fb < fa) {
			return b
		}
		return a
	}
	if (gte && b > a) || (!gte && b < a) {
		return b
	}
	return a
}

// gather runs one source.
func (s *Service) gather(ctx context.Context, sp *core.CollectionSpec, src core.CollectionSource, rules []*rule) sourceResult {
	want := wantFor(sp)
	kinds := kindsFor(sp.MediaType)
	if src.MediaType == MediaMovie || src.MediaType == MediaShow {
		kinds = kindsFor(src.MediaType)
	}
	var res sourceResult
	warn := func(format string, args ...any) { res.warnings = append(res.warnings, fmt.Sprintf(format, args...)) }
	needTMDB := func() bool {
		if s.api == nil || s.api.c == nil {
			warn("%s needs TMDB, which is not configured", sourceName(src))
			return false
		}
		return true
	}
	switch src.Kind {
	case "catalog":
		res.all = true
	case "titles":
		for _, k := range src.Keys {
			if rk, err := core.ParseRatingKey(k); err == nil {
				res.keys = append(res.keys, rk)
			}
		}
	case "tmdbList":
		n := want
		if n > 100 {
			n = 100
		}
		for _, kind := range kinds {
			items, err := s.cat.List(ctx, src.List, coreType(kind), n)
			if err != nil {
				warn("TMDB list %s: %v", src.List, err)
				continue
			}
			res.titles = append(res.titles, items...)
		}
	case "tmdbCollection":
		id := src.ID
		if id <= 0 {
			if !needTMDB() {
				break
			}
			r, err := s.names.resolve(ctx, "collection", ref{Name: src.Label}, "")
			if err != nil {
				warn("couldn't find a TMDB collection called %q", src.Label)
				break
			}
			id, res.resolvedID, res.resolvedLabel = r.ID, r.ID, r.Name
		}
		name, items, err := s.cat.Collection(ctx, id)
		if err != nil {
			warn("TMDB collection %d: %v", id, err)
			break
		}
		if res.resolvedLabel == "" && src.Label == "" {
			res.resolvedID, res.resolvedLabel = id, name
		}
		res.titles = items
	case "tmdbKeyword", "tmdbCompany", "tmdbNetwork":
		if !needTMDB() {
			break
		}
		entity := map[string]string{"tmdbKeyword": "keyword", "tmdbCompany": "company", "tmdbNetwork": "network"}[src.Kind]
		r, err := s.names.resolve(ctx, entity, ref{ID: src.ID, Name: src.Label}, "")
		if err != nil || r.ID <= 0 {
			warn("couldn't find the %s %q on TMDB", entity, firstNonEmpty(src.Label, strconv.FormatInt(src.ID, 10)))
			break
		}
		res.resolvedID, res.resolvedLabel = r.ID, r.Name
		param := map[string]string{"keyword": "with_keywords", "company": "with_companies", "network": "with_networks"}[entity]
		if entity == "network" {
			kinds = []string{"tv"}
		}
		for _, kind := range kinds {
			s.discoverInto(ctx, sp, kind, url.Values{param: {strconv.FormatInt(r.ID, 10)}}, rules, want, &res, entity+" "+strconv.Quote(r.Name))
		}
	case "tmdbPerson":
		if !needTMDB() {
			break
		}
		r, err := s.names.resolve(ctx, "person", ref{ID: src.ID, Name: src.Label}, "")
		if err != nil || r.ID <= 0 {
			warn("couldn't find the person %q on TMDB", firstNonEmpty(src.Label, strconv.FormatInt(src.ID, 10)))
			break
		}
		items, name, err := s.api.personCredits(ctx, r.ID, src.Role)
		if err != nil {
			warn("TMDB credits for %s: %v", firstNonEmpty(r.Name, src.Label), err)
			break
		}
		res.resolvedID, res.resolvedLabel = r.ID, firstNonEmpty(name, r.Name)
		res.titles = items
	case "watchProvider":
		if !needTMDB() {
			break
		}
		region := s.region(src.Region)
		r, err := s.names.resolve(ctx, "watchProvider", ref{ID: src.ID, Name: src.Label}, region)
		if err != nil || r.ID <= 0 {
			warn("couldn't find the streaming service %q in %s", firstNonEmpty(src.Label, strconv.FormatInt(src.ID, 10)), region)
			break
		}
		res.resolvedID, res.resolvedLabel = r.ID, r.Name
		for _, kind := range kinds {
			s.discoverInto(ctx, sp, kind, url.Values{
				"with_watch_providers": {strconv.FormatInt(r.ID, 10)}, "watch_region": {region},
				"with_watch_monetization_types": {"flatrate|free|ads"},
			}, rules, want, &res, "watch provider "+strconv.Quote(r.Name))
		}
	case "tmdbDiscover":
		if !needTMDB() {
			break
		}
		for _, kind := range kinds {
			q, err := s.discoverParams(ctx, kind, src.Params)
			if err != nil {
				warn("discover: %v", err)
				break
			}
			s.discoverInto(ctx, sp, kind, q, rules, want, &res, "discover")
		}
	case "traktList", "letterboxdList", "mdblist":
		var refs []extRef
		var err error
		switch src.Kind {
		case "traktList":
			refs, err = s.fetchTrakt(ctx, src.URL)
		case "letterboxdList":
			refs, err = s.fetchLetterboxd(ctx, src.URL)
		default:
			refs, err = s.fetchMDBList(ctx, src.URL)
		}
		if err != nil {
			warn("%v", err)
			break
		}
		if len(refs) == 0 {
			warn("%s is empty", sourceName(src))
			break
		}
		titles, unresolved, pending := s.resolveExternal(ctx, src.Kind, refs)
		res.titles = titles
		if unresolved > 0 {
			warn("%d of %d items in %s could not be matched to TMDB", unresolved, len(refs), sourceName(src))
		}
		if pending > 0 {
			warn("%d items in %s are still being matched; they will appear after the next refresh", pending, sourceName(src))
		}
	}
	return res
}

func sourceName(src core.CollectionSource) string {
	switch src.Kind {
	case "traktList":
		return "the Trakt list"
	case "letterboxdList":
		return "the Letterboxd list"
	case "mdblist":
		return "the MDBList list"
	case "tmdbDiscover":
		return "TMDB discover"
	case "watchProvider":
		return "the streaming service source"
	}
	return src.Kind
}

var idParams = map[string]string{
	"with_keywords": "keyword", "without_keywords": "keyword",
	"with_companies": "company", "without_companies": "company",
	"with_people": "person", "with_cast": "person", "with_crew": "person",
	"with_networks": "network", "with_watch_providers": "watchProvider", "without_watch_providers": "watchProvider",
}

var listSplit = regexp.MustCompile(`([,|])`)

// discoverParams copies raw discover parameters, resolving names where
// TMDB expects ids: with_keywords "christmas|holiday" becomes the keyword
// ids joined the same way, and with_genres accepts genre names.
func (s *Service) discoverParams(ctx context.Context, kind string, params map[string]string) (url.Values, error) {
	q := url.Values{}
	region := s.region(params["watch_region"])
	for k, v := range params {
		k = strings.TrimSpace(k)
		v = strings.TrimSpace(v)
		if k == "" || v == "" || k == "page" || k == "api_key" || k == "language" {
			continue
		}
		entity, isID := idParams[k]
		if !isID && k != "with_genres" && k != "without_genres" {
			q.Set(k, v)
			continue
		}
		parts := listSplit.Split(v, -1)
		seps := listSplit.FindAllString(v, -1)
		var b strings.Builder
		for i, p := range parts {
			p = strings.TrimSpace(p)
			if p == "" {
				continue
			}
			var id int64
			if n, err := strconv.ParseInt(p, 10, 64); err == nil {
				id = n
			} else if !isID {
				g, ok := s.api.genreID(ctx, kind, p)
				if !ok {
					return nil, fmt.Errorf("unknown genre %q", p)
				}
				id = int64(g)
			} else {
				r, err := s.names.resolve(ctx, entity, ref{Name: p}, region)
				if err != nil || r.ID <= 0 {
					return nil, fmt.Errorf("couldn't find the %s %q on TMDB", entity, p)
				}
				id = r.ID
			}
			if b.Len() > 0 && i > 0 {
				b.WriteString(seps[i-1])
			}
			b.WriteString(strconv.FormatInt(id, 10))
		}
		if b.Len() > 0 {
			q.Set(k, b.String())
		}
	}
	if q.Get("with_watch_providers") != "" && q.Get("watch_region") == "" {
		q.Set("watch_region", region)
	}
	return q, nil
}

// resolveExternal maps list items to catalogue titles: by TMDB id, then
// IMDb id, then (Letterboxd) a cached slug, an exact TMDB title+year
// search, or the film page. It returns titles in list order, how many
// could not be matched, and how many were deferred to a later refresh to
// stay polite to Letterboxd.
func (s *Service) resolveExternal(ctx context.Context, source string, refs []extRef) ([]core.Title, int, int) {
	type slot struct {
		key core.RatingKey
		t   *core.Title
	}
	slots := make([]slot, len(refs))
	var mu sync.Mutex
	unresolved, pending := 0, 0
	filmPageBudget := 25
	sem := make(chan struct{}, 4)
	var wg sync.WaitGroup
	for i := range refs {
		r := refs[i]
		i := i
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			key, deferred := s.resolveRef(ctx, source, r, func() bool {
				mu.Lock()
				defer mu.Unlock()
				if filmPageBudget <= 0 {
					return false
				}
				filmPageBudget--
				return true
			})
			mu.Lock()
			defer mu.Unlock()
			if key == 0 {
				if deferred {
					pending++
				} else {
					unresolved++
				}
				return
			}
			slots[i].key = key
		}()
	}
	wg.Wait()
	var keys []core.RatingKey
	for _, sl := range slots {
		if sl.key != 0 {
			keys = append(keys, sl.key)
		}
	}
	titles := s.titlesFor(ctx, keys)
	if missing := len(keys) - len(titles); missing > 0 {
		unresolved += missing
	}
	return titles, unresolved, pending
}

// resolveRef maps one external item to a rating key. allowPage reserves a
// Letterboxd film-page fetch; deferred reports that one was needed but the
// budget was spent.
func (s *Service) resolveRef(ctx context.Context, source string, r extRef, allowPage func() bool) (core.RatingKey, bool) {
	if r.TMDB > 0 {
		switch r.Type {
		case "show":
			return core.ShowKey(r.TMDB), false
		case "movie", "":
			return core.MovieKey(r.TMDB), false
		}
	}
	cacheSrc, cacheID := "", ""
	switch {
	case r.IMDB != "":
		cacheSrc, cacheID = "imdb", r.IMDB
	case r.Slug != "":
		cacheSrc, cacheID = "letterboxd", r.Slug
	case r.Title != "":
		cacheSrc, cacheID = "title:"+r.Type, strings.ToLower(r.Title)+"|"+strconv.Itoa(r.Year)
	default:
		return 0, false
	}
	var k int64
	if err := s.db.QueryRowContext(ctx, `SELECT key FROM collections_external WHERE source=? AND ext=?`, cacheSrc, cacheID).Scan(&k); err == nil {
		return core.RatingKey(k), false
	}
	remember := func(key core.RatingKey) core.RatingKey {
		if key != 0 {
			if _, err := s.db.ExecContext(ctx, `INSERT INTO collections_external(source,ext,key,at) VALUES(?,?,?,?)
				ON CONFLICT(source,ext) DO UPDATE SET key=excluded.key, at=excluded.at`, cacheSrc, cacheID, int64(key), s.now().Unix()); err != nil {
				s.log.Debug("collections: remember external key", "err", err)
			}
		}
		return key
	}
	if r.IMDB != "" {
		if t, err := s.cat.ByExternalID(ctx, "imdb", r.IMDB); err == nil && t != nil && (t.Type == core.TypeMovie || t.Type == core.TypeShow) {
			return remember(t.Key), false
		}
	}
	if r.Title != "" && s.api != nil && s.api.c != nil {
		if key := s.searchExact(ctx, r); key != 0 {
			return remember(key), false
		}
	}
	if r.Slug != "" && source == "letterboxdList" {
		if !allowPage() {
			return 0, true
		}
		id, typ, err := s.letterboxdFilmTMDB(ctx, r.Slug)
		if err == nil && id > 0 {
			if typ == "tv" {
				return remember(core.ShowKey(id)), false
			}
			return remember(core.MovieKey(id)), false
		}
	}
	return 0, false
}

// searchExact finds a title on TMDB by exact (normalised) title and year.
func (s *Service) searchExact(ctx context.Context, r extRef) core.RatingKey {
	kinds := []string{"movie"}
	switch r.Type {
	case "show":
		kinds = []string{"tv"}
	case "":
		kinds = []string{"movie", "tv"}
	}
	want := normName(r.Title)
	for _, kind := range kinds {
		q := url.Values{"query": {r.Title}, "include_adult": {"false"}}
		if r.Year > 0 {
			if kind == "movie" {
				q.Set("year", strconv.Itoa(r.Year))
			} else {
				q.Set("first_air_date_year", strconv.Itoa(r.Year))
			}
		}
		var res tmdbPage
		if err := s.api.get(ctx, "/search/"+kind, q, &res); err != nil {
			continue
		}
		for _, it := range res.Results {
			name := firstNonEmpty(it.Title, it.Name)
			orig := firstNonEmpty(it.OriginalTitle, it.OriginalName)
			if normName(name) != want && normName(orig) != want {
				continue
			}
			date := firstNonEmpty(it.ReleaseDate, it.FirstAirDate)
			if r.Year > 0 && len(date) >= 4 {
				y, _ := strconv.Atoi(date[:4])
				if y < r.Year-1 || y > r.Year+1 {
					continue
				}
			}
			if kind == "tv" {
				return core.ShowKey(it.ID)
			}
			return core.MovieKey(it.ID)
		}
	}
	return 0
}

// titlesFor returns catalogue titles for keys, from the index, the mirror,
// or (for titles the mirror has never seen) an on-demand TMDB fetch.
func (s *Service) titlesFor(ctx context.Context, keys []core.RatingKey) []core.Title {
	if len(keys) == 0 {
		return nil
	}
	loaded, err := loadTitles(ctx, s.db, keys)
	if err != nil {
		s.log.Debug("collections: load titles", "err", err)
	}
	have := make(map[core.RatingKey]core.Title, len(loaded))
	for _, t := range loaded {
		have[t.Key] = t
	}
	var missing []core.RatingKey
	for _, k := range keys {
		if _, ok := have[k]; !ok {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		var mu sync.Mutex
		sem := make(chan struct{}, 4)
		var wg sync.WaitGroup
		for _, k := range missing {
			k := k
			wg.Add(1)
			sem <- struct{}{}
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				t, err := s.cat.Get(ctx, k)
				if err != nil || t == nil {
					return
				}
				mu.Lock()
				have[k] = *t
				mu.Unlock()
			}()
		}
		wg.Wait()
	}
	out := make([]core.Title, 0, len(keys))
	seen := map[core.RatingKey]bool{}
	for _, k := range keys {
		if t, ok := have[k]; ok && !seen[k] && (t.Type == core.TypeMovie || t.Type == core.TypeShow) {
			seen[k] = true
			out = append(out, t)
		}
	}
	return out
}
