package plex

import (
	"context"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/braedonsaunders/ether/internal/core"
	"github.com/braedonsaunders/ether/internal/plex/model"
)

// Plex metadata type numbers.
const (
	typeNumMovie   = 1
	typeNumShow    = 2
	typeNumSeason  = 3
	typeNumEpisode = 4
)

func typeNumber(t core.MediaType) int {
	switch t {
	case core.TypeMovie:
		return typeNumMovie
	case core.TypeShow:
		return typeNumShow
	case core.TypeSeason:
		return typeNumSeason
	case core.TypeEpisode:
		return typeNumEpisode
	}
	return 0
}

func typeFromParam(s string) core.MediaType {
	switch strings.ToLower(s) {
	case "1", "movie", "movies":
		return core.TypeMovie
	case "2", "show", "shows", "tv":
		return core.TypeShow
	case "3", "season":
		return core.TypeSeason
	case "4", "episode", "episodes":
		return core.TypeEpisode
	case "18", "collection":
		return "collection"
	}
	return ""
}

const viewModeList = 65592

// defaultUnpagedLimit bounds list responses for clients that don't page.
const defaultUnpagedLimit = 500

// sectionPrefs is /library/sections/{id}/prefs: the library settings the
// apps read (visibility, collection mode, episode sorting and ordering,
// season display), with the values Ether actually behaves by. PMS types
// default/value as strings in this endpoint's JSON, unlike /:/prefs.
// Writes are not supported (Ether's libraries are generated).
func sectionPrefs(sec Section) []model.Setting {
	set := func(id, label, summary, typ, def, val, enum string) model.Setting {
		return model.Setting{ID: id, Label: label, Summary: summary, Type: typ, Default: def, Value: val, EnumValues: enum}
	}
	visibility := set("hidden", "Visibility", "Restrict where content from this library should appear.", "int", "0", "0",
		"0:Include in home screen and global search|1:Exclude from home screen|2:Exclude from home screen and global search")
	collections := set("collectionMode", "Collections", "How to display collections on the Library tab.", "int", "2", "2",
		"0:Hide collections but show their items|1:Hide items which are in collections|2:Show collections and their items")
	original := set("originalTitles", "Use original titles", "Use the original titles for all items regardless of the library language.", "bool", "false", "false", "")
	if sec.Type == core.TypeShow {
		return []model.Setting{
			visibility,
			set("episodeSort", "Episode sorting", "How to sort the episodes for shows.", "text", "-1", "-1", "0:Oldest first|1:Newest first"),
			set("showOrdering", "Episode ordering", "Determines the ordering of episodes.", "text", "tmdbAiring", "tmdbAiring", "tmdbAiring:The Movie Database"),
			set("useSeasonTitles", "Use season titles", "Use season titles when available.", "bool", "false", "false", ""),
			original,
			set("enableBIFGeneration", "Enable video preview thumbnails", "Generate video preview thumbnails for items in this library when enabled in server settings.", "bool", "true", "false", ""),
			collections,
			set("flattenSeasons", "Seasons", "Choose whether to show or hide seasons.", "int", "0", "0", "0:Show|2:Hide for single-season series|1:Hide"),
			set("enableIntroMarkerGeneration", "Enable intro detection", "Detect intros and allow intro skipping for items in this library when enabled in server settings.", "bool", "true", "false", ""),
			set("enableCreditsMarkerGeneration", "Enable credits detection", "Detect credits and allow credits skipping for items in this library when enabled in server settings.", "bool", "true", "false", ""),
		}
	}
	return []model.Setting{
		visibility,
		set("enableCinemaTrailers", "Enable Cinema Trailers", "Allow Cinema Trailers to play before items in this library.", "bool", "true", "false", ""),
		original,
		set("enableBIFGeneration", "Enable video preview thumbnails", "Generate video preview thumbnails for items in this library when enabled in server settings.", "bool", "true", "false", ""),
		collections,
		set("enableCreditsMarkerGeneration", "Enable credits detection", "Detect credits and allow credits skipping for items in this library when enabled in server settings.", "bool", "true", "false", ""),
	}
}

func sectionTypeTitle(sec Section) (typeTitle, all string) {
	if sec.Type == core.TypeShow {
		return "TV Shows", "All Shows"
	}
	return "Movies", "All Movies"
}

func (s *Server) sectionContainer(sec Section) *model.MediaContainer {
	mc := libraryContainer()
	mc.LibrarySectionID = model.Ptr(sec.ID)
	mc.LibrarySectionTitle = sec.Title
	mc.LibrarySectionUUID = sec.UUID
	mc.Title1 = sec.Title
	mc.ViewMode = viewModeList
	return mc
}

// sectionParam resolves {section} and checks the user's access.
func (s *Server) sectionParam(w http.ResponseWriter, r *http.Request) (Section, bool) {
	id, err := strconv.Atoi(chi.URLParam(r, "section"))
	if err != nil {
		writeHTTPError(w, http.StatusNotFound)
		return Section{}, false
	}
	sec, ok := s.sectionByID(id)
	if !ok || !s.canSeeSection(FromContext(r.Context()).User, id) {
		writeHTTPError(w, http.StatusNotFound)
		return Section{}, false
	}
	return sec, true
}

func (s *Server) handleLibraryRoot(w http.ResponseWriter, r *http.Request) {
	mc := &model.MediaContainer{AllowSync: model.Ptr(s.downloadsAllowed(FromContext(r.Context()).User)), Identifier: model.LibraryIdentifier, MediaTagPrefix: model.MediaTagPrefix, MediaTagVersion: model.MediaTagVersion, Title1: "Plex Library"}
	for _, d := range []struct{ key, title string }{{"sections", "Library Sections"}, {"recentlyAdded", "Recently Added Content"}, {"onDeck", "On Deck Content"}} {
		mc.Directory = append(mc.Directory, model.Directory{Key: d.key, Title: d.title})
	}
	mc.Size = len(mc.Directory)
	s.write(w, r, mc)
}

func (s *Server) handleSections(w http.ResponseWriter, r *http.Request) {
	u := FromContext(r.Context()).User
	mc := &model.MediaContainer{AllowSync: model.Ptr(s.downloadsAllowed(u)), Title1: "Plex Library"}
	now := s.now().Unix()
	for _, sec := range s.userSections(u) {
		agent, scanner := agentFor(sec.Type)
		mc.Directory = append(mc.Directory, model.Directory{
			AllowSync:        model.Ptr(s.downloadsAllowed(u)),
			Filters:          model.Ptr(true),
			Refreshing:       model.Ptr(false),
			Key:              strconv.Itoa(sec.ID),
			Type:             string(sec.Type),
			Title:            sec.Title,
			Agent:            agent,
			Scanner:          scanner,
			Language:         sec.Language,
			UUID:             sec.UUID,
			UpdatedAt:        now,
			CreatedAt:        now,
			ScannedAt:        now,
			Content:          model.Ptr(true),
			IsDirectory:      model.Ptr(true),
			ContentChangedAt: now,
			Hidden:           model.Ptr(0),
			Location:         []model.Location{{ID: sec.ID, Path: "/ether/" + strings.ToLower(strings.ReplaceAll(sec.Title, " ", "-"))}},
		})
	}
	mc.Size = len(mc.Directory)
	s.write(w, r, mc)
}

// handleSection is /library/sections/{id}: the section's listing directory
// (and its Meta when includeDetails/includeMeta is set).
func (s *Server) handleSection(w http.ResponseWriter, r *http.Request) {
	sec, ok := s.sectionParam(w, r)
	if !ok {
		return
	}
	typeTitle, allTitle := sectionTypeTitle(sec)
	tn := strconv.Itoa(typeNumber(sec.Type))
	mc := libraryContainer()
	mc.LibrarySectionID = model.Ptr(sec.ID)
	mc.Title1 = sec.Title
	mc.ViewMode = viewModeList
	mc.Content = "secondary"
	mc.ViewGroup = "secondary"
	sub := []struct {
		key, title string
		secondary  bool
	}{
		{"all", allTitle, false},
		{"unwatched", "Unwatched", false},
		{"newest", "Recently Released", false},
		{"recentlyAdded", "Recently Added", false},
		{"recentlyViewed", "Recently Viewed", false},
		{"onDeck", "Continue Watching", false},
		{"collection", "By Collection", true},
		{"genre", "By Genre", true},
		{"year", "By Year", true},
		{"decade", "By Decade", true},
		{"country", "By Country", true},
		{"contentRating", "By Content Rating", true},
		{"firstCharacter", "By First Letter", true},
	}
	for _, d := range sub {
		dir := model.Directory{Key: d.key, Title: d.title}
		if d.secondary {
			dir.Secondary = model.Ptr(true)
		}
		mc.Directory = append(mc.Directory, dir)
	}
	mc.Directory = append(mc.Directory, model.Directory{Key: "search?type=" + tn, Prompt: "Search " + typeTitle, Search: model.Ptr(true), Title: "Search..."})
	mc.Size = len(mc.Directory)
	q := r.URL.Query()
	if q.Get("includeDetails") == "1" || q.Get("includeMeta") == "1" {
		mc.Meta = s.meta(sec, sec.Type, "")
	}
	s.write(w, r, mc)
}

// ---------------------------------------------------------------------------
// Meta: types, filters, sorts, fields

type sortDef struct {
	key, title, defDir string
}

var sortDefs = []sortDef{
	{"titleSort", "Title", "asc"},
	{"originallyAvailableAt", "Release Date", "desc"},
	{"rating", "Critic Rating", "desc"},
	{"audienceRating", "Audience Rating", "desc"},
	{"addedAt", "Date Added", "desc"},
	{"popularity", "Popularity", "desc"},
}

type filterDef struct {
	filter, filterType, title string
}

var filterDefs = []filterDef{
	{"genre", "string", "Genre"},
	{"year", "integer", "Year"},
	{"decade", "integer", "Decade"},
	{"contentRating", "string", "Content Rating"},
	{"collection", "string", "Collection"},
	{"country", "string", "Country"},
	{"studio", "string", "Studio"},
	{"unwatched", "boolean", "Unwatched"},
}

func (s *Server) metaType(sec Section, t core.MediaType, activeSort string) model.MetaType {
	id := strconv.Itoa(sec.ID)
	tn := strconv.Itoa(typeNumber(t))
	typeTitle, _ := sectionTypeTitle(sec)
	mt := model.MetaType{Key: "/library/sections/" + id + "/all?type=" + tn, Type: string(t), Title: typeTitle, Active: true}
	for _, f := range filterDefs {
		key := "/library/sections/" + id + "/" + f.filter
		mt.Filter = append(mt.Filter, model.Filter{Filter: f.filter, FilterType: f.filterType, Key: key, Title: f.title, Type: "filter"})
	}
	activeKey, activeDir := parseSortParam(activeSort)
	for i, sd := range sortDefs {
		so := model.Sort{DefaultDirection: sd.defDir, DescKey: sd.key + ":desc", Key: sd.key, Title: sd.title}
		if sd.key == "titleSort" {
			so.FirstCharacterKey = "/library/sections/" + id + "/firstCharacter"
		}
		if i == 0 {
			so.Default = sd.defDir
		}
		if (activeKey == "" && i == 0) || activeKey == sd.key {
			so.Active = model.Ptr(true)
			dir := activeDir
			if dir == "" {
				dir = sd.defDir
			}
			so.ActiveDirection = dir
		}
		mt.Sort = append(mt.Sort, so)
	}
	mt.Field = []model.Field{
		{Key: "title", Title: "Title", Type: "string"},
		{Key: "studio", Title: "Studio", Type: "string"},
		{Key: "contentRating", Title: "Content Rating", Type: "tag"},
		{Key: "year", Title: "Year", Type: "integer", SubType: "year"},
		{Key: "decade", Title: "Decade", Type: "integer", SubType: "decade"},
		{Key: "originallyAvailableAt", Title: "Release Date", Type: "date"},
		{Key: "duration", Title: "Duration", Type: "integer", SubType: "duration"},
		{Key: "genre", Title: "Genre", Type: "tag"},
		{Key: "collection", Title: "Collection", Type: "tag"},
		{Key: "director", Title: "Director", Type: "tag"},
		{Key: "writer", Title: "Writer", Type: "tag"},
		{Key: "actor", Title: "Actor", Type: "tag"},
		{Key: "country", Title: "Country", Type: "tag"},
		{Key: "audienceRating", Title: "Audience Rating", Type: "integer", SubType: "rating"},
		{Key: "unwatched", Title: "Unwatched", Type: "boolean"},
		{Key: "inProgress", Title: "In Progress", Type: "boolean"},
	}
	return mt
}

func (s *Server) meta(sec Section, t core.MediaType, activeSort string) *model.Meta {
	return &model.Meta{
		Type: []model.MetaType{s.metaType(sec, t, activeSort)},
		FieldType: []model.FieldType{
			{Type: "tag", Operator: []model.Operator{{Key: "=", Title: "is"}, {Key: "!=", Title: "is not"}}},
			{Type: "integer", Operator: []model.Operator{{Key: "=", Title: "is"}, {Key: "!=", Title: "is not"}, {Key: ">>=", Title: "is greater than"}, {Key: "<<=", Title: "is less than"}}},
			{Type: "string", Operator: []model.Operator{{Key: "=", Title: "contains"}, {Key: "!=", Title: "does not contain"}, {Key: "==", Title: "is"}, {Key: "!==", Title: "is not"}, {Key: "<=", Title: "begins with"}, {Key: ">=", Title: "ends with"}}},
			{Type: "boolean", Operator: []model.Operator{{Key: "=", Title: "is true"}, {Key: "!=", Title: "is false"}}},
			{Type: "date", Operator: []model.Operator{{Key: "<<=", Title: "is before"}, {Key: ">>=", Title: "is after"}}},
		},
	}
}

func parseSortParam(s string) (key, dir string) {
	first, _, _ := strings.Cut(s, ",")
	key, opts, _ := strings.Cut(first, ":")
	for _, o := range strings.Split(opts, ":") {
		if o == "desc" || o == "asc" {
			dir = o
		}
	}
	return key, dir
}

// ---------------------------------------------------------------------------
// Browse

type browseRequest struct {
	q         core.BrowseQuery
	unwatched bool
	limit     int // "limit" param; 0 = none
}

// parseBrowse converts Plex filter/sort params into a catalogue query.
func (s *Server) parseBrowse(ctx context.Context, sec Section, t core.MediaType, u *core.User, q url.Values) browseRequest {
	br := browseRequest{}
	f := core.Filter{Type: t}
	if u != nil {
		f.MaxContentRating = u.MaxContentRating
	}
	splitVals := func(v string) []string {
		var out []string
		for _, p := range strings.Split(v, ",") {
			if p = strings.TrimSpace(p); p != "" {
				out = append(out, p)
			}
		}
		return out
	}
	atoi := func(v string) int { n, _ := strconv.Atoi(strings.TrimSpace(v)); return n }
	for rawKey, vals := range q {
		if len(vals) == 0 {
			continue
		}
		v := vals[0]
		// Media-query operators arrive inside the key: "year>>=2000" parses
		// as key "year>>", value "2000".
		key, op := rawKey, "="
		for _, suf := range []string{"!==", "==", "!=", ">>", "<<", ">", "<", "!"} {
			if strings.HasSuffix(rawKey, suf) {
				key, op = strings.TrimSuffix(rawKey, suf), suf
				break
			}
		}
		switch key {
		case "genre":
			for _, g := range splitVals(v) {
				f.Genres = append(f.Genres, s.facetTitle(ctx, t, "genre", g))
			}
		case "country":
			for _, c := range splitVals(v) {
				f.Countries = append(f.Countries, s.facetTitle(ctx, t, "country", c))
			}
		case "contentRating":
			for _, c := range splitVals(v) {
				f.ContentRatings = append(f.ContentRatings, s.facetTitle(ctx, t, "contentRating", c))
			}
		case "studio":
			f.Studio = s.facetTitle(ctx, t, "studio", v)
		case "language":
			f.Languages = splitVals(v)
		case "year":
			ys := splitVals(v)
			switch op {
			case ">>":
				f.YearFrom = atoi(v) + 1
			case ">":
				f.YearFrom = atoi(v)
			case "<<":
				f.YearTo = atoi(v) - 1
			case "<":
				f.YearTo = atoi(v)
			default:
				for _, y := range ys {
					n := atoi(y)
					if n == 0 {
						continue
					}
					if f.YearFrom == 0 || n < f.YearFrom {
						f.YearFrom = n
					}
					if n > f.YearTo {
						f.YearTo = n
					}
				}
			}
		case "decade":
			n := atoi(v)
			if n > 0 {
				f.YearFrom, f.YearTo = n, n+9
			}
		case "collection":
			if id, err := strconv.ParseInt(v, 10, 64); err == nil {
				if rk := core.RatingKey(id); rk.Kind() == core.KindCollection {
					id = rk.TMDB()
				}
				f.Collection = id
			}
		case "actor", "director", "writer", "producer":
			if id, err := strconv.ParseInt(v, 10, 64); err == nil {
				f.Person = id
			}
		case "title":
			f.Query = v
		case "audienceRating", "rating":
			if x, err := strconv.ParseFloat(v, 64); err == nil && (op == ">>" || op == ">") {
				f.MinRating = x
			}
		case "duration":
			if n := atoi(v); n > 0 {
				switch op {
				case "<<", "<":
					f.MaxRuntime = time.Duration(n) * time.Millisecond
				case ">>", ">":
					f.MinRuntime = time.Duration(n) * time.Millisecond
				}
			}
		case "unwatched":
			br.unwatched = v == "1" || v == "true"
		case "viewCount":
			br.unwatched = v == "0" && op == "="
		case "limit":
			br.limit = atoi(v)
		}
	}
	key, dir := parseSortParam(q.Get("sort"))
	br.q.Sort, br.q.Desc = sortFor(key, dir)
	br.q.Filter = f
	return br
}

// sortFor maps a Plex sort key to a catalogue sort.
func sortFor(key, dir string) (core.SortOrder, bool) {
	var so core.SortOrder
	def := true // desc
	switch key {
	case "titleSort", "title", "":
		so, def = core.SortTitle, false
	case "originallyAvailableAt", "year", "releaseDate":
		so = core.SortReleased
	case "rating", "audienceRating", "userRating":
		so = core.SortRating
	case "addedAt", "updatedAt", "lastViewedAt":
		so = core.SortAdded
	default:
		so = core.SortPopularity
	}
	switch dir {
	case "desc":
		return so, true
	case "asc":
		return so, false
	}
	return so, def
}

// maxPageSize bounds an explicit X-Plex-Container-Size handed to backends:
// far above any real client page, far below where offset+size overflows.
const maxPageSize = 10_000

// pageOf returns the page window the client asked for.
func (pr *Request) pageOf(limit int) (offset, size int) {
	offset, size = 0, defaultUnpagedLimit
	if pr.HasPage {
		offset = pr.Start
		if pr.Size >= 0 {
			size = min(pr.Size, maxPageSize)
		}
	}
	if limit > 0 {
		if offset >= limit {
			return offset, 0
		}
		size = min(size, limit-offset)
	}
	return offset, size
}

// browse runs a catalogue query honouring paging and the unwatched filter.
func (s *Server) browse(ctx context.Context, pr *Request, br browseRequest) ([]core.Title, int, int, error) {
	offset, size := pr.pageOf(br.limit)
	if !br.unwatched {
		q := br.q
		q.Offset, q.Limit = offset, size
		if size == 0 {
			q.Limit = 1 // learn the total without rendering items
		}
		page, err := s.d.Catalog.Browse(ctx, q)
		if err != nil {
			return nil, 0, offset, err
		}
		items := page.Items
		if size == 0 {
			items = nil
		}
		total := page.Total
		if br.limit > 0 && total > br.limit {
			total = br.limit
		}
		return items, total, offset, nil
	}
	// Unwatched: scan the catalogue in chunks, filtering by watch state.
	const chunk, maxScan = 200, 5000
	var out []core.Title
	matched, scanned, total := 0, 0, 0
	for scanned < maxScan {
		q := br.q
		q.Offset, q.Limit = scanned, chunk
		page, err := s.d.Catalog.Browse(ctx, q)
		if err != nil {
			return nil, 0, offset, err
		}
		total = page.Total
		if len(page.Items) == 0 {
			break
		}
		st := s.stateFor(ctx, pr, page.Items)
		for _, t := range page.Items {
			if st[t.Key].ViewCount > 0 {
				total--
				continue
			}
			if matched >= offset && len(out) < size {
				out = append(out, t)
			}
			matched++
		}
		scanned += len(page.Items)
		if scanned >= page.Total || (len(out) >= size && matched > offset+size) {
			break
		}
	}
	if total < matched {
		total = matched
	}
	return out, total, offset, nil
}

// handleSectionSub dispatches /library/sections/{id}/{sub}.
func (s *Server) handleSectionSub(w http.ResponseWriter, r *http.Request) {
	sec, ok := s.sectionParam(w, r)
	if !ok {
		return
	}
	sub := chi.URLParam(r, "sub")
	switch sub {
	case "all", "unwatched", "newest", "recentlyAdded", "search", "autocomplete":
		s.sectionList(w, r, sec, sub)
	case "onDeck":
		s.sectionOnDeck(w, r, sec)
	case "recentlyViewed":
		s.sectionRecentlyViewed(w, r, sec)
	case "collections", "collection":
		s.sectionCollections(w, r, sec, sub)
	case "genre", "year", "decade", "contentRating", "country", "studio", "firstCharacter", "categories":
		s.sectionFacet(w, r, sec, sub)
	case "sorts":
		mt := s.metaType(sec, sec.Type, r.URL.Query().Get("sort"))
		mc := &model.MediaContainer{Size: len(mt.Sort), Sort: mt.Sort}
		s.write(w, r, mc)
	case "filters":
		mt := s.metaType(sec, sec.Type, "")
		var dirs []model.Directory
		for _, f := range mt.Filter {
			dirs = append(dirs, model.Directory{Key: f.Key, Title: f.Title, Type: "filter", Extra: []model.Attr{model.A("filter", f.Filter), model.A("filterType", f.FilterType)}})
		}
		s.write(w, r, &model.MediaContainer{Size: len(dirs), Directory: dirs})
	case "prefs":
		settings := sectionPrefs(sec)
		s.write(w, r, &model.MediaContainer{Size: len(settings), Setting: settings})
	case "refresh", "analyze", "emptyTrash":
		s.ownerOnly(s.handleOK)(w, r)
	default:
		s.handleUnknown(w, r)
	}
}

func (s *Server) sectionList(w http.ResponseWriter, r *http.Request, sec Section, sub string) {
	ctx := r.Context()
	pr := FromContext(ctx)
	q := r.URL.Query()
	t := typeFromParam(q.Get("type"))
	if t == "" {
		t = sec.Type
	}
	_, allTitle := sectionTypeTitle(sec)
	mc := s.sectionContainer(sec)
	mc.Title2 = allTitle
	mc.ViewGroup = string(t)
	if t == "collection" {
		s.sectionCollections(w, r, sec, "collections")
		return
	}
	// Seasons/episodes are not browsable as flat section lists.
	if t != sec.Type {
		mc.SetPage(0, 0)
		s.write(w, r, mc)
		return
	}
	br := s.parseBrowse(ctx, sec, t, pr.User, q)
	switch sub {
	case "unwatched":
		br.unwatched = true
		mc.Title2 = "Unwatched"
	case "newest":
		br.q.Sort, br.q.Desc = core.SortReleased, true
		mc.Title2 = "Recently Released"
	case "recentlyAdded":
		br.q.Sort, br.q.Desc = core.SortAdded, true
		mc.Title2 = "Recently Added"
	case "search", "autocomplete":
		query := q.Get("query")
		if query == "" {
			query = q.Get("title")
		}
		if query == "" {
			query = q.Get("title.query")
		}
		s.sectionSearch(w, r, sec, t, query, mc)
		return
	}
	items, total, offset, err := s.browse(ctx, pr, br)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	mc.Metadata = s.renderTitles(ctx, pr, items, renderOpts{media: true, inSection: true})
	mc.Size = len(mc.Metadata)
	mc.SetPage(offset, total)
	if q.Get("includeMeta") == "1" {
		mc.Meta = s.meta(sec, t, q.Get("sort"))
	}
	s.write(w, r, mc)
}

func (s *Server) sectionSearch(w http.ResponseWriter, r *http.Request, sec Section, t core.MediaType, query string, mc *model.MediaContainer) {
	ctx := r.Context()
	pr := FromContext(ctx)
	var items []core.Title
	if strings.TrimSpace(query) != "" {
		limit := 50
		if n, _ := strconv.Atoi(r.URL.Query().Get("limit")); n > 0 {
			limit = min(n, 200)
		}
		res, err := s.d.Catalog.Search(ctx, query, []core.MediaType{t}, limit)
		if err != nil {
			s.writeErr(w, r, err)
			return
		}
		items = s.filterAllowed(pr.User, res)
	}
	mc.Title2 = "Search for '" + query + "'"
	mc.Metadata = s.renderTitles(ctx, pr, items, renderOpts{media: true, inSection: true})
	mc.Size = len(mc.Metadata)
	s.write(w, r, mc)
}

// filterAllowed drops titles in sections the user cannot see.
func (s *Server) filterAllowed(u *core.User, ts []core.Title) []core.Title {
	return s.visibleTitles(u, ts)
}

func (s *Server) sectionFacet(w http.ResponseWriter, r *http.Request, sec Section, sub string) {
	ctx := r.Context()
	t := typeFromParam(r.URL.Query().Get("type"))
	if t == "" || t == "collection" {
		t = sec.Type
	}
	facet := sub
	if sub == "categories" {
		facet = "genre"
	}
	mc := libraryContainer()
	mc.Title1 = sec.Title
	mc.ViewMode = viewModeList
	mc.Content = "secondary"
	mc.ViewGroup = "secondary"
	titles := map[string]string{"genre": "By Genre", "year": "By Year", "decade": "By Decade", "contentRating": "By Content Rating", "country": "By Country", "studio": "By Studio", "firstCharacter": "By First Letter", "categories": ""}
	mc.Title2 = titles[sub]
	vals, err := s.facet(ctx, t, facet)
	if err != nil {
		// Catalogues may not support every facet (e.g. firstCharacter);
		// an empty list is what PMS returns for an empty library.
		s.log.Debug("facet unavailable", "facet", facet, "err", err)
		vals = nil
	}
	id := strconv.Itoa(sec.ID)
	for _, v := range vals {
		var d model.Directory
		switch sub {
		case "firstCharacter":
			d = model.Directory{Size: model.Ptr(v.Count), Key: url.PathEscape(v.Key), Title: v.Title}
		case "categories":
			d = model.Directory{Type: "directory", Key: "/library/sections/" + id + "/all?genre=" + url.QueryEscape(v.Key), Title: v.Title}
		default:
			d = model.Directory{FastKey: "/library/sections/" + id + "/all?" + facet + "=" + url.QueryEscape(v.Key), Key: v.Key, Title: v.Title, Type: facet}
		}
		mc.Directory = append(mc.Directory, d)
	}
	if sub == "categories" {
		mc.Meta = &model.Meta{Type: []model.MetaType{{Key: "/library/sections/" + id + "/categories", Type: "directory", Title: "Categories", Active: true}}}
	}
	if sub == "year" || sub == "decade" {
		sort.SliceStable(mc.Directory, func(i, j int) bool { return mc.Directory[i].Title > mc.Directory[j].Title })
	}
	mc.Size = len(mc.Directory)
	s.write(w, r, mc)
}

// ---------------------------------------------------------------------------
// On deck, recently added, recently viewed

// inProgressTitles loads the user's in-progress items and next episodes.
func (s *Server) inProgressTitles(ctx context.Context, pr *Request, t core.MediaType, limit int) ([]core.Title, map[core.RatingKey]core.WatchStatus) {
	if s.d.State == nil || pr.User == nil {
		return nil, nil
	}
	sts, err := s.d.State.InProgress(ctx, pr.User.ID, limit*2+10)
	if err != nil {
		s.log.Warn("load in-progress", "err", err)
		return nil, nil
	}
	keys := make([]core.RatingKey, 0, len(sts))
	state := make(map[core.RatingKey]core.WatchStatus, len(sts))
	for _, st := range sts {
		keys = append(keys, st.Key)
		state[st.Key] = st
	}
	titles := s.getTitles(ctx, keys)
	// Resume unfinished media and advance recently completed series. A
	// completed episode has no offset, so InProgress alone loses the show.
	if t != core.TypeMovie {
		titles = s.appendNextContinue(ctx, pr, titles, state, limit)
	}
	sort.SliceStable(titles, func(i, j int) bool {
		a, b := state[titles[i].Key].LastViewedAt, state[titles[j].Key].LastViewedAt
		if !a.Equal(b) {
			return a.After(b)
		}
		return titles[i].Key < titles[j].Key
	})
	seenShows := map[int64]bool{}
	out := titles[:0:0]
	for _, ti := range titles {
		if t != "" {
			want := t
			if t == core.TypeShow {
				want = core.TypeEpisode
			}
			if ti.Type != want {
				continue
			}
		}
		sec, ok := s.sectionFor(ti.Type)
		if ok && !s.canSeeSection(pr.User, sec.ID) {
			continue
		}
		if ti.Type == core.TypeEpisode {
			if seenShows[ti.Key.TMDB()] {
				continue
			}
			seenShows[ti.Key.TMDB()] = true
		}
		out = append(out, ti)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, state
}

// getTitles fetches titles concurrently, preserving order and skipping
// failures.
func (s *Server) getTitles(ctx context.Context, keys []core.RatingKey) []core.Title {
	res := make([]*core.Title, len(keys))
	sem := make(chan struct{}, 8)
	var wg sync.WaitGroup
	for i, k := range keys {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if t, err := s.d.Catalog.Get(ctx, k); err == nil && t != nil {
				res[i] = t
			}
		}()
	}
	wg.Wait()
	out := make([]core.Title, 0, len(keys))
	for _, t := range res {
		if t != nil {
			out = append(out, *t)
		}
	}
	return out
}

func (s *Server) sectionOnDeck(w http.ResponseWriter, r *http.Request, sec Section) {
	ctx := r.Context()
	pr := FromContext(ctx)
	_, size := pr.pageOf(0)
	items, state := s.inProgressTitles(ctx, pr, sec.Type, min(size, 50))
	mc := s.sectionContainer(sec)
	mc.Title2 = "On Deck"
	mc.Mixed = model.Ptr(true)
	mc.Metadata = s.onDeckItems(ctx, pr, items, state)
	mc.Size = len(mc.Metadata)
	s.write(w, r, mc)
}

func (s *Server) handleOnDeck(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	pr := FromContext(ctx)
	_, size := pr.pageOf(0)
	items, state := s.inProgressTitles(ctx, pr, "", min(size, 50))
	mc := libraryContainer()
	mc.Mixed = model.Ptr(true)
	mc.Metadata = s.onDeckItems(ctx, pr, items, state)
	mc.Size = len(mc.Metadata)
	s.write(w, r, mc)
}

// onDeckItems renders On Deck the way PMS does: movies as list items,
// episodes in full (tag ids, Guid, Rating, streams), every item led by its
// sync flag and section UUID.
func (s *Server) onDeckItems(ctx context.Context, pr *Request, items []core.Title, state map[core.RatingKey]core.WatchStatus) []model.Metadata {
	// Filter first: renderTitles drops hidden titles, and the loop below
	// pairs its output with items by index.
	items = s.visibleTitles(pr.User, items)
	md := s.renderTitles(ctx, pr, items, renderOpts{media: true, detailEpisodes: true, state: state})
	for i := range md {
		md[i].AllowSync = model.Ptr(s.downloadsAllowed(pr.User) && isPlayable(items[i].Type))
		if sec, ok := s.sectionFor(items[i].Type); ok {
			md[i].LibrarySectionUUID = sec.UUID
		}
	}
	return md
}

func (s *Server) sectionRecentlyViewed(w http.ResponseWriter, r *http.Request, sec Section) {
	ctx := r.Context()
	pr := FromContext(ctx)
	mc := s.sectionContainer(sec)
	mc.Title2 = "Recently Viewed"
	if s.d.State != nil && pr.User != nil {
		hist, err := s.d.State.History(ctx, pr.User.ID, time.Time{}, 100)
		if err == nil {
			seen := map[core.RatingKey]bool{}
			var keys []core.RatingKey
			for _, h := range hist {
				if !seen[h.Key] {
					seen[h.Key] = true
					keys = append(keys, h.Key)
				}
			}
			var items []core.Title
			for _, t := range s.getTitles(ctx, keys) {
				if sct, ok := s.sectionFor(t.Type); ok && sct.ID == sec.ID {
					items = append(items, t)
				}
			}
			mc.Metadata = s.renderTitles(ctx, pr, items, renderOpts{media: true})
		}
	}
	mc.Size = len(mc.Metadata)
	s.write(w, r, mc)
}

func (s *Server) handleRecentlyAdded(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	pr := FromContext(ctx)
	_, size := pr.pageOf(0)
	size = min(size, 50)
	var items []core.Title
	rq := r.URL.Query()
	wantType := typeFromParam(rq.Get("type"))
	wantSection, _ := strconv.Atoi(rq.Get("sectionID"))
	for _, sec := range s.userSections(pr.User) {
		if wantSection > 0 && sec.ID != wantSection || wantType != "" && wantType != sec.Type {
			continue
		}
		q := core.BrowseQuery{Filter: core.Filter{Type: sec.Type}, Sort: core.SortAdded, Desc: true, Limit: size}
		if pr.User != nil {
			q.Filter.MaxContentRating = pr.User.MaxContentRating
		}
		page, err := s.d.Catalog.Browse(ctx, q)
		if err != nil {
			s.writeErr(w, r, err)
			return
		}
		items = append(items, page.Items...)
	}
	sort.SliceStable(items, func(i, j int) bool { return items[i].UpdatedAt.After(items[j].UpdatedAt) })
	if len(items) > size {
		items = items[:size]
	}
	mc := libraryContainer()
	mc.Title1 = "Recently Added"
	mc.Mixed = model.Ptr(true)
	mc.Metadata = s.renderTitles(ctx, pr, items, renderOpts{media: true})
	mc.Size = len(mc.Metadata)
	s.write(w, r, mc)
}

// handleLibraryAll is /library/all?type=N across sections.
func (s *Server) handleLibraryAll(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	t := typeFromParam(q.Get("type"))
	sec, ok := s.sectionFor(t)
	if !ok || t == "" || !s.canSeeSection(FromContext(r.Context()).User, sec.ID) {
		s.write(w, r, libraryContainer())
		return
	}
	s.sectionList(w, r, sec, "all")
}
