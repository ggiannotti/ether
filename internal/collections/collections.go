// Package collections is Ether's collection engine. It implements
// core.Collections (what Plex sees in the Collections tab), core.CollectionRows
// (owner collections offered to the personalization engine as Home and
// section rows) and the Collection Studio behind the owner UI.
//
// A collection is a core.CollectionSpec: candidates gathered from sources
// (the catalogue, TMDB lists, collections, discover, keywords, companies,
// networks, people and streaming services, public Trakt, Letterboxd and
// MDBList lists, hand-picked titles), filtered by rules, sorted, limited,
// optionally rotated, and published with generated artwork. External titles
// join the browsable catalogue so every member is playable. Specs refresh
// on a schedule in the background, or live on request.
//
// TMDB franchises (the catalogue's TMDB collections with two or more
// titles) are listed alongside, as before.
package collections

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/braedonsaunders/ether/internal/artwork"
	"github.com/braedonsaunders/ether/internal/catalog"
	"github.com/braedonsaunders/ether/internal/clusterbus"
	"github.com/braedonsaunders/ether/internal/config"
	"github.com/braedonsaunders/ether/internal/core"
	"github.com/braedonsaunders/ether/internal/flight"
)

var (
	_ core.Collections    = (*Service)(nil)
	_ core.CollectionRows = (*Service)(nil)
)

// TitleUpserter adds titles to the catalogue mirror; *catalog.Catalog
// satisfies it. Collection members found outside the catalogue are joined
// to the browsable set through it.
type TitleUpserter interface {
	UpsertTitle(ctx context.Context, t core.Title, browsable bool) error
}

// Service is the collection engine.
type Service struct {
	db       *sql.DB
	mirror   *catalog.Mirror
	cat      core.Catalog
	log      *slog.Logger
	now      func() time.Time
	upserter TitleUpserter
	api      *tmdbAPI
	names    *nameResolver
	idx      *index
	web      *webClient
	bases    ExternalBases
	depsMu   sync.RWMutex
	state    core.State
	auth     core.Auth
	chat     Chatter
	// Ephemeral vibe-search collections (vibe_ephemeral.go).
	vibeRefiner core.VibeRefiner
	vibe        *vibeRegistry
	vibeTTL     time.Duration
	vibeMax     int
	scorer      atomic.Pointer[scorerBox]
	art         *artwork.Renderer
	images      *artwork.Images
	catCfg      func() config.Catalog
	traktCfg    func() config.Trakt

	liveTTL     time.Duration
	tick        time.Duration
	buildBudget time.Duration

	mu       sync.Mutex
	members  map[int64]*memberSet
	flights  flight.Group[int64, *memberSet]
	personal map[string]personalEntry

	invalMu sync.Mutex
	inval   *clusterbus.Invalidator // nil on a standalone server

	// settings captured from options before New finishes.
	opts options
}

type scorerBox struct{ s core.PersonalScorer }

type options struct {
	tmdb       TMDB
	httpClient *http.Client
	userAgent  string
	indexTTL   time.Duration
	bases      *ExternalBases
	noSeed     bool
}

// Option customises a Service.
type Option func(*Service)

// WithNow sets the clock (tests).
func WithNow(fn func() time.Time) Option { return func(s *Service) { s.now = fn } }

// WithTMDB enables TMDB-backed sources, lookups and rule pushdown.
func WithTMDB(c TMDB) Option { return func(s *Service) { s.opts.tmdb = c } }

// WithState enables per-user rules (watched, in progress).
func WithState(st core.State) Option { return func(s *Service) { s.SetState(st) } }

// WithAuth lets previews evaluate per-user rules as the owner.
func WithAuth(a core.Auth) Option { return func(s *Service) { s.SetAuth(a) } }

// WithScorer sets the personal scorer used by the "personal" sort.
func WithScorer(sc core.PersonalScorer) Option {
	return func(s *Service) { s.scorer.Store(&scorerBox{sc}) }
}

// WithLLM enables the natural-language collection generator.
func WithLLM(c Chatter) Option { return func(s *Service) { s.SetLLM(c) } }

// WithArtwork enables generated collection posters.
func WithArtwork(r *artwork.Renderer, im *artwork.Images) Option {
	return func(s *Service) { s.art, s.images = r, im }
}

// WithCatalogConfig supplies the catalogue settings (region for streaming
// services, whether unreleased titles are shown).
func WithCatalogConfig(fn func() config.Catalog) Option { return func(s *Service) { s.catCfg = fn } }

// WithTraktConfig supplies the Trakt client id used to read public lists.
func WithTraktConfig(fn func() config.Trakt) Option { return func(s *Service) { s.traktCfg = fn } }

// WithHTTPClient sets the client for Letterboxd, MDBList and Trakt.
func WithHTTPClient(c *http.Client) Option { return func(s *Service) { s.opts.httpClient = c } }

// WithUserAgent identifies Ether to list sites.
func WithUserAgent(ua string) Option { return func(s *Service) { s.opts.userAgent = ua } }

// WithExternalBases points external list fetches at other origins (tests).
func WithExternalBases(b ExternalBases) Option { return func(s *Service) { s.opts.bases = &b } }

// WithIndexTTL sets how often the catalogue index checks for changes
// (default 2m; 0 checks on every use).
func WithIndexTTL(d time.Duration) Option { return func(s *Service) { s.opts.indexTTL = d } }

// WithLiveTTL sets how long a live collection's evaluation is reused
// (default 2m).
func WithLiveTTL(d time.Duration) Option { return func(s *Service) { s.liveTTL = d } }

// WithSchedulerTick sets how often Run looks for collections due a refresh.
func WithSchedulerTick(d time.Duration) Option { return func(s *Service) { s.tick = d } }

// WithoutDefaults skips seeding the starter collections (tests).
func WithoutDefaults() Option { return func(s *Service) { s.opts.noSeed = true } }

// New creates the collection engine, migrating v1 smart collections and
// seeding built-ins and starter collections.
func New(db *sql.DB, cat core.Catalog, log *slog.Logger, opts ...Option) (*Service, error) {
	if db == nil {
		return nil, fmt.Errorf("collections: nil *sql.DB")
	}
	if cat == nil {
		return nil, fmt.Errorf("collections: nil Catalog")
	}
	if log == nil {
		log = slog.Default()
	}
	s := &Service{db: db, cat: cat, log: log, now: time.Now,
		liveTTL: 2 * time.Minute, tick: time.Minute, buildBudget: 20 * time.Second,
		members: map[int64]*memberSet{}, personal: map[string]personalEntry{},
		opts: options{indexTTL: 2 * time.Minute}}
	for _, fn := range opts {
		fn(s)
	}
	if u, ok := cat.(TitleUpserter); ok {
		s.upserter = u
	}
	s.api = newTMDBAPI(s.opts.tmdb, s.now)
	s.names = newNameResolver(db, s.api, s.now)
	s.mirror = catalog.NewMirror(db, s.now)
	s.idx = newIndex(db, s.now, s.opts.indexTTL)
	ua := s.opts.userAgent
	if ua == "" {
		ua = "Ether (+https://github.com/braedonsaunders/ether)"
	}
	s.web = newWebClient(s.opts.httpClient, ua, s.now)
	s.bases = defaultBases()
	if s.opts.bases != nil {
		s.bases = *s.opts.bases
	}
	ctx := context.Background()
	if err := s.seedBuiltins(ctx); err != nil {
		return nil, fmt.Errorf("collections: built-ins: %w", err)
	}
	if err := s.separateDiscoveryCollections(ctx); err != nil {
		return nil, err
	}
	if err := s.seedLibraryDefaults(ctx); err != nil {
		return nil, fmt.Errorf("collections: defaults: %w", err)
	}
	return s, nil
}

// SetScorer installs (or replaces) the personal scorer after construction,
// for when the personalization engine is built after the collections.
func (s *Service) SetScorer(sc core.PersonalScorer) { s.scorer.Store(&scorerBox{sc}) }

// SetState installs watch state for per-user rules (watched, in progress).
func (s *Service) SetState(st core.State) {
	s.depsMu.Lock()
	s.state = st
	s.depsMu.Unlock()
}

// SetAuth lets previews apply per-user rules as the owner.
func (s *Service) SetAuth(a core.Auth) {
	s.depsMu.Lock()
	s.auth = a
	s.depsMu.Unlock()
}

// SetLLM installs the model the collection generator drafts with.
func (s *Service) SetLLM(c Chatter) {
	s.depsMu.Lock()
	s.chat = c
	s.depsMu.Unlock()
}

func (s *Service) stateDep() core.State {
	s.depsMu.RLock()
	defer s.depsMu.RUnlock()
	return s.state
}

func (s *Service) authDep() core.Auth {
	s.depsMu.RLock()
	defer s.depsMu.RUnlock()
	return s.auth
}

func (s *Service) chatDep() Chatter {
	s.depsMu.RLock()
	defer s.depsMu.RUnlock()
	return s.chat
}

func (s *Service) personalScorer() core.PersonalScorer {
	if b := s.scorer.Load(); b != nil {
		return b.s
	}
	return nil
}

// ---------------------------------------------------------------------------
// Members: built, cached and served per user.

// memberSet is a collection's built members plus what serving needs.
type memberSet struct {
	keys      []core.RatingKey
	personal  []*rule
	personalX []*rule
	unshared  map[core.RatingKey]bool // see buildResult.unshared
	spec      core.CollectionSpec
	builtAt   time.Time
	hash      string
	loadedAt  time.Time
}

func (ms *memberSet) personalRules() personalRules {
	return personalRules{include: ms.personal, exclude: ms.personalX, any: ms.spec.Match == MatchAny, unshared: ms.unshared}
}

// ErrBuiltIn is returned when deleting a built-in collection.
var ErrBuiltIn = errors.New("built-in collections can be disabled but not deleted")

// conflictError marks errors the API reports as 409.
type conflictError struct{ error }

func (conflictError) Conflict() bool { return true }

func (s *Service) cached(id int64) *memberSet {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.members[id]
}

func (s *Service) invalidate(id int64) {
	s.mu.Lock()
	delete(s.members, id)
	for k := range s.personal {
		if pe := s.personal[k]; pe.id == id {
			delete(s.personal, k)
		}
	}
	s.mu.Unlock()
}

// membersFor returns a row's members, building when it never was built,
// was edited, or (live) the cached evaluation expired.
func (s *Service) membersFor(ctx context.Context, row *specRow, wait bool) (*memberSet, error) {
	if key, ok := localFacetSpec(row.spec); ok {
		snap, err := s.idx.get(ctx)
		if err != nil {
			return nil, err
		}
		keys := append([]core.RatingKey(nil), snap.facetKeys(key)...)
		row.itemCount = len(keys)
		return s.newMemberSet(row.spec, keys, nil, snap.builtAt, "local-facet"), nil
	}
	ms := s.cached(row.id)
	fresh := ms != nil && ms.builtAt.Equal(row.lastBuilt) && row.buildHash != ""
	if row.spec.Refresh == RefreshLive {
		fresh = ms != nil && s.now().Sub(ms.loadedAt) < s.liveTTL
	}
	if fresh {
		return ms, nil
	}
	if row.spec.Refresh != RefreshLive && !row.lastBuilt.IsZero() && row.buildHash != "" {
		keys, unshared, err := s.loadMembers(ctx, row.id)
		if err != nil {
			return nil, err
		}
		ms := s.newMemberSet(row.spec, keys, unshared, row.lastBuilt, row.buildHash)
		s.mu.Lock()
		s.members[row.id] = ms
		s.mu.Unlock()
		return ms, nil
	}
	if !wait {
		go func() {
			bctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			_, _ = s.build(bctx, row)
		}()
		if ms != nil {
			return ms, nil // stale while revalidating
		}
		return nil, errNotBuilt
	}
	built, err := s.build(ctx, row)
	if err != nil && ms != nil {
		s.log.Debug("collections: refresh failed; serving the previous build", "id", row.id, "err", err)
		return ms, nil
	}
	return built, err
}

var errNotBuilt = errors.New("collection not built yet")

func (s *Service) newMemberSet(sp core.CollectionSpec, keys []core.RatingKey, unshared map[core.RatingKey]bool, builtAt time.Time, hash string) *memberSet {
	ms := &memberSet{keys: keys, unshared: unshared, spec: sp, builtAt: builtAt, hash: hash, loadedAt: s.now()}
	if rs, err := compileRules(sp.Rules); err == nil {
		splitPersonal(rs, &ms.personal)
	}
	if xs, err := compileRules(sp.Exclude); err == nil {
		splitPersonal(xs, &ms.personalX)
	}
	return ms
}

// build evaluates a row and persists the result. Concurrent builds of the
// same row share one evaluation.
func (s *Service) build(ctx context.Context, row *specRow) (*memberSet, error) {
	// The evaluation is shared, so it must not die with the first caller.
	return s.flights.DoDetached(ctx, row.id, func(ctx context.Context) (*memberSet, error) {
		ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()
		ms, err := s.buildNow(ctx, row)
		if err == nil {
			s.mu.Lock()
			s.members[row.id] = ms
			s.mu.Unlock()
		}
		return ms, err
	})
}

func (s *Service) buildNow(ctx context.Context, row *specRow) (*memberSet, error) {
	started := time.Now()
	res, err := s.evaluate(ctx, row.spec, evalOptions{seed: row.publicID(), persist: true})
	if err != nil {
		return nil, err
	}
	// Remember what labels resolved to, so the Studio shows real ids.
	if !row.builtin && resolvedChanged(row.spec, res.resolved) {
		if js, err := specJSON(res.resolved); err == nil {
			if _, err := s.db.ExecContext(ctx, `UPDATE collections_specs SET spec_json=? WHERE id=?`, js, row.id); err != nil {
				s.log.Debug("collections: save resolved spec", "id", row.publicID(), "err", err)
			}
		}
	}
	at := s.now().UTC().Truncate(time.Second)
	if err := s.saveBuild(ctx, row.id, res.keys, res.unshared, res.itemCount(), res.warnings, res.hash, at); err != nil {
		return nil, err
	}
	s.log.Debug("collections: built", "id", row.publicID(), "name", row.spec.Name, "items", res.itemCount(),
		"warnings", len(res.warnings), "took", time.Since(started).Round(time.Millisecond))
	ms := &memberSet{keys: res.keys, personal: res.personal, personalX: res.personalX, unshared: res.unshared, spec: row.spec,
		builtAt: at, hash: res.hash, loadedAt: s.now()}
	if row.buildHash != res.hash {
		row.changedAt = at
	}
	row.lastBuilt, row.buildHash, row.itemCount, row.warnings = at, res.hash, res.itemCount(), res.warnings
	return ms, nil
}

func resolvedChanged(a, b core.CollectionSpec) bool {
	if len(a.Sources) != len(b.Sources) {
		return false
	}
	for i := range a.Sources {
		if a.Sources[i].ID != b.Sources[i].ID || a.Sources[i].Label != b.Sources[i].Label {
			return true
		}
	}
	return false
}

// personalEntry caches a user's personal ordering of a collection.
type personalEntry struct {
	id   int64
	hash string
	keys []core.RatingKey
	at   time.Time
}

// serve returns the members a user sees: content-rating and personal
// rules applied, personal order, limit, rotation.
func (s *Service) serve(ctx context.Context, row *specRow, user *core.User, wait bool) ([]core.RatingKey, error) {
	ms, err := s.membersFor(ctx, row, wait)
	if err != nil {
		return nil, err
	}
	snap := s.idx.current()
	if snap == nil {
		if snap, err = s.idx.get(ctx); err != nil {
			return nil, err
		}
	}
	keys := s.filterForUser(ctx, ms.keys, ms.personalRules(), user, snap)
	sp := ms.spec
	if sp.Sort == "personal" && user != nil {
		keys = s.personalOrder(ctx, row.id, ms.hash, user, keys)
	}
	if sp.Limit > 0 && len(keys) > sp.Limit {
		keys = keys[:sp.Limit]
	}
	if sp.Rotate != nil && sp.Rotate.Size > 0 {
		keys = rotate(keys, sp.Rotate.Size, sp.Rotate.Every, row.publicID(), s.now())
	}
	return keys, nil
}

const personalTTL = 10 * time.Minute

// personalOrder re-ranks members by the user's taste model.
func (s *Service) personalOrder(ctx context.Context, id int64, hash string, user *core.User, keys []core.RatingKey) []core.RatingKey {
	sc := s.personalScorer()
	if sc == nil || len(keys) < 2 {
		return keys
	}
	ck := fmt.Sprintf("%d|%d", id, user.ID)
	s.mu.Lock()
	if pe, ok := s.personal[ck]; ok && pe.hash == hash && s.now().Sub(pe.at) < personalTTL && len(pe.keys) == len(keys) {
		s.mu.Unlock()
		return pe.keys
	}
	s.mu.Unlock()
	const maxScored = 500
	head := keys
	if len(head) > maxScored {
		head = keys[:maxScored]
	}
	titles, err := loadTitles(ctx, s.db, head)
	if err != nil {
		return keys
	}
	scores, err := sc.ScoreTitles(ctx, user, titles)
	if err != nil || len(scores) == 0 {
		return keys
	}
	pos := make(map[core.RatingKey]int, len(head))
	for i, k := range head {
		pos[k] = i
	}
	ranked := append([]core.RatingKey(nil), head...)
	sort.SliceStable(ranked, func(i, j int) bool {
		si, sj := scores[ranked[i]], scores[ranked[j]]
		if si != sj {
			return si > sj
		}
		return pos[ranked[i]] < pos[ranked[j]]
	})
	out := append(ranked, keys[len(head):]...)
	s.mu.Lock()
	if len(s.personal) > 2000 {
		s.personal = map[string]personalEntry{}
	}
	s.personal[ck] = personalEntry{id: id, hash: hash, keys: out, at: s.now()}
	s.mu.Unlock()
	return out
}

// ---------------------------------------------------------------------------
// core.Collections

// visible reports whether a user may see a collection at all.
func visible(row *specRow, user *core.User) bool {
	if !row.enabled {
		return false
	}
	if row.owner != 0 {
		return user != nil && user.ID == row.owner
	}
	return row.spec.Targets.Users.Includes(user)
}

func typeMatches(sp core.CollectionSpec, t core.MediaType) bool {
	switch t {
	case "":
		return true
	case core.TypeMovie:
		return sp.MediaType == MediaMovie || sp.MediaType == MediaMixed
	case core.TypeShow:
		return sp.MediaType == MediaShow || sp.MediaType == MediaMixed
	}
	return false
}

func (s *Service) collectionFor(ctx context.Context, row *specRow, user *core.User, t core.MediaType, keys []core.RatingKey) core.Collection {
	c := core.Collection{Key: row.key(), Title: row.spec.Name, Summary: row.spec.Description, Kind: "smart",
		OwnerUserID: row.owner, ChildCount: len(keys)}
	if row.builtin {
		c.Kind = "trending"
	}
	switch row.spec.MediaType {
	case MediaMovie:
		c.Type = core.TypeMovie
	case MediaShow:
		c.Type = core.TypeShow
	default:
		c.Type = t
	}
	if snap := s.idx.current(); snap != nil {
		for _, k := range keys {
			e := snap.entries[k]
			if e == nil {
				continue
			}
			if c.Type == "" {
				c.Type = e.typ
			}
			if c.PosterPath == "" && e.poster != "" {
				c.PosterPath = e.poster
			}
			if c.BackdropPath == "" && e.backdrop != "" {
				c.BackdropPath = e.backdrop
			}
			if c.PosterPath != "" && c.BackdropPath != "" {
				break
			}
		}
	}
	if c.Type == "" {
		c.Type = core.TypeMovie
	}
	c.UpdatedAt = row.stamp()
	return c
}

// List returns the collections a user sees in a section's Collections tab:
// owner collections targeted at them, built-ins, and TMDB franchises.
func (s *Service) List(ctx context.Context, user *core.User, t core.MediaType) ([]core.Collection, error) {
	rows, err := s.loadRows(ctx)
	if err != nil {
		return nil, err
	}
	out := []core.Collection{}
	deadline := time.Now().Add(2 * time.Second)
	// Built-ins first, as Plex has always listed them.
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].builtin && !rows[j].builtin })
	for _, row := range rows {
		if !visible(row, user) || !row.spec.Targets.CollectionsTab || !typeMatches(row.spec, t) {
			continue
		}
		wait := row.spec.Refresh == RefreshLive || time.Now().Before(deadline)
		bctx, cancel := context.WithDeadline(ctx, deadline.Add(3*time.Second))
		keys, err := s.serve(bctx, row, user, wait)
		cancel()
		if err != nil {
			if !errors.Is(err, errNotBuilt) {
				s.log.Debug("collections: list", "id", row.publicID(), "err", err)
			}
			continue
		}
		if len(keys) == 0 {
			continue
		}
		out = append(out, s.collectionFor(ctx, row, user, t, keys))
	}
	out = append(out, s.listEphemeral(user, t)...)
	if t == "" || t == core.TypeMovie {
		fr, err := s.listFranchises(ctx, user)
		if err == nil {
			out = append(out, fr...)
		} else {
			s.log.Debug("collections: franchise list unavailable", "err", err)
		}
	}
	return out, nil
}

// Get returns one collection by key.
func (s *Service) Get(ctx context.Context, user *core.User, key core.RatingKey) (*core.Collection, error) {
	if key.IsEphemeral() {
		return s.getEphemeral(ctx, user, key)
	}
	switch key.Kind() {
	case core.KindCollection:
		return s.getFranchise(ctx, user, key)
	case core.KindSmart:
		row, err := s.rowByKey(ctx, key)
		if err != nil {
			return nil, err
		}
		if !visible(row, user) {
			return nil, core.ErrNotFound
		}
		keys, err := s.serve(ctx, row, user, true)
		if err != nil {
			return nil, err
		}
		c := s.collectionFor(ctx, row, user, "", keys)
		return &c, nil
	}
	return nil, core.ErrNotFound
}

// Items returns a page of a collection's titles as the user sees them.
func (s *Service) Items(ctx context.Context, user *core.User, key core.RatingKey, offset, limit int) (core.Page, error) {
	if limit <= 0 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	if key.IsEphemeral() {
		return s.ephemeralItems(ctx, user, key, offset, limit)
	}
	switch key.Kind() {
	case core.KindCollection:
		return s.franchiseItems(ctx, user, key, offset, limit)
	case core.KindSmart:
		row, err := s.rowByKey(ctx, key)
		if err != nil {
			return core.Page{}, err
		}
		if !visible(row, user) {
			return core.Page{}, core.ErrNotFound
		}
		keys, err := s.serve(ctx, row, user, true)
		if err != nil {
			return core.Page{}, err
		}
		total := len(keys)
		if offset >= total {
			return core.Page{Items: []core.Title{}, Total: total}, nil
		}
		items, err := loadTitles(ctx, s.db, keys[offset:pageEnd(offset, limit, total)])
		if err != nil {
			return core.Page{}, err
		}
		return core.Page{Items: items, Total: total}, nil
	}
	return core.Page{}, core.ErrNotFound
}

// CreateSmart persists a filter-based smart collection (core.Filter) as a spec.
// OwnerUserID 0 = server-wide; otherwise only that user sees it.
func (s *Service) CreateSmart(ctx context.Context, c core.Collection) (*core.Collection, error) {
	if c.Title == "" {
		return nil, fmt.Errorf("collections: smart collection needs a title")
	}
	if c.Filter == nil {
		return nil, fmt.Errorf("collections: smart collection needs a filter")
	}
	sp := specFromFilter(c)
	if err := Validate(&sp); err != nil {
		return nil, err
	}
	row, err := s.insertRow(ctx, sp, c.OwnerUserID)
	if err != nil {
		return nil, err
	}
	out := c
	out.Key = row.key()
	out.Kind = "smart"
	if out.Type == "" {
		out.Type = c.Filter.Type
	}
	if out.Sort == "" {
		out.Sort = core.SortPopularity
	}
	f := *c.Filter
	out.Filter = &f
	return &out, nil
}

// DeleteSmart removes an owner or user collection. Built-ins and
// franchises cannot be deleted.
func (s *Service) DeleteSmart(ctx context.Context, key core.RatingKey) error {
	row, err := s.rowByKey(ctx, key)
	if err != nil {
		return err
	}
	if row.builtin {
		return core.ErrNotFound
	}
	if err := s.deleteRow(ctx, row.id); err != nil {
		return err
	}
	s.changed(row.id)
	return nil
}

// ---------------------------------------------------------------------------
// Franchises (TMDB collections in the catalogue)

func (s *Service) listFranchises(ctx context.Context, user *core.User) ([]core.Collection, error) {
	out, err := s.mirror.Franchises(ctx)
	if err != nil {
		return nil, err
	}
	if user != nil && user.MaxContentRating != "" {
		// Child counts must reflect what this user may watch.
		for i := range out {
			if p, err := s.franchiseItems(ctx, user, out[i].Key, 0, 1); err == nil {
				out[i].ChildCount = p.Total
			}
		}
		kept := out[:0]
		for _, c := range out {
			if c.ChildCount > 0 {
				kept = append(kept, c)
			}
		}
		out = kept
	}
	return out, nil
}

func (s *Service) getFranchise(ctx context.Context, user *core.User, key core.RatingKey) (*core.Collection, error) {
	name, items, err := s.cat.Collection(ctx, key.TMDB())
	if err != nil {
		fr, ferr := s.listFranchises(ctx, nil)
		if ferr == nil {
			for _, c := range fr {
				if c.Key == key {
					c := c
					return &c, nil
				}
			}
		}
		return nil, err
	}
	allowed := filterTitlesByRating(items, user)
	c := &core.Collection{Key: key, Title: name, Kind: "franchise", Type: core.TypeMovie, ChildCount: len(allowed)}
	if art, err := s.mirror.CollectionArt(ctx, key.TMDB()); err == nil {
		c.PosterPath, c.BackdropPath, c.Summary = art.PosterPath, art.BackdropPath, art.Summary
	}
	if c.PosterPath == "" && len(items) > 0 {
		c.PosterPath = items[0].PosterPath
	}
	if c.BackdropPath == "" && len(items) > 0 {
		c.BackdropPath = items[0].BackdropPath
	}
	return c, nil
}

func (s *Service) franchiseItems(ctx context.Context, user *core.User, key core.RatingKey, offset, limit int) (core.Page, error) {
	_, items, err := s.cat.Collection(ctx, key.TMDB())
	if err != nil {
		return core.Page{}, err
	}
	items = filterTitlesByRating(items, user)
	sort.SliceStable(items, func(i, j int) bool { return items[i].Released.Before(items[j].Released) })
	total := len(items)
	if offset >= total {
		return core.Page{Items: []core.Title{}, Total: total}, nil
	}
	return core.Page{Items: items[offset:pageEnd(offset, limit, total)], Total: total}, nil
}

// pageEnd is the end of the page [offset, offset+limit) clipped to total,
// for 0 <= offset < total and limit > 0, without overflowing on huge limits.
func pageEnd(offset, limit, total int) int {
	if limit >= total-offset {
		return total
	}
	return offset + limit
}

func filterTitlesByRating(items []core.Title, user *core.User) []core.Title {
	if user == nil || user.MaxContentRating == "" {
		return items
	}
	out := make([]core.Title, 0, len(items))
	for _, t := range items {
		if !ratingExceeds(t.ContentRating, user.MaxContentRating) {
			out = append(out, t)
		}
	}
	return out
}

// BuiltinKey returns the rating key of a built-in collection by slug
// (e.g. "trending-week-movie"), or 0.
func BuiltinKey(slug string) core.RatingKey {
	for _, b := range builtins {
		if b.slug == slug {
			return core.SmartKey(b.id)
		}
	}
	return 0
}

func idString(id int64) string { return strconv.FormatInt(id, 10) }
