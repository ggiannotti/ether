package plex

// "Play Version" support (roadmap item #8): official Plex apps render a
// native version picker whenever a <Video> item has several <Media>
// children. This file renders one Media per distinct candidate release on
// detail pages, gives non-Auto variants stable integer Media/Part ids,
// and maps a viewer's selection (mediaIndex on decision/start requests,
// or a variant Part id on direct-play requests) back to the chosen
// release for core.VariantPlayback.PrepareVariant.
//
// PMS shapes (python-plexapi plexapi/media.py, plexapi/video.py):
//   - Media carries `title` (the version name Plex apps show for named
//     versions; optimized versions also carry `target`/`proxyType`, which
//     Ether does not claim) — Ether sets `title` to the variant label.
//   - Movie editions use `editionTitle` on the <Video> item — Ether sets
//     it to the Auto variant's cut when the default pick is a cut.
//
// Ids: rating keys already sit near 10^15 with kind digits 1-9 at 10^15,
// so variant ids live in the free 8*10^15 block (Kind() == 8 is not a
// RatingKey kind, so ParseRatingKey rejects them and nothing mistakes
// them for items) and stay below 2^53 for JavaScript clients. The id is a
// hash of (ratingKey, release ID) with linear collision probing; mappings
// are persisted in the variants_ids table so old part keys keep working
// across restarts (without a DB they are recomputed deterministically,
// matching streamRegistry's approach). The Auto variant keeps
// Media.ID == Part.ID == ratingKey, exactly as before variants existed.

import (
	"context"
	"database/sql"
	"fmt"
	"hash/fnv"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/braedonsaunders/ether/internal/core"
	"github.com/braedonsaunders/ether/internal/plex/model"
	"github.com/braedonsaunders/ether/internal/store"
)

func init() {
	store.Register("variants", 1, `
CREATE TABLE variants_ids (
  media_id INTEGER PRIMARY KEY,
  rating_key INTEGER NOT NULL,
  release_id TEXT NOT NULL
);
CREATE UNIQUE INDEX variants_ids_pair ON variants_ids(rating_key, release_id);`)
}

const (
	variantIDBase  = 9_000_000_000_000_000
	variantIDSpace = 7_199_254_740_990 // ids stay below 2^53

	maxVariantIDs       = 200_000
	renderedVariantsTTL = 30 * time.Minute
	maxRenderedTitles   = 10_000
	variantRenderBudget = 3 * time.Second // incl. the resolver's in-flight wait
)

// ---------------------------------------------------------------------------
// Variant Media/Part ids

type variantRef struct {
	key       core.RatingKey
	releaseID string
}

type variantIDRegistry struct {
	mu     sync.Mutex
	db     *sql.DB
	byID   map[int64]variantRef
	byPair map[variantRef]int64
	loaded bool
}

func newVariantIDRegistry(db *sql.DB) *variantIDRegistry {
	return &variantIDRegistry{db: db, byID: map[int64]variantRef{}, byPair: map[variantRef]int64{}}
}

// loadLocked reads persisted mappings once; the caller holds mu.
func (r *variantIDRegistry) loadLocked() {
	if r.loaded {
		return
	}
	r.loaded = true
	if r.db == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := r.db.QueryContext(ctx, `SELECT media_id, rating_key, release_id FROM variants_ids`)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var id, rk int64
		var rel string
		if rows.Scan(&id, &rk, &rel) == nil {
			ref := variantRef{key: core.RatingKey(rk), releaseID: rel}
			r.byID[id] = ref
			r.byPair[ref] = id
		}
	}
	if err := rows.Err(); err != nil {
		slog.Default().Warn("plex: variant ids not fully loaded", "err", err)
	}
}

func variantHash(key core.RatingKey, releaseID string) int64 {
	h := fnv.New64a()
	h.Write([]byte(strconv.FormatInt(int64(key), 10) + "/" + releaseID))
	return variantIDBase + int64(h.Sum64()%variantIDSpace) + 1
}

// idFor returns the stable Media/Part id of a non-Auto variant.
func (r *variantIDRegistry) idFor(key core.RatingKey, releaseID string) int64 {
	ref := variantRef{key: key, releaseID: releaseID}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.loadLocked()
	if id, ok := r.byPair[ref]; ok {
		return id
	}
	if len(r.byID) >= maxVariantIDs && r.db == nil {
		r.byID = map[int64]variantRef{}
		r.byPair = map[variantRef]int64{}
	}
	id := variantHash(key, releaseID)
	for {
		existing, ok := r.byID[id]
		if !ok || existing == ref {
			break
		}
		id++ // collision: probe (the space is ~10^15 wide, so this terminates)
		if id >= variantIDBase+variantIDSpace {
			id = variantIDBase + 1
		}
	}
	r.byID[id] = ref
	r.byPair[ref] = id
	if r.db != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, _ = r.db.ExecContext(ctx,
			`INSERT INTO variants_ids(media_id, rating_key, release_id) VALUES (?,?,?) ON CONFLICT DO NOTHING`,
			id, int64(key), releaseID)
		cancel()
	}
	return id
}

func (r *variantIDRegistry) lookup(id int64) (variantRef, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.loadLocked()
	ref, ok := r.byID[id]
	return ref, ok
}

// partTitle maps a Part id back to its title: the rating key itself for
// the Auto part, or a registry id for a variant part (with its release).
func (s *Server) partTitle(partID int64) (key core.RatingKey, releaseID string, ok bool) {
	if s.variantIDs != nil {
		if ref, found := s.variantIDs.lookup(partID); found {
			return ref.key, ref.releaseID, true
		}
	}
	k := core.RatingKey(partID)
	if k.Valid() && isPlayableKey(k) {
		return k, "", true
	}
	return 0, "", false
}

// ---------------------------------------------------------------------------
// Rendered variant lists (for mediaIndex resolution)

type renderedVariantKey struct {
	userID   int64
	clientID string
	key      core.RatingKey
}

type renderedVariantSet struct {
	releaseIDs []string // rendered order, Auto first
	at         time.Time
}

// renderedVariants remembers, per viewer+client+title, the variant order
// last rendered on a detail page. Clients select a version by position
// (mediaIndex) without naming the release, so playback must resolve the
// position against what that viewer actually saw — variant order depends
// on the client's capabilities.
type renderedVariants struct {
	mu sync.Mutex
	m  map[renderedVariantKey]renderedVariantSet
}

func newRenderedVariants() *renderedVariants {
	return &renderedVariants{m: map[renderedVariantKey]renderedVariantSet{}}
}

func (r *renderedVariants) put(k renderedVariantKey, releaseIDs []string, now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.m) >= maxRenderedTitles {
		r.m = map[renderedVariantKey]renderedVariantSet{}
	}
	r.m[k] = renderedVariantSet{releaseIDs: append([]string(nil), releaseIDs...), at: now}
}

func (r *renderedVariants) get(k renderedVariantKey, now time.Time) ([]string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.m[k]
	if !ok || now.Sub(e.at) > renderedVariantsTTL {
		return nil, false
	}
	return append([]string(nil), e.releaseIDs...), true
}

func (r *renderedVariants) expire(now time.Time, ttl time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for k, e := range r.m {
		if now.Sub(e.at) > ttl {
			delete(r.m, k)
		}
	}
}

// ---------------------------------------------------------------------------
// Rendering

// mediaForVariants renders one Media per release variant for a detail
// page. It returns (nil, "") to fall back to the single virtual Media:
// no variant resolver wired, nothing cached yet, or only Auto exists.
// The second value is the item's editionTitle, or "".
func (s *Server) mediaForVariants(ctx context.Context, pr *Request, t *core.Title, stamp int64) ([]model.Media, string) {
	if s.d.Variants == nil || pr.User == nil {
		return nil, ""
	}
	caps := pr.ClientCaps(s.cfg().Transcode.RemoteMaxBitrateKbps)
	vctx, cancel := context.WithTimeout(ctx, variantRenderBudget)
	defer cancel()
	vs, err := s.d.Variants.Variants(vctx, core.ResolveRequest{Title: t, Client: caps, User: pr.User}, 4)
	if err != nil {
		s.log.Debug("variants unavailable", "key", t.Key, "err", err)
		return nil, ""
	}
	if len(vs) < 2 {
		return nil, ""
	}
	ids := make([]string, len(vs))
	for i, v := range vs {
		ids[i] = v.Release.ID
	}
	s.renderedVariants.put(renderedVariantKey{userID: pr.User.ID, clientID: pr.ClientID, key: t.Key}, ids, s.now())

	out := make([]model.Media, 0, len(vs))
	for _, v := range vs {
		m := buildMediaVariant(t, v, stamp, s.variantIDs)
		if s.d.Trickplay != nil && s.d.Trickplay.TrickplayAvailable(v.Release.ID) {
			m.Part[0].Indexes = "sd"
		}
		if v.Auto {
			// Only the default version carries the stream list on a
			// detail page (audio/subtitle pickers operate on it); the
			// other versions' streams are still estimates. Cached
			// sidecars join it as in mediaFor.
			info := s.withCachedSidecars(ctx, pr.User, t, v.Info)
			sel := resolveSelection(s.selections.get(pr.User.ID, int64(t.Key)), info)
			sel = s.preferredSelection(ctx, pr.User, t.Key, info, t.Language, sel)
			m.Part[0].Stream = s.buildStreams(t.Key, info, sel, pr.User)
		}
		out = append(out, m)
	}
	edition := ""
	if t.Type == core.TypeMovie {
		edition = vs[0].Edition
	}
	return out, edition
}

// buildMediaVariant renders one variant's Media/Part. Auto keeps the
// historical ids (ratingKey); other variants get their registry id and
// the variant label as the Media title Plex apps show in "Play Version".
func buildMediaVariant(t *core.Title, v core.ReleaseVariant, stamp int64, registry *variantIDRegistry) model.Media {
	m := buildMedia(t, v.Info, stamp)
	var id int64
	if v.Auto || registry == nil {
		id = int64(t.Key)
	} else {
		id = registry.idFor(t.Key, v.Release.ID)
	}
	if id != int64(t.Key) {
		ext := containerExt(plexContainer(v.Info.Container))
		m.ID = id
		m.Part[0].ID = id
		m.Part[0].Key = fmt.Sprintf("/library/parts/%d/%d/file.%s", id, stamp, ext)
	}
	if v.Label != "" {
		m.Extra = []model.Attr{model.A("title", v.Label)}
	}
	return m
}

// applyEditionTitle sets the item's editionTitle (PMS's edition label for
// movies) when the default version is a distinct cut.
func applyEditionTitle(m *model.Metadata, edition string) {
	if edition == "" {
		return
	}
	m.Extra = append(m.Extra, model.A("editionTitle", edition))
}

// ---------------------------------------------------------------------------
// Selection → playback

// resolveVariantSelection maps a play request's mediaIndex back to the
// release the viewer picked, using the list rendered for them; when that
// memory is gone (server restart, TTL) it recomputes the list, which is
// deterministic for the same cached search results and client. It
// returns "" for Auto (or when the index cannot be resolved).
// partIDFromPath extracts the numeric Part id from a
// "/library/parts/{id}/..." path, as sent by clients that address the
// chosen version's part key in decision/start requests.
func partIDFromPath(p string) (int64, bool) {
	i := strings.Index(p, "/library/parts/")
	if i < 0 {
		return 0, false
	}
	p = p[i+len("/library/parts/"):]
	p, _, _ = strings.Cut(p, "/")
	p, _, _ = strings.Cut(p, "?")
	id, err := strconv.ParseInt(p, 10, 64)
	return id, err == nil
}

func (s *Server) resolveVariantSelection(ctx context.Context, pr *Request, p *playRequest) string {
	if p.partReleaseID != "" {
		return p.partReleaseID
	}
	if p.mediaIndex <= 0 || pr.User == nil {
		return ""
	}
	k := renderedVariantKey{userID: pr.User.ID, clientID: pr.ClientID, key: p.key}
	if ids, ok := s.renderedVariants.get(k, s.now()); ok {
		if p.mediaIndex < len(ids) {
			return ids[p.mediaIndex]
		}
		s.log.Info("variant mediaIndex out of range; playing auto version",
			"key", p.key, "mediaIndex", p.mediaIndex, "rendered", len(ids))
		return ""
	}
	if s.d.Variants == nil || s.d.Catalog == nil {
		return ""
	}
	t, err := s.d.Catalog.Get(ctx, p.key)
	if err != nil {
		return ""
	}
	vctx, cancel := context.WithTimeout(ctx, variantRenderBudget)
	defer cancel()
	vs, err := s.d.Variants.Variants(vctx, core.ResolveRequest{Title: t, Client: p.caps, User: pr.User}, 4)
	if err != nil || p.mediaIndex >= len(vs) {
		return ""
	}
	return vs[p.mediaIndex].Release.ID
}

// preparePlayback prepares a play session, honouring a "Play Version"
// pick when the playback manager supports it.
func (s *Server) preparePlayback(ctx context.Context, pr *Request, p *playRequest) (*core.PlaySession, error) {
	releaseID := s.resolveVariantSelection(ctx, pr, p)
	return s.prepareForRelease(ctx, pr.User, p.key, p.caps, releaseID)
}

// prepareForRelease prepares a session on a specific release ("" = Auto).
// When the playback manager cannot target a release, it logs and plays
// the normal choice instead of failing.
func (s *Server) prepareForRelease(ctx context.Context, user *core.User, key core.RatingKey, caps core.ClientCaps, releaseID string) (*core.PlaySession, error) {
	if releaseID != "" {
		if vp, ok := s.d.Playback.(core.VariantPlayback); ok {
			return vp.PrepareVariant(ctx, user, key, caps, releaseID)
		}
		s.log.Info("variant pick ignored: playback manager has no variant support",
			"key", key, "release", releaseID)
	}
	return s.d.Playback.Prepare(ctx, user, key, caps)
}
