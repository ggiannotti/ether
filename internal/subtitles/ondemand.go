package subtitles

// Cache-only sidecar listing, viewer-driven subtitle search ("on-demand"
// subtitles in Plex's terms) and the detail-page prefetch.
//
// Sidecars reach Plex clients as external Stream elements on the title's
// Part. Before a play session exists the Plex front end only has an
// estimated MediaInfo, so it lists the sidecars already on disk for the
// title (CachedSidecars) rather than searching on the request path. The
// files themselves are fetched by the detail-page warm-up (playback's
// AugmentMedia on the warmed release), by PrefetchSidecars when that
// warm-up does not run, at Play, or when the viewer picks a search result.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/braedonsaunders/ether/internal/core"
	"github.com/braedonsaunders/ether/internal/langtag"
)

// manualSig is the index-file signature of a viewer-picked track. Release
// signatures are hex or "any", so it cannot collide with one.
const manualSig = "manual"

const (
	offerTTL  = time.Hour
	maxOffers = 4096

	prefetchConcurrency = 2
	prefetchPerHour     = 20
	prefetchTimeout     = 45 * time.Second
)

type offerKey struct {
	key    core.RatingKey
	fileID int64
}

// offer is a search result shown to a viewer, kept so a later pick can
// only download a file that search actually returned for that title.
type offer struct {
	cand Candidate
	lang string
	at   time.Time
}

func sidecarStream(index int, lang string, sel selection) core.StreamInfo {
	return core.StreamInfo{
		Index: index, Type: "subtitle", Codec: "srt", Language: lang, Forced: sel.Forced,
		Title: sidecarTitle(lang, sel.Release), External: true, ExternalURL: URLPrefix + sel.ID,
	}
}

func sortedLanguages(m map[string]selection) []string {
	out := make([]string, 0, len(m))
	for l := range m {
		out = append(out, l)
	}
	sort.Strings(out)
	return out
}

type indexedSelection struct {
	lang, sig string
	sel       selection
}

// titleSelections reads every remembered selection of a title whose files
// are still cached: one per language and release signature, plus viewer
// picks (sig manualSig).
func (s *Service) titleSelections(key core.RatingKey) []indexedSelection {
	if s.dir == "" {
		return nil
	}
	prefix := strconv.FormatInt(int64(key), 10) + "."
	matches, err := filepath.Glob(filepath.Join(s.dir, "index", prefix+"*.json"))
	if err != nil {
		return nil
	}
	var out []indexedSelection
	for _, path := range matches {
		parts := strings.Split(strings.TrimPrefix(filepath.Base(path), prefix), ".")
		if len(parts) != 3 || parts[2] != "json" || parts[0] == "" {
			continue
		}
		b, err := os.ReadFile(path)
		if err != nil || len(b) > 64<<10 {
			continue
		}
		var sel selection
		if json.Unmarshal(b, &sel) != nil || sel.ID == "" || !s.cached(sel.ID, "srt") || !s.cached(sel.ID, "vtt") {
			continue
		}
		out = append(out, indexedSelection{lang: parts[0], sig: parts[1], sel: sel})
	}
	return out
}

// manualSelections returns a title's viewer-picked tracks by language.
func (s *Service) manualSelections(key core.RatingKey) map[string]selection {
	out := map[string]selection{}
	for _, e := range s.titleSelections(key) {
		if e.sig == manualSig {
			out[e.lang] = e.sel
		}
	}
	return out
}

func (s *Service) manualPath(key core.RatingKey, lang string) string {
	return filepath.Join(s.dir, "index", fmt.Sprintf("%d.%s.%s.json", int64(key), lang, manualSig))
}

// CachedSidecars lists, without any network call, the sidecars already on
// disk for a title: viewer-picked tracks, then for each of the viewer's
// preferred languages missing from embedded the most recently fetched
// track (whichever release it was matched to). Streams whose ExternalURL
// is already in embedded are skipped, so it can top up a session's
// enriched MediaInfo too. Nil when the service is disabled.
func (s *Service) CachedSidecars(title *core.Title, user *core.User, embedded []core.StreamInfo) []core.StreamInfo {
	if title == nil || !s.Enabled() {
		return nil
	}
	all := s.titleSelections(title.Key)
	if len(all) == 0 {
		return nil
	}
	manual := map[string]selection{}
	newest := map[string]selection{}
	for _, e := range all {
		if e.sig == manualSig {
			manual[e.lang] = e.sel
			continue
		}
		if cur, ok := newest[e.lang]; !ok || e.sel.At.After(cur.At) {
			newest[e.lang] = e.sel
		}
	}
	present := map[string]bool{}
	nextIndex := 0
	for _, st := range embedded {
		if st.External && st.ExternalURL != "" {
			present[st.ExternalURL] = true
		}
		if st.Index >= nextIndex {
			nextIndex = st.Index + 1
		}
	}
	var out []core.StreamInfo
	add := func(lang string, sel selection) {
		if present[URLPrefix+sel.ID] {
			return
		}
		present[URLPrefix+sel.ID] = true
		out = append(out, sidecarStream(nextIndex, lang, sel))
		nextIndex++
	}
	for _, lang := range MissingLanguages(user, embedded) {
		if sel, ok := manual[lang]; ok {
			add(lang, sel)
			delete(manual, lang)
		} else if sel, ok := newest[lang]; ok {
			add(lang, sel)
		}
	}
	for _, lang := range sortedLanguages(manual) {
		add(lang, manual[lang])
	}
	return out
}

// SearchOnDemand is the viewer-driven search behind Plex's subtitle
// "Search": ranked OpenSubtitles candidates for one language (releaseName,
// when a session knows it, improves the ranking). Results are remembered
// for an hour so DownloadOnDemand only fetches files offered for the title.
func (s *Service) SearchOnDemand(ctx context.Context, title *core.Title, lang, releaseName string) ([]Candidate, error) {
	if title == nil || !s.Enabled() {
		return nil, nil
	}
	lang = langtag.Normalize(lang)
	if lang == "" {
		return nil, fmt.Errorf("subtitle search needs a language")
	}
	cands, err := s.SearchWithHash(ctx, title, lang, releaseName, "")
	if err != nil {
		return nil, err
	}
	now := s.now()
	s.offersMu.Lock()
	if s.offers == nil || len(s.offers)+len(cands) > maxOffers {
		kept := map[offerKey]offer{}
		for k, o := range s.offers {
			if now.Sub(o.at) < offerTTL && len(kept) < maxOffers/2 {
				kept[k] = o
			}
		}
		s.offers = kept
	}
	for _, c := range cands {
		s.offers[offerKey{title.Key, c.FileID}] = offer{cand: c, lang: lang, at: now}
	}
	s.offersMu.Unlock()
	return cands, nil
}

// DownloadOnDemand fetches a file a recent SearchOnDemand offered for the
// title, remembers it as the title's pick for its language (replacing any
// earlier pick) and returns its sidecar stream. core.ErrNotFound means the
// file was not offered for this title (or the offer expired).
func (s *Service) DownloadOnDemand(ctx context.Context, title *core.Title, fileID int64) (core.StreamInfo, error) {
	if title == nil || !s.Enabled() || s.dir == "" {
		return core.StreamInfo{}, core.ErrNotFound
	}
	s.offersMu.Lock()
	o, ok := s.offers[offerKey{title.Key, fileID}]
	s.offersMu.Unlock()
	if !ok || s.now().Sub(o.at) >= offerTTL {
		return core.StreamInfo{}, core.ErrNotFound
	}
	id := cacheID(title, o.lang, fileID)
	if _, err := s.fetchAndCache(ctx, id, o.cand); err != nil {
		return core.StreamInfo{}, err
	}
	release := o.cand.Release
	if release == "" {
		release = o.cand.FileName
	}
	sel := selection{ID: id, FileID: fileID, Language: o.lang, Release: release, At: s.now().UTC(), Forced: o.cand.Forced}
	p := s.manualPath(title.Key, o.lang)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return core.StreamInfo{}, err
	}
	b, _ := json.Marshal(sel)
	if err := writeFileAtomic(p, b); err != nil {
		return core.StreamInfo{}, err
	}
	return sidecarStream(0, o.lang, sel), nil
}

// OpenFormatAt is OpenFormat with the cue times moved back by offset, for
// a transcode session whose timeline starts offset into the title. Cues
// that end before the offset are dropped.
func (s *Service) OpenFormatAt(id, format string, offset time.Duration) (io.ReadCloser, string, error) {
	if offset <= 0 {
		return s.OpenFormat(id, format)
	}
	rc, _, err := s.OpenFormat(id, "srt")
	if err != nil {
		return nil, "", err
	}
	b, err := io.ReadAll(io.LimitReader(rc, 32<<20))
	rc.Close()
	if err != nil {
		return nil, "", err
	}
	format = strings.ToLower(strings.TrimPrefix(format, "."))
	cues, err := ParseCues(string(b))
	if err == nil {
		cues, err = Shift(cues, -offset, 1)
	}
	if err != nil {
		return nil, "", err
	}
	text := RenderSRT(cues)
	if format == "vtt" {
		text = RenderVTT(cues)
	} else {
		format = "srt"
	}
	return io.NopCloser(strings.NewReader(text)), format, nil
}

// PrefetchSidecars fetches, in the background, the viewer's preferred
// languages a title has no cached sidecar for. The Plex front end calls it
// when a detail page opens and the playback warm-up (which fetches
// sidecars for the release it resolves) did not run, so the next
// metadata or play-queue request can list them. Without a release name
// the best-ranked track is cached under the release-agnostic signature.
//
// Downloads count against the OpenSubtitles daily quota, so prefetches
// are budgeted: one per title at a time, two server-wide, twenty an hour,
// none while the quota is exhausted. False means nothing was started.
func (s *Service) PrefetchSidecars(title *core.Title, user *core.User) bool {
	if title == nil || user == nil || !s.Enabled() {
		return false
	}
	switch title.Type {
	case core.TypeMovie, core.TypeEpisode:
	default:
		return false
	}
	missing := MissingLanguages(user, nil)
	if len(missing) == 0 {
		return false
	}
	have := map[string]bool{}
	for _, st := range s.CachedSidecars(title, user, nil) {
		have[st.Language] = true
	}
	need := false
	for _, l := range missing {
		if !have[l] {
			need = true
			break
		}
	}
	if !need {
		return false
	}
	now := s.now()
	s.mu.Lock()
	quota := s.quotaTill
	s.mu.Unlock()
	if now.Before(quota) {
		return false
	}
	s.prefetchMu.Lock()
	if s.prefetching == nil {
		s.prefetching = map[core.RatingKey]bool{}
	}
	recent := s.prefetchRuns[:0]
	for _, at := range s.prefetchRuns {
		if now.Sub(at) < time.Hour {
			recent = append(recent, at)
		}
	}
	s.prefetchRuns = recent
	if s.prefetching[title.Key] || len(s.prefetching) >= prefetchConcurrency || len(recent) >= prefetchPerHour {
		s.prefetchMu.Unlock()
		return false
	}
	s.prefetching[title.Key] = true
	s.prefetchRuns = append(s.prefetchRuns, now)
	s.prefetchWG.Add(1)
	s.prefetchMu.Unlock()

	t, u := *title, *user
	go func() {
		defer s.prefetchWG.Done()
		defer func() {
			s.prefetchMu.Lock()
			delete(s.prefetching, t.Key)
			s.prefetchMu.Unlock()
		}()
		ctx, cancel := context.WithTimeout(context.Background(), prefetchTimeout)
		defer cancel()
		if _, err := s.SidecarsForRelease(ctx, &t, &u, nil, ""); err != nil {
			s.log.Debug("subtitle prefetch", "key", t.Key, "err", err)
		}
	}()
	return true
}
