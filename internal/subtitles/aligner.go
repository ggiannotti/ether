package subtitles

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/braedonsaunders/ether/internal/core"
	"github.com/braedonsaunders/ether/internal/flight"
	"github.com/braedonsaunders/ether/internal/langtag"
)

// Aligner implements core.TimelineAligner: it maps a moment in one release
// of a title to the same moment in another release using only subtitle
// dialogue timing, so no video is ever downloaded. Timelines come from
// subtitles already cached on disk or from OpenSubtitles files matched to
// each release by name (or, when only a runtime is known, by how close the
// file's last cue lands to that runtime). Everything is off unless the
// OpenSubtitles service is enabled.
//
// Failover calls AlignTime with a sub-second deadline: on a cache miss
// that returns ok=false immediately and warms the cache in the background.
// Callers with time to spare (marker shifting) get a synchronous build.
type Aligner struct {
	s *Service

	mu       sync.Mutex
	cache    map[string]*Piecewise
	negative map[string]time.Time
	flights  flight.Group[string, *Piecewise]
}

var _ core.TimelineAligner = (*Aligner)(nil)

const (
	alignNegativeTTL = time.Hour
	alignBuildBudget = 6 // subtitle downloads per alignment build, both sides
	alignFastWindow  = 2 * time.Second
)

// TimelineAligner returns the service's (single) core.TimelineAligner.
// Wiring is one line each for markers and playback:
//
//	markers.New(markers.Options{..., Aligner: subs.TimelineAligner()})
//	playback.Deps{..., Aligner: subs.TimelineAligner()}
func (s *Service) TimelineAligner() *Aligner {
	s.alignOnce.Do(func() {
		s.aligner = &Aligner{
			s:        s,
			cache:    map[string]*Piecewise{},
			negative: map[string]time.Time{},
		}
	})
	return s.aligner
}

func sideSig(r core.Release, d time.Duration) string {
	switch {
	case r.ID != "":
		return "id:" + r.ID
	case r.Name != "":
		return "name:" + releaseSig(r.Name)
	default:
		return "dur:" + strconv.FormatInt(d.Milliseconds(), 10)
	}
}

func alignCacheKey(req core.AlignRequest) string {
	h := sha1.Sum([]byte(fmt.Sprintf("%d|%s|%s", int64(req.Key),
		sideSig(req.From, req.FromDuration), sideSig(req.To, req.ToDuration))))
	return hex.EncodeToString(h[:12])
}

// AlignTime implements core.TimelineAligner.
func (a *Aligner) AlignTime(ctx context.Context, req core.AlignRequest, at time.Duration) (time.Duration, bool) {
	if at < 0 || a == nil || !a.s.Enabled() {
		return 0, false
	}
	if sideSig(req.From, req.FromDuration) == sideSig(req.To, req.ToDuration) {
		return at, true // same release (or same known runtime): identity
	}
	key := alignCacheKey(req)
	if pw, ok := a.lookup(key); ok {
		return pw.Map(at)
	}
	if a.negRecent(key) {
		return 0, false
	}
	if dl, has := ctx.Deadline(); has && time.Until(dl) < alignFastWindow {
		a.Prewarm(req) // failover path: never block on the network
		return 0, false
	}
	pw := a.build(ctx, key, req)
	if pw == nil {
		return 0, false
	}
	return pw.Map(at)
}

// Prewarm starts a background alignment build for a release pair so a
// later failover or marker lookup hits the cache. Duplicate and
// recently-failed pairs are skipped.
func (a *Aligner) Prewarm(req core.AlignRequest) {
	if a == nil || !a.s.Enabled() {
		return
	}
	key := alignCacheKey(req)
	if _, ok := a.lookup(key); ok || a.negRecent(key) {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		a.build(ctx, key, req)
	}()
}

func (a *Aligner) negRecent(key string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	at, ok := a.negative[key]
	return ok && a.s.now().Sub(at) < alignNegativeTTL
}

func (a *Aligner) diskPath(key string) string {
	if a.s.dir == "" {
		return ""
	}
	return filepath.Join(a.s.dir, "align", key+".json")
}

// lookup serves the memory cache, then the on-disk cache (a small JSON
// read, fast enough for the failover deadline).
func (a *Aligner) lookup(key string) (*Piecewise, bool) {
	a.mu.Lock()
	pw, ok := a.cache[key]
	a.mu.Unlock()
	if ok {
		return pw, true
	}
	if p := a.diskPath(key); p != "" {
		if raw, err := os.ReadFile(p); err == nil {
			var stored Piecewise
			if json.Unmarshal(raw, &stored) == nil && len(stored.Segments) > 0 {
				a.mu.Lock()
				a.cache[key] = &stored
				a.mu.Unlock()
				return &stored, true
			}
		}
	}
	return nil, false
}

func (a *Aligner) store(key string, pw *Piecewise) {
	a.mu.Lock()
	a.cache[key] = pw
	delete(a.negative, key)
	a.mu.Unlock()
	if p := a.diskPath(key); p != "" {
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err == nil {
			if raw, err := json.Marshal(pw); err == nil {
				if err := writeFileAtomic(p, raw); err != nil {
					a.s.log.Debug("alignment cache write", "err", err)
				}
			}
		}
	}
}

// build single-flights the expensive part: timeline fetch + alignment.
func (a *Aligner) build(ctx context.Context, key string, req core.AlignRequest) *Piecewise {
	pw, _ := a.flights.Do(ctx, key, func(ctx context.Context) (*Piecewise, error) {
		ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
		defer cancel()
		pw := a.compute(ctx, req)
		if pw == nil {
			if err := ctx.Err(); err != nil {
				return nil, err // a cancelled build is not a negative result
			}
			a.mu.Lock()
			a.negative[key] = a.s.now()
			a.mu.Unlock()
			return nil, nil
		}
		a.store(key, pw)
		return pw, nil
	})
	return pw
}

func titleForAlign(key core.RatingKey) *core.Title {
	t := &core.Title{Key: key}
	switch key.Kind() {
	case core.KindEpisode:
		t.Type = core.TypeEpisode
		t.Season, t.Episode = key.Season(), key.Episode()
	case core.KindMovie:
		t.Type = core.TypeMovie
		t.IDs.TMDB = key.TMDB()
	}
	return t
}

type timeline struct {
	cues    []Cue
	fileID  int64
	release string
}

func (t timeline) last() time.Duration {
	if len(t.cues) == 0 {
		return 0
	}
	return t.cues[len(t.cues)-1].End
}

// buildState shares one OpenSubtitles search between both sides of a pair
// and caps total downloads, so a build costs at most one search per
// language and alignBuildBudget downloads against the owner's quota.
type buildState struct {
	searches  map[string][]Candidate
	downloads int
}

func (a *Aligner) compute(ctx context.Context, req core.AlignRequest) *Piecewise {
	title := titleForAlign(req.Key)
	if title.Type != core.TypeMovie && title.Type != core.TypeEpisode {
		return nil
	}
	bs := &buildState{searches: map[string][]Candidate{}}
	for _, lang := range a.s.alignLanguages {
		from := a.timelines(ctx, bs, title, lang, req.From.Name, req.FromDuration)
		to := a.timelines(ctx, bs, title, lang, req.To.Name, req.ToDuration)
		var best *Piecewise
		for _, ft := range from {
			for _, tt := range to {
				if ft.fileID != 0 && ft.fileID == tt.fileID {
					continue // same file on both sides proves nothing
				}
				pw, err := AlignPiecewise(ft.cues, tt.cues, AlignOptions{})
				if err != nil {
					continue
				}
				if best == nil || pw.Matched > best.Matched ||
					(pw.Matched == best.Matched && pw.Residual < best.Residual) {
					best = pw
				}
			}
		}
		if best != nil {
			return best
		}
		if ctx.Err() != nil {
			return nil
		}
	}
	return nil
}

func (a *Aligner) cuesByID(id string) ([]Cue, error) {
	r, _, err := a.s.Open(id)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	raw, err := io.ReadAll(io.LimitReader(r, 16<<20))
	if err != nil {
		return nil, err
	}
	return ParseCues(string(raw))
}

// durationFit rejects subtitle files that cannot belong to a release of
// the given runtime: dialogue ending well after the credits would, or a
// file that stops a third of the way through the film.
func durationFit(cues []Cue, dur time.Duration) bool {
	if dur <= 0 || len(cues) == 0 {
		return len(cues) > 0
	}
	last := cues[len(cues)-1].End
	return last <= dur+2*time.Minute && last >= dur/3
}

// timelines returns up to three candidate dialogue timelines for one side
// of a pair, best first: the remembered sidecar for this exact release,
// other sidecars already cached for the title, then OpenSubtitles results
// ranked for the release name (or, with no name, closest last cue to the
// known runtime). Only subtitle files are fetched — never video.
func (a *Aligner) timelines(ctx context.Context, bs *buildState, title *core.Title, lang, releaseName string, dur time.Duration) []timeline {
	var out []timeline
	seen := map[int64]bool{}
	add := func(id string, fileID int64, release string) {
		if fileID != 0 && seen[fileID] {
			return
		}
		cues, err := a.cuesByID(id)
		if err != nil || !durationFit(cues, dur) {
			return
		}
		seen[fileID] = true
		out = append(out, timeline{cues: cues, fileID: fileID, release: release})
	}
	if releaseName != "" {
		if sel, ok := a.s.loadSelection(title, lang, releaseName); ok {
			add(sel.ID, sel.FileID, sel.Release)
		}
	}
	// Sidecars already on disk for this title+language, any release.
	if a.s.dir != "" {
		pat := filepath.Join(a.s.dir, "index", fmt.Sprintf("%d.%s.*.json", int64(title.Key), lang))
		if paths, _ := filepath.Glob(pat); len(paths) > 0 {
			sort.Strings(paths)
			for _, p := range paths {
				if len(out) >= 3 {
					break
				}
				raw, err := os.ReadFile(p)
				if err != nil {
					continue
				}
				var sel selection
				if json.Unmarshal(raw, &sel) == nil && sel.ID != "" {
					add(sel.ID, sel.FileID, sel.Release)
				}
			}
		}
	}
	cands, searched := bs.searches[lang]
	if !searched {
		found, err := a.s.Search(ctx, title, lang, releaseName)
		if err != nil {
			a.s.log.Debug("alignment subtitle search failed", "lang", lang, "err", err)
		}
		cands = found
		bs.searches[lang] = cands
	}
	ranked := append([]Candidate(nil), cands...)
	Rank(ranked, releaseName)
	for _, c := range ranked {
		if len(out) >= 3 || ctx.Err() != nil {
			break
		}
		id := cacheID(title, lang, c.FileID)
		if !a.s.cached(id, "srt") {
			if bs.downloads >= alignBuildBudget {
				break
			}
			bs.downloads++
			if _, err := a.s.fetchAndCache(ctx, id, c); err != nil {
				a.s.log.Debug("alignment subtitle download failed", "file", c.FileID, "err", err)
				continue
			}
			release := c.Release
			if release == "" {
				release = c.FileName
			}
			if releaseName != "" {
				a.s.saveSelection(title, lang, releaseName, selection{ID: id, FileID: c.FileID, Language: lang, Release: release, At: a.s.now().UTC()})
			}
		}
		add(id, c.FileID, c.Release)
	}
	if releaseName == "" && dur > 0 && len(out) > 1 {
		sort.SliceStable(out, func(i, j int) bool {
			di, dj := out[i].last()-dur, out[j].last()-dur
			if di < 0 {
				di = -di
			}
			if dj < 0 {
				dj = -dj
			}
			return di < dj
		})
	}
	return out
}

// DialogueTimeline returns the best dialogue timeline for one release
// (cached sidecar first, then OpenSubtitles), without downloading video.
func (s *Service) DialogueTimeline(ctx context.Context, title *core.Title, lang, releaseName string, dur time.Duration) ([]Cue, bool) {
	if !s.Enabled() || title == nil {
		return nil, false
	}
	if lang == "" {
		lang = s.alignLanguages[0]
	}
	bs := &buildState{searches: map[string][]Candidate{}}
	tls := s.TimelineAligner().timelines(ctx, bs, title, langtag.Normalize(lang), releaseName, dur)
	if len(tls) == 0 {
		return nil, false
	}
	return tls[0].cues, true
}

// CuesForInfo returns dialogue cues for the release described by info when
// the system already holds their text: a cached sidecar stream in
// info.Streams. It never fetches video and never touches the network.
// Embedded tracks whose text was never extracted are not available here.
func (s *Service) CuesForInfo(info core.MediaInfo) ([]Cue, bool) {
	for _, st := range info.Streams {
		if st.Type != "subtitle" || !strings.HasPrefix(st.ExternalURL, URLPrefix) {
			continue
		}
		if cues, err := s.TimelineAligner().cuesByID(st.ExternalURL); err == nil && len(cues) > 0 {
			return cues, true
		}
	}
	return nil, false
}
