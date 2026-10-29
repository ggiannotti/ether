package subtitles

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/braedonsaunders/ether/internal/config"
	"github.com/braedonsaunders/ether/internal/core"
)

type forcedCand struct {
	id        int64
	downloads int
	hi        bool
	foreign   bool
}

// forcedFake serves configurable hearing_impaired / foreign_parts_only
// candidates; downloaded file ids are recorded.
type forcedFake struct {
	mu         sync.Mutex
	downloaded []int64
	srv        *httptest.Server
}

func newForcedFake(t *testing.T, cands []forcedCand) *forcedFake {
	t.Helper()
	f := &forcedFake{}
	mux := http.NewServeMux()
	mux.HandleFunc("/subtitles", func(w http.ResponseWriter, r *http.Request) {
		lang := r.URL.Query().Get("languages")
		data := []any{}
		for _, c := range cands {
			data = append(data, map[string]any{"attributes": map[string]any{
				"language": lang, "download_count": c.downloads,
				"release":          fmt.Sprintf("Movie.2024.%d", c.id),
				"hearing_impaired": c.hi, "foreign_parts_only": c.foreign,
				"files": []any{map[string]any{"file_id": c.id, "file_name": fmt.Sprintf("f%d.srt", c.id)}},
			}})
		}
		json.NewEncoder(w).Encode(map[string]any{"data": data})
	})
	mux.HandleFunc("/download", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			FileID int64 `json:"file_id"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		f.mu.Lock()
		f.downloaded = append(f.downloaded, body.FileID)
		f.mu.Unlock()
		json.NewEncoder(w).Encode(map[string]any{"link": f.srv.URL + fmt.Sprintf("/file/%d", body.FileID)})
	})
	mux.HandleFunc("/file/", func(w http.ResponseWriter, r *http.Request) {
		var id int64
		fmt.Sscanf(strings.TrimPrefix(r.URL.Path, "/file/"), "%d", &id)
		fmt.Fprintf(w, "1\n00:00:01,000 --> 00:00:02,000\ntrack-%d\n\n", id)
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

func (f *forcedFake) downloads() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int64{}, f.downloaded...)
}

func forcedTitle(key int64) *core.Title {
	return &core.Title{Key: core.MovieKey(key), Type: core.TypeMovie, IDs: core.IDs{IMDB: "tt0000123", TMDB: key}}
}

func openText(t *testing.T, svc *Service, st core.StreamInfo) string {
	t.Helper()
	rc, _, err := svc.Open(st.ExternalURL)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	b, _ := io.ReadAll(rc)
	return string(b)
}

// Mutation: HI+foreign_parts must NOT parse as forced; real forced must.
func TestForcedDefinitionHIPlusForeignIsNotForced(t *testing.T) {
	ctx := context.Background()
	f := newForcedFake(t, []forcedCand{
		{id: 21, downloads: 9000},                          // full
		{id: 22, downloads: 5000, hi: true, foreign: true}, // HI + foreign: SDH, not forced
		{id: 23, downloads: 100, foreign: true},            // real forced
	})
	svc := New(Options{Config: config.OpenSubtitles{Enabled: true, APIKey: "k"}, CacheDir: t.TempDir(), BaseURL: f.srv.URL})
	cands, err := svc.Search(ctx, forcedTitle(42), "en", "")
	if err != nil || len(cands) != 3 {
		t.Fatalf("search: %+v %v", cands, err)
	}
	byID := map[int64]Candidate{}
	for _, c := range cands {
		byID[c.FileID] = c
	}
	if byID[22].Forced || !byID[22].HearingImp {
		t.Fatalf("HI+foreign must be HearingImp, not Forced: %+v", byID[22])
	}
	if !byID[23].Forced || byID[23].HearingImp {
		t.Fatalf("foreign non-HI must be Forced: %+v", byID[23])
	}
	if byID[21].Forced || byID[21].HearingImp {
		t.Fatalf("full track: %+v", byID[21])
	}
}

// Mutation: forced-only auto-fetch downloads the real forced track, never
// a full (or HI+foreign) track; with no real forced candidate it downloads
// nothing.
func TestSidecarsForcedOnlyRequiresRealForced(t *testing.T) {
	ctx := context.Background()
	user := &core.User{SubtitleLanguages: []string{"en"}}
	prefs := Preferences{Mode: "forced-only"}

	f := newForcedFake(t, []forcedCand{
		{id: 21, downloads: 9000},
		{id: 22, downloads: 5000, hi: true, foreign: true},
		{id: 23, downloads: 100, foreign: true},
	})
	svc := New(Options{Config: config.OpenSubtitles{Enabled: true, APIKey: "k"}, CacheDir: t.TempDir(), BaseURL: f.srv.URL})
	out, err := svc.SidecarsForRelease(ctx, forcedTitle(43), user, nil, "", prefs)
	if err != nil || len(out) != 1 || !out[0].Forced {
		t.Fatalf("forced-only sidecar: %+v %v", out, err)
	}
	if got := openText(t, svc, out[0]); !strings.Contains(got, "track-23") {
		t.Fatalf("forced-only fetched wrong track: %q (downloads %v)", got, f.downloads())
	}
	if d := f.downloads(); len(d) != 1 || d[0] != 23 {
		t.Fatalf("forced-only downloads = %v, want only [23]", d)
	}

	// Only full / HI+foreign candidates: nothing is downloaded.
	f2 := newForcedFake(t, []forcedCand{
		{id: 21, downloads: 9000},
		{id: 22, downloads: 5000, hi: true, foreign: true},
	})
	svc2 := New(Options{Config: config.OpenSubtitles{Enabled: true, APIKey: "k"}, CacheDir: t.TempDir(), BaseURL: f2.srv.URL})
	out, err = svc2.SidecarsForRelease(ctx, forcedTitle(44), user, nil, "", prefs)
	if err != nil || len(out) != 0 {
		t.Fatalf("forced-only without forced candidate: %+v %v", out, err)
	}
	if d := f2.downloads(); len(d) != 0 {
		t.Fatalf("forced-only downloaded full/HI tracks: %v", d)
	}
}

// Mutation: style=sdh prefers an SDH candidate over a higher-ranked normal
// one; without the style the normal (best-ranked) track wins.
func TestSidecarsStyleSDHPrefersSDH(t *testing.T) {
	ctx := context.Background()
	user := &core.User{SubtitleLanguages: []string{"en"}}
	cands := []forcedCand{
		{id: 21, downloads: 9000},         // normal, best ranked
		{id: 24, downloads: 50, hi: true}, // SDH
	}

	f := newForcedFake(t, cands)
	svc := New(Options{Config: config.OpenSubtitles{Enabled: true, APIKey: "k"}, CacheDir: t.TempDir(), BaseURL: f.srv.URL})
	out, err := svc.SidecarsForRelease(ctx, forcedTitle(45), user, nil, "", Preferences{Mode: "always", Style: "sdh"})
	if err != nil || len(out) != 1 {
		t.Fatalf("sdh sidecar: %+v %v", out, err)
	}
	if got := openText(t, svc, out[0]); !strings.Contains(got, "track-24") {
		t.Fatalf("style=sdh fetched %q (downloads %v), want SDH track-24", got, f.downloads())
	}
	if !strings.Contains(out[0].Title, "SDH") {
		t.Fatalf("SDH sidecar title should mark SDH for trackprefs: %q", out[0].Title)
	}

	f2 := newForcedFake(t, cands)
	svc2 := New(Options{Config: config.OpenSubtitles{Enabled: true, APIKey: "k"}, CacheDir: t.TempDir(), BaseURL: f2.srv.URL})
	out, err = svc2.SidecarsForRelease(ctx, forcedTitle(46), user, nil, "")
	if err != nil || len(out) != 1 {
		t.Fatalf("default sidecar: %+v %v", out, err)
	}
	if got := openText(t, svc2, out[0]); !strings.Contains(got, "track-21") {
		t.Fatalf("default fetched %q, want best-ranked track-21", got)
	}
}

// Owner repro (PR #47): a full track cached by a normal fetch must NOT
// answer a later forced-only request for the same title/release — the
// forced track (file 23) must be searched and downloaded.
func TestForcedOnlyAfterFullCachedDownloadsForced(t *testing.T) {
	ctx := context.Background()
	user := &core.User{SubtitleLanguages: []string{"en"}}
	const release = "Movie.2024.1080p.WEB-DL"
	f := newForcedFake(t, []forcedCand{
		{id: 21, downloads: 9000}, // full, best ranked
		{id: 22, downloads: 5000, hi: true, foreign: true},
		{id: 23, downloads: 100, foreign: true}, // real forced
	})
	svc := New(Options{Config: config.OpenSubtitles{Enabled: true, APIKey: "k"}, CacheDir: t.TempDir(), BaseURL: f.srv.URL})
	title := forcedTitle(47)

	out, err := svc.SidecarsForRelease(ctx, title, user, nil, release)
	if err != nil || len(out) != 1 || out[0].Forced {
		t.Fatalf("normal sidecar: %+v %v", out, err)
	}
	if got := openText(t, svc, out[0]); !strings.Contains(got, "track-21") {
		t.Fatalf("normal fetched %q, want full track-21", got)
	}
	if d := f.downloads(); len(d) != 1 || d[0] != 21 {
		t.Fatalf("downloads after normal = %v, want [21]", d)
	}

	out, err = svc.SidecarsForRelease(ctx, title, user, nil, release, Preferences{Mode: "forced-only"})
	if err != nil || len(out) != 1 || !out[0].Forced {
		t.Fatalf("forced-only after full cached: %+v %v", out, err)
	}
	if got := openText(t, svc, out[0]); !strings.Contains(got, "track-23") {
		t.Fatalf("forced-only served %q, want forced track-23 (downloads %v)", got, f.downloads())
	}
	if d := f.downloads(); len(d) != 2 || d[1] != 23 {
		t.Fatalf("downloads = %v, want [21 23]", d)
	}
}

// Two viewers with different preferences on the same title/release each
// get their own track, and one viewer's negative result does not block
// another preference.
func TestMultiPreferenceViewersSameTitle(t *testing.T) {
	ctx := context.Background()
	user := &core.User{SubtitleLanguages: []string{"en"}}
	const release = "Movie.2024.1080p.WEB-DL"

	f := newForcedFake(t, []forcedCand{
		{id: 21, downloads: 9000},               // full
		{id: 23, downloads: 100, foreign: true}, // forced
		{id: 24, downloads: 50, hi: true},       // SDH
	})
	svc := New(Options{Config: config.OpenSubtitles{Enabled: true, APIKey: "k"}, CacheDir: t.TempDir(), BaseURL: f.srv.URL})
	title := forcedTitle(48)

	normal, err := svc.SidecarsForRelease(ctx, title, user, nil, release)
	if err != nil || len(normal) != 1 || normal[0].Forced {
		t.Fatalf("normal viewer: %+v %v", normal, err)
	}
	if got := openText(t, svc, normal[0]); !strings.Contains(got, "track-21") {
		t.Fatalf("normal viewer got %q, want track-21", got)
	}
	forced, err := svc.SidecarsForRelease(ctx, title, user, nil, release, Preferences{Mode: "forced-only"})
	if err != nil || len(forced) != 1 || !forced[0].Forced {
		t.Fatalf("forced viewer: %+v %v", forced, err)
	}
	if got := openText(t, svc, forced[0]); !strings.Contains(got, "track-23") {
		t.Fatalf("forced viewer got %q, want track-23", got)
	}
	sdh, err := svc.SidecarsForRelease(ctx, title, user, nil, release, Preferences{Mode: "always", Style: "sdh"})
	if err != nil || len(sdh) != 1 {
		t.Fatalf("sdh viewer: %+v %v", sdh, err)
	}
	if got := openText(t, svc, sdh[0]); !strings.Contains(got, "track-24") {
		t.Fatalf("sdh viewer got %q, want track-24 (downloads %v)", got, f.downloads())
	}
	if !strings.Contains(sdh[0].Title, "SDH") {
		t.Fatalf("sdh sidecar title should mark SDH: %q", sdh[0].Title)
	}

	// Negative isolation: forced-only finds nothing (only a full track
	// exists), then a normal request for the same title/release must
	// still search and download the full track.
	f2 := newForcedFake(t, []forcedCand{{id: 21, downloads: 9000}})
	svc2 := New(Options{Config: config.OpenSubtitles{Enabled: true, APIKey: "k"}, CacheDir: t.TempDir(), BaseURL: f2.srv.URL})
	title2 := forcedTitle(49)
	out, err := svc2.SidecarsForRelease(ctx, title2, user, nil, release, Preferences{Mode: "forced-only"})
	if err != nil || len(out) != 0 {
		t.Fatalf("forced-only without forced candidate: %+v %v", out, err)
	}
	out, err = svc2.SidecarsForRelease(ctx, title2, user, nil, release)
	if err != nil || len(out) != 1 {
		t.Fatalf("normal after forced-only negative: %+v %v", out, err)
	}
	if got := openText(t, svc2, out[0]); !strings.Contains(got, "track-21") {
		t.Fatalf("normal after negative got %q, want track-21 (negative of another prefs blocked it?)", got)
	}
}
