package subtitles

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf16"

	"github.com/braedonsaunders/ether/internal/config"
	"github.com/braedonsaunders/ether/internal/core"
	"github.com/braedonsaunders/ether/internal/langtag"
)

// osFake is a configurable OpenSubtitles fake.
type osFake struct {
	searches, downloads, logins, files atomic.Int32
	mu                                 sync.Mutex
	lastQuery                          map[string]string
	empty                              bool                  // search returns no data
	quota                              bool                  // /download answers 406
	expireToken                        string                // token answered with 401 once
	body                               func(id int64) []byte // file content per file id
	rateLimitOnce                      atomic.Bool
	srv                                *httptest.Server
}

func newOSFake(t *testing.T) *osFake {
	f := &osFake{body: func(int64) []byte { return []byte("1\n00:00:01,000 --> 00:00:02,000\nHallo\n\n") }}
	mux := http.NewServeMux()
	mux.HandleFunc("/subtitles", func(w http.ResponseWriter, r *http.Request) {
		f.searches.Add(1)
		if f.rateLimitOnce.CompareAndSwap(true, false) {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(429)
			return
		}
		q := map[string]string{}
		for k := range r.URL.Query() {
			q[k] = r.URL.Query().Get(k)
		}
		f.mu.Lock()
		f.lastQuery = q
		f.mu.Unlock()
		if f.empty {
			w.Write([]byte(`{"total_count":0,"data":[]}`))
			return
		}
		lang := q["languages"]
		json.NewEncoder(w).Encode(map[string]any{"data": []any{
			map[string]any{"attributes": map[string]any{"language": lang, "download_count": 900, "release": "Show.S01E02.1080p.WEB",
				"files": []any{map[string]any{"file_id": 11, "file_name": "a.srt"}}}},
			map[string]any{"attributes": map[string]any{"language": lang, "download_count": 100, "release": "Other",
				"files": []any{map[string]any{"file_id": 12, "file_name": "b.srt"}}}},
			map[string]any{"attributes": map[string]any{"language": "xx", "download_count": 99999, "release": "Wrong language",
				"files": []any{map[string]any{"file_id": 13, "file_name": "c.srt"}}}},
		}})
	})
	mux.HandleFunc("/login", func(w http.ResponseWriter, r *http.Request) {
		n := f.logins.Add(1)
		var body map[string]string
		json.NewDecoder(r.Body).Decode(&body)
		if body["username"] != "u" || body["password"] != "p" {
			w.WriteHeader(401)
			return
		}
		fmt.Fprintf(w, `{"token":"tok%d"}`, n)
	})
	mux.HandleFunc("/download", func(w http.ResponseWriter, r *http.Request) {
		f.downloads.Add(1)
		f.mu.Lock()
		expire := f.expireToken
		if expire != "" && r.Header.Get("Authorization") == "Bearer "+expire {
			f.expireToken = ""
			f.mu.Unlock()
			w.WriteHeader(401)
			return
		}
		f.mu.Unlock()
		if f.quota {
			w.WriteHeader(406)
			w.Write([]byte(`{"requests":5,"remaining":0,"message":"quota","reset_time_utc":"2099-01-01T00:00:00Z"}`))
			return
		}
		var body struct {
			FileID    int64  `json:"file_id"`
			SubFormat string `json:"sub_format"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		if body.SubFormat != "srt" {
			http.Error(w, "want sub_format", 400)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"link": f.srv.URL + fmt.Sprintf("/file/%d", body.FileID), "remaining": 9})
	})
	mux.HandleFunc("/file/", func(w http.ResponseWriter, r *http.Request) {
		f.files.Add(1)
		var id int64
		fmt.Sscanf(strings.TrimPrefix(r.URL.Path, "/file/"), "%d", &id)
		w.Write(f.body(id))
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

func newSvc(t *testing.T, f *osFake, cfg config.OpenSubtitles, dir string) *Service {
	t.Helper()
	if cfg.APIKey == "" {
		cfg.APIKey = "k"
	}
	cfg.Enabled = true
	return New(Options{Config: cfg, CacheDir: dir, BaseURL: f.srv.URL, MaxRetryAfter: 10 * time.Millisecond})
}

var episode = &core.Title{Key: core.EpisodeKey(95396, 1, 2), Type: core.TypeEpisode, ShowTitle: "Severance", Season: 1, Episode: 2}

func TestSidecarsSelectionIndexAndNegativeCache(t *testing.T) {
	ctx := context.Background()
	f := newOSFake(t)
	dir := t.TempDir()
	svc := newSvc(t, f, config.OpenSubtitles{}, dir)
	user := &core.User{SubtitleLanguages: []string{"de", "ger", "DE"}} // duplicates collapse

	out, err := svc.SidecarsForRelease(ctx, episode, user, nil, "Show.S01E02.1080p.WEB")
	if err != nil || len(out) != 1 || out[0].Language != "de" || out[0].Index != 0 {
		t.Fatalf("first: %+v %v", out, err)
	}
	if !strings.Contains(out[0].Title, "Show.S01E02.1080p.WEB") {
		t.Fatalf("best candidate not chosen: %q", out[0].Title)
	}
	f.mu.Lock()
	q := f.lastQuery
	f.mu.Unlock()
	if q["parent_tmdb_id"] != "95396" || q["season_number"] != "1" || q["episode_number"] != "2" || q["type"] != "episode" || q["languages"] != "de" {
		t.Fatalf("episode query from key: %v", q)
	}

	// A fresh Service over the same dir reuses the remembered selection:
	// no search, no download.
	svc2 := newSvc(t, f, config.OpenSubtitles{}, dir)
	s0, d0 := f.searches.Load(), f.downloads.Load()
	out2, err := svc2.SidecarsForRelease(ctx, episode, user, nil, "Show.S01E02.1080p.WEB")
	if err != nil || len(out2) != 1 || out2[0].ExternalURL != out[0].ExternalURL {
		t.Fatalf("index reuse: %+v %v", out2, err)
	}
	if f.searches.Load() != s0 || f.downloads.Load() != d0 {
		t.Fatal("remembered selection still hit the network")
	}
	// A different release is ranked separately.
	if _, err := svc2.SidecarsForRelease(ctx, episode, user, nil, "Show.S01E02.720p.HDTV"); err != nil {
		t.Fatal(err)
	}
	if f.searches.Load() != s0+1 {
		t.Fatal("different release should search again")
	}

	// Negative cache: nothing found is remembered.
	f.empty = true
	movie := &core.Title{Key: core.MovieKey(5), Type: core.TypeMovie}
	for i := 0; i < 3; i++ {
		if out, err := svc.Sidecars(ctx, movie, user, nil); err != nil || len(out) != 0 {
			t.Fatalf("empty: %+v %v", out, err)
		}
	}
	f.mu.Lock()
	q = f.lastQuery
	f.mu.Unlock()
	if q["tmdb_id"] != "5" || q["type"] != "movie" {
		t.Fatalf("movie query from key: %v", q)
	}
	if n := f.searches.Load(); n != s0+2 {
		t.Fatalf("negative cache: %d searches", n-s0)
	}
}

func TestDownloadQuotaReloginAndRateLimit(t *testing.T) {
	ctx := context.Background()
	f := newOSFake(t)
	svc := newSvc(t, f, config.OpenSubtitles{Username: "u", Password: "p"}, t.TempDir())
	f.rateLimitOnce.Store(true)
	f.expireToken = "tok1"
	user := &core.User{SubtitleLanguages: []string{"fr"}}
	title := &core.Title{Key: core.MovieKey(1), Type: core.TypeMovie, IDs: core.IDs{IMDB: "tt0000001"}}
	out, err := svc.Sidecars(ctx, title, user, nil)
	if err != nil || len(out) != 1 {
		t.Fatalf("relogin path: %+v %v", out, err)
	}
	if f.logins.Load() != 2 {
		t.Fatalf("expired token should trigger one re-login, got %d logins", f.logins.Load())
	}
	if f.searches.Load() != 2 {
		t.Fatalf("429 should be retried once: %d searches", f.searches.Load())
	}

	// Quota: one download attempt, then no more for any language.
	f.quota = true
	d0 := f.downloads.Load()
	user2 := &core.User{SubtitleLanguages: []string{"es", "it", "nl"}}
	out, err = svc.Sidecars(ctx, &core.Title{Key: core.MovieKey(2), Type: core.TypeMovie}, user2, nil)
	if err != nil || len(out) != 0 {
		t.Fatalf("quota: %+v %v", out, err)
	}
	if n := f.downloads.Load() - d0; n != 1 {
		t.Fatalf("quota should stop after first refusal, got %d downloads", n)
	}
	if _, err := svc.requestLink(ctx, Candidate{FileID: 1}); !errors.Is(err, ErrQuota) {
		t.Fatalf("quota remembered until reset: %v", err)
	}
}

func TestLegacyEncodingsAndZip(t *testing.T) {
	ctx := context.Background()
	f := newOSFake(t)
	// Russian in Windows-1251: "Привет" = CF F0 E8 E2 E5 F2.
	cp1251 := append([]byte("1\r\n00:00:01,000 --> 00:00:02,000\r\n"), 0xCF, 0xF0, 0xE8, 0xE2, 0xE5, 0xF2)
	// UTF-16BE without BOM, zipped with a junk file first.
	be := func(s string) []byte {
		var b []byte
		for _, u := range utf16.Encode([]rune(s)) {
			b = append(b, byte(u>>8), byte(u))
		}
		return b
	}
	var zbuf bytes.Buffer
	zw := zip.NewWriter(&zbuf)
	w, _ := zw.Create("README.txt")
	w.Write([]byte("hello"))
	w, _ = zw.Create("Movie.ja.srt")
	w.Write(be("1\n00:00:05,500 --> 00:00:07,000\nこんにちは <font color=\"red\">red</font> & <i>it</i>\n\n"))
	zw.Close()
	f.body = func(id int64) []byte {
		if id == 11 {
			return cp1251
		}
		return zbuf.Bytes()
	}
	svc := newSvc(t, f, config.OpenSubtitles{}, t.TempDir())
	out, err := svc.Sidecars(ctx, &core.Title{Key: core.MovieKey(3), Type: core.TypeMovie}, &core.User{SubtitleLanguages: []string{"ru"}},
		[]core.StreamInfo{{Index: 0, Type: "video"}, {Index: 1, Type: "audio", Language: "rus"}, {Index: 2, Type: "subtitle", Language: "eng"}})
	if err != nil || len(out) != 1 || out[0].Index != 3 {
		t.Fatalf("ru sidecar: %+v %v", out, err)
	}
	rc, _, err := svc.Open(out[0].ExternalURL)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(rc)
	rc.Close()
	if !strings.Contains(string(b), "Привет") {
		t.Fatalf("cp1251 decode: %q", b)
	}

	raw := zbuf.Bytes()
	sub, err := unzipSubtitle(raw)
	if err != nil {
		t.Fatal(err)
	}
	srt, vtt, err := NormalizeLang(sub, "ja")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(srt, "こんにちは") || !strings.Contains(srt, "00:00:05,500 --> 00:00:07,000") {
		t.Fatalf("utf16be srt: %q", srt)
	}
	if strings.Contains(vtt, "<font") || !strings.Contains(vtt, "&amp;") || !strings.Contains(vtt, "<i>it</i>") || !strings.Contains(vtt, "00:00:05.500") {
		t.Fatalf("vtt sanitising: %q", vtt)
	}
}

func TestConcurrentSidecarsSingleFlight(t *testing.T) {
	ctx := context.Background()
	f := newOSFake(t)
	svc := newSvc(t, f, config.OpenSubtitles{}, t.TempDir())
	user := &core.User{SubtitleLanguages: []string{"sv"}}
	title := &core.Title{Key: core.MovieKey(4), Type: core.TypeMovie}
	var wg sync.WaitGroup
	urls := make([]string, 8)
	for i := range urls {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			out, err := svc.Sidecars(ctx, title, user, nil)
			if err == nil && len(out) == 1 {
				urls[i] = out[0].ExternalURL
			}
		}(i)
	}
	wg.Wait()
	for _, u := range urls {
		if u == "" || u != urls[0] {
			t.Fatalf("inconsistent sidecars: %v", urls)
		}
	}
	if n := f.files.Load(); n > 2 {
		t.Fatalf("file fetched %d times; single-flight expected", n)
	}
}

func TestLanguageHelpers(t *testing.T) {
	for in, want := range map[string]string{"eng": "en", "EN-us": "en", "pt_BR": "pt-br", "pob": "pt-br", "por": "pt", "zh-TW": "zh-tw", "chi": "zh", "fre": "fr"} {
		if got := langtag.Normalize(in); got != want {
			t.Errorf("langtag.Normalize(%q)=%q want %q", in, got, want)
		}
	}
	if queryLang("por") != "pt-pt" || queryLang("zh") != "zh-cn" || queryLang("pt-BR") != "pt-br" {
		t.Fatal("queryLang mapping")
	}
	miss := MissingLanguages(&core.User{SubtitleLanguages: []string{"en", "es", "en-GB"}}, []core.StreamInfo{{Type: "audio", Language: "spa"}, {Type: "subtitle", Language: "eng"}})
	if len(miss) != 1 || miss[0] != "es" {
		t.Fatalf("audio tracks must not count as subtitles: %v", miss)
	}
	if MissingLanguages(nil, nil) != nil {
		t.Fatal("nil user")
	}
	svc := New(Options{Config: config.OpenSubtitles{Enabled: true, APIKey: "k"}})
	if svc.Enabled() {
		t.Fatal("no cache dir must disable sidecars")
	}
	if _, _, err := svc.OpenFormat("abc", "ass"); err == nil {
		t.Fatal("unsupported format")
	}
}
