package resolver

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/braedonsaunders/ether/internal/core"
	"golang.org/x/net/html"
)

// TRaSH Guides publishes its Radarr and Sonarr quality profiles and custom
// formats as JSON in its GitHub repository. Directory listings come from the
// GitHub API (60 unauthenticated requests an hour per IP), files from the raw
// content host, so a listing is cached and reused far more than it is fetched.
const (
	trashRepo = "TRaSH-Guides/Guides"
	trashRef  = "master"

	trashKindProfiles = "quality-profiles"
	trashKindFormats  = "cf"

	trashListLimit    = 2 << 20 // a directory listing
	trashFileLimit    = 1 << 20 // one profile or custom format
	trashMaxEntries   = 2000    // files listed per directory
	trashCacheEntries = 2000
	trashCacheBytes   = 32 << 20
	trashParallel     = 8
)

var (
	// ErrTrashNotFound reports a profile that TRaSH Guides doesn't list.
	ErrTrashNotFound = errors.New("TRaSH Guides has no such profile")
	// ErrTrashUnavailable wraps failures to reach TRaSH Guides.
	ErrTrashUnavailable = errors.New("TRaSH Guides is unavailable")

	trashFileName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._+-]{0,150}\.json$`)
)

// TrashOptions configures a TrashGuides client. Zero values use GitHub.
type TrashOptions struct {
	APIBase   string // default https://api.github.com
	RawBase   string // default https://raw.githubusercontent.com
	Client    *http.Client
	UserAgent string
	TTL       time.Duration // cache lifetime, default 1h
	Now       func() time.Time
	Parallel  int // file fetches in flight, default trashParallel
}

// TrashGuides reads TRaSH Guides profiles and custom formats, caching what
// it fetched in memory for TTL. When a refresh fails, the last good copy is
// served instead.
type TrashGuides struct {
	o  TrashOptions
	mu sync.Mutex
	// dirs: "radarr/cf" -> listed file names. files: "radarr/cf/x.json".
	dirs    map[string]trashCached[[]string]
	files   map[string]trashCached[[]byte]
	bytes   int
	flights map[string]*trashFlight
}

type trashCached[T any] struct {
	v  T
	at time.Time
}

type trashFlight struct {
	done  chan struct{}
	names []string
	err   error
}

// NewTrashGuides returns a client for TRaSH Guides' published JSON.
func NewTrashGuides(o TrashOptions) *TrashGuides {
	o.APIBase = strings.TrimRight(cmpOr(o.APIBase, "https://api.github.com"), "/")
	o.RawBase = strings.TrimRight(cmpOr(o.RawBase, "https://raw.githubusercontent.com"), "/")
	if o.Client == nil {
		o.Client = &http.Client{Timeout: 20 * time.Second}
	}
	o.UserAgent = cmpOr(o.UserAgent, "Ether (+https://github.com/braedonsaunders/ether)")
	if o.TTL <= 0 {
		o.TTL = time.Hour
	}
	if o.Parallel <= 0 {
		o.Parallel = trashParallel
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	return &TrashGuides{o: o, dirs: map[string]trashCached[[]string]{}, files: map[string]trashCached[[]byte]{}, flights: map[string]*trashFlight{}}
}

// TrashProfile summarises one TRaSH quality profile for picking.
type TrashProfile struct {
	ID          string `json:"id"` // file name without .json
	TrashID     string `json:"trashId,omitempty"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"` // plain text
	Group       int    `json:"group,omitempty"`
	URL         string `json:"url,omitempty"` // the guide page, always https://trash-guides.info
	ScoreSet    string `json:"scoreSet,omitempty"`
	Formats     int    `json:"formats"`
}

func validApp(app string) bool { return app == AppRadarr || app == AppSonarr }

// Profiles lists app's TRaSH quality profiles, grouped as the guide groups them.
func (g *TrashGuides) Profiles(ctx context.Context, app string) ([]TrashProfile, error) {
	if !validApp(app) {
		return nil, fmt.Errorf("app must be radarr or sonarr")
	}
	names, err := g.list(ctx, app, trashKindProfiles)
	if err != nil {
		return nil, err
	}
	out := make([]TrashProfile, len(names))
	ok := make([]bool, len(names))
	var firstErr error
	var mu sync.Mutex
	g.eachSliding(ctx, names, func(ctx context.Context, i int, name string) bool {
		data, err := g.file(ctx, app, trashKindProfiles, name)
		if err != nil {
			mu.Lock()
			firstErr = cmp.Or(firstErr, err)
			mu.Unlock()
			return true
		}
		if p, perr := summariseTrashProfile(name, data); perr == nil {
			out[i], ok[i] = p, true
		}
		return true
	})
	var list []TrashProfile
	for i, p := range out {
		if ok[i] {
			list = append(list, p)
		}
	}
	if len(list) == 0 && firstErr != nil {
		return nil, firstErr
	}
	slices.SortFunc(list, func(a, b TrashProfile) int {
		return cmp.Or(cmp.Compare(a.Group, b.Group), strings.Compare(strings.ToLower(a.Name), strings.ToLower(b.Name)))
	})
	return list, nil
}

// Profile returns the JSON of app's TRaSH profile id (a listed file name
// without .json).
func (g *TrashGuides) Profile(ctx context.Context, app, id string) ([]byte, error) {
	if !validApp(app) {
		return nil, fmt.Errorf("app must be radarr or sonarr")
	}
	names, err := g.list(ctx, app, trashKindProfiles)
	if err != nil {
		return nil, err
	}
	if !slices.Contains(names, id+".json") {
		return nil, ErrTrashNotFound
	}
	return g.file(ctx, app, trashKindProfiles, id+".json")
}

// CustomFormats finds the definitions of the trash IDs in want (format name
// -> trash ID) among app's custom formats. File names are slugs that don't
// reliably follow from a format's name, so cached files are searched first,
// then files named like a wanted format, then the rest, fetching at most
// Parallel at a time and stopping once every ID is found. The error
// reports why some IDs could not be looked up; the result holds what was found.
func (g *TrashGuides) CustomFormats(ctx context.Context, app string, want map[string]string) (map[string]core.CustomFormat, error) {
	found := map[string]core.CustomFormat{}
	if len(want) == 0 {
		return found, nil
	}
	if !validApp(app) {
		return found, fmt.Errorf("app must be radarr or sonarr")
	}
	names, err := g.list(ctx, app, trashKindFormats)
	if err != nil {
		return found, err
	}
	remaining := map[string]bool{}
	guess := map[string]bool{}
	for name, id := range want {
		remaining[id] = true
		guess[trashSlug(name)+".json"] = true
	}
	var mu sync.Mutex
	var firstErr error
	take := func(data []byte) {
		var f core.CustomFormat
		if json.Unmarshal(data, &f) != nil || !remaining[f.ID] {
			return
		}
		found[f.ID] = f
		delete(remaining, f.ID)
	}
	var cold, likely []string
	for _, name := range names {
		if data, ok := g.cachedFile(app, trashKindFormats, name); ok {
			take(data)
		} else if guess[name] {
			likely = append(likely, name)
		} else {
			cold = append(cold, name)
		}
	}
	if len(remaining) == 0 {
		return found, nil
	}
	g.eachWave(ctx, append(likely, cold...), func(ctx context.Context, _ int, name string) bool {
		mu.Lock()
		more := len(remaining) > 0
		mu.Unlock()
		if !more {
			return false
		}
		data, err := g.file(ctx, app, trashKindFormats, name)
		mu.Lock()
		defer mu.Unlock()
		if err != nil {
			firstErr = cmp.Or(firstErr, err)
			return true
		}
		take(data)
		return len(remaining) > 0
	})
	if len(remaining) > 0 && firstErr != nil {
		return found, firstErr
	}
	return found, nil
}

// eachWave runs fn over names in waves of at most g.o.Parallel calls,
// starting no further wave once fn returns false or ctx ends. Waves
// protect CustomFormats' early stop: a sliding window would keep
// starting work past the point that asked to stop while its earlier
// calls were still queued, defeating the early stop. Calls in a wave
// run with the parent ctx to completion (wg.Wait), so a fetch
// already paid for is cached by g.file even when another call in the
// wave asked to stop; only starting the next wave is suppressed.
func (g *TrashGuides) eachWave(ctx context.Context, names []string, fn func(context.Context, int, string) bool) {
	parallel := g.o.Parallel
	if parallel <= 0 {
		parallel = trashParallel
	}
	for start := 0; start < len(names); start += parallel {
		if ctx.Err() != nil {
			return
		}
		var wg sync.WaitGroup
		var stop sync.Once
		stopped := make(chan struct{})
		for i, name := range names[start:min(start+parallel, len(names))] {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if !fn(ctx, start+i, name) {
					stop.Do(func() { close(stopped) })
				}
			}()
		}
		wg.Wait()
		if isClosed(stopped) {
			return
		}
	}
}

// eachSliding runs fn over names with at most g.o.Parallel calls in
// flight, starting a new call as soon as one finishes instead of
// waiting for the slowest call of a wave. Profiles' fn never returns
// false, so there is no early stop for a wave barrier to protect and
// the barrier would only leave slots idle behind a slow fetch; it
// still starts no new call once fn returns false or ctx ends.
func (g *TrashGuides) eachSliding(ctx context.Context, names []string, fn func(context.Context, int, string) bool) {
	parallel := g.o.Parallel
	if parallel <= 0 {
		parallel = trashParallel
	}
	sem := make(chan struct{}, parallel)
	var wg sync.WaitGroup
	var stop sync.Once
	stopped := make(chan struct{})
	for i, name := range names {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
		case <-stopped:
		}
		if ctx.Err() != nil || isClosed(stopped) {
			break
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if !fn(ctx, i, name) {
				stop.Do(func() { close(stopped) })
			}
		}()
	}
	wg.Wait()
}

func isClosed(c chan struct{}) bool {
	select {
	case <-c:
		return true
	default:
		return false
	}
}

// list returns the JSON file names in app's kind directory, from cache
// when fresh. Concurrent misses share one GitHub API request.
func (g *TrashGuides) list(ctx context.Context, app, kind string) ([]string, error) {
	key := app + "/" + kind
	g.mu.Lock()
	cached, have := g.dirs[key]
	if have && g.o.Now().Sub(cached.at) < g.o.TTL {
		g.mu.Unlock()
		return cached.v, nil
	}
	f := g.flights[key]
	if f == nil {
		f = &trashFlight{done: make(chan struct{})}
		g.flights[key] = f
		go func() {
			// Detached from any one caller, so a cancelled request does not
			// fail the others waiting on it.
			lctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			f.names, f.err = g.fetchList(lctx, app, kind)
			g.mu.Lock()
			if f.err == nil {
				g.dirs[key] = trashCached[[]string]{v: f.names, at: g.o.Now()}
			}
			delete(g.flights, key)
			g.mu.Unlock()
			close(f.done)
		}()
	}
	g.mu.Unlock()
	select {
	case <-f.done:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if f.err != nil {
		if have {
			return cached.v, nil // stale beats nothing
		}
		return nil, f.err
	}
	return f.names, nil
}

func (g *TrashGuides) fetchList(ctx context.Context, app, kind string) ([]string, error) {
	u := fmt.Sprintf("%s/repos/%s/contents/docs/json/%s/%s?ref=%s", g.o.APIBase, trashRepo, app, kind, trashRef)
	data, err := g.get(ctx, u, trashListLimit, true)
	if err != nil {
		return nil, err
	}
	var entries []struct {
		Name string `json:"name"`
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("%w: unexpected directory listing", ErrTrashUnavailable)
	}
	var names []string
	for _, e := range entries {
		if e.Type == "file" && trashFileName.MatchString(e.Name) {
			names = append(names, e.Name)
		}
		if len(names) == trashMaxEntries {
			break
		}
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("%w: no %s files listed", ErrTrashUnavailable, kind)
	}
	return names, nil
}

func (g *TrashGuides) cachedFile(app, kind, name string) ([]byte, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	c, ok := g.files[app+"/"+kind+"/"+name]
	if !ok || g.o.Now().Sub(c.at) >= g.o.TTL {
		return nil, false
	}
	return c.v, true
}

// file returns one listed file, from cache when fresh.
func (g *TrashGuides) file(ctx context.Context, app, kind, name string) ([]byte, error) {
	if !trashFileName.MatchString(name) {
		return nil, ErrTrashNotFound
	}
	key := app + "/" + kind + "/" + name
	g.mu.Lock()
	cached, have := g.files[key]
	g.mu.Unlock()
	if have && g.o.Now().Sub(cached.at) < g.o.TTL {
		return cached.v, nil
	}
	u := fmt.Sprintf("%s/%s/%s/docs/json/%s/%s/%s", g.o.RawBase, trashRepo, trashRef, app, kind, url.PathEscape(name))
	data, err := g.get(ctx, u, trashFileLimit, false)
	if err != nil {
		if have {
			return cached.v, nil
		}
		return nil, err
	}
	if !json.Valid(data) {
		return nil, fmt.Errorf("%w: %s is not valid JSON", ErrTrashUnavailable, name)
	}
	g.store(key, data)
	return data, nil
}

// store caches a file, evicting the oldest entries beyond the size bounds.
func (g *TrashGuides) store(key string, data []byte) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if old, ok := g.files[key]; ok {
		g.bytes -= len(old.v)
	}
	g.files[key] = trashCached[[]byte]{v: data, at: g.o.Now()}
	g.bytes += len(data)
	for len(g.files) > trashCacheEntries || g.bytes > trashCacheBytes {
		var oldest string
		var at time.Time
		for k, c := range g.files {
			if oldest == "" || c.at.Before(at) {
				oldest, at = k, c.at
			}
		}
		g.bytes -= len(g.files[oldest].v)
		delete(g.files, oldest)
	}
}

func (g *TrashGuides) get(ctx context.Context, u string, limit int64, api bool) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", g.o.UserAgent)
	if api {
		req.Header.Set("Accept", "application/vnd.github+json")
		req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	}
	resp, err := g.o.Client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("%w: could not reach GitHub", ErrTrashUnavailable)
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusOK:
	case resp.StatusCode == http.StatusNotFound:
		return nil, ErrTrashNotFound
	case resp.StatusCode == http.StatusTooManyRequests || (resp.StatusCode == http.StatusForbidden && resp.Header.Get("X-RateLimit-Remaining") == "0"):
		msg := "GitHub's hourly request limit is used up"
		if reset, err := strconv.ParseInt(resp.Header.Get("X-RateLimit-Reset"), 10, 64); err == nil && reset > 0 {
			if wait := time.Until(time.Unix(reset, 0)).Round(time.Minute); wait > 0 {
				msg += fmt.Sprintf("; try again in %d minutes", max(1, int(wait.Minutes())))
			}
		}
		return nil, fmt.Errorf("%w: %s, or paste the profile JSON instead", ErrTrashUnavailable, msg)
	default:
		return nil, fmt.Errorf("%w: GitHub answered HTTP %d", ErrTrashUnavailable, resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, fmt.Errorf("%w: the download was interrupted", ErrTrashUnavailable)
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("%w: response too large", ErrTrashUnavailable)
	}
	return data, nil
}

func summariseTrashProfile(file string, data []byte) (TrashProfile, error) {
	var native struct {
		TrashID     string            `json:"trash_id"`
		Name        string            `json:"name"`
		Description string            `json:"trash_description"`
		URL         string            `json:"trash_url"`
		Group       int               `json:"group"`
		ScoreSet    string            `json:"trash_score_set"`
		FormatItems map[string]string `json:"formatItems"`
	}
	if err := json.Unmarshal(data, &native); err != nil || strings.TrimSpace(native.Name) == "" {
		return TrashProfile{}, fmt.Errorf("not a quality profile")
	}
	p := TrashProfile{ID: strings.TrimSuffix(file, ".json"), TrashID: native.TrashID, Name: strings.TrimSpace(native.Name),
		Description: plainText(native.Description), Group: native.Group, ScoreSet: native.ScoreSet, Formats: len(native.FormatItems)}
	if u, err := url.Parse(native.URL); err == nil && u.Scheme == "https" && (u.Host == "trash-guides.info" || strings.HasSuffix(u.Host, ".trash-guides.info")) {
		p.URL = u.String()
	}
	return p, nil
}

// plainText reduces TRaSH's HTML descriptions to text: tags are dropped,
// line breaks kept, entities decoded and runs of spaces collapsed.
func plainText(s string) string {
	if len(s) > 16<<10 {
		s = s[:16<<10]
	}
	z := html.NewTokenizer(strings.NewReader(s))
	var b strings.Builder
	hidden := 0 // inside <script> or <style>
	for {
		tt := z.Next()
		switch tt {
		case html.ErrorToken:
			var lines []string
			for _, line := range strings.Split(b.String(), "\n") {
				if line = strings.Join(strings.Fields(line), " "); line != "" {
					lines = append(lines, line)
				}
			}
			return strings.Join(lines, "\n")
		case html.TextToken:
			if hidden == 0 {
				b.Write(z.Text())
			}
		case html.StartTagToken, html.SelfClosingTagToken, html.EndTagToken:
			switch name, _ := z.TagName(); string(name) {
			case "br", "p", "li", "div":
				b.WriteByte('\n')
			case "script", "style":
				if tt == html.StartTagToken {
					hidden++
				} else if tt == html.EndTagToken && hidden > 0 {
					hidden--
				}
			}
		}
	}
}

var nonSlug = regexp.MustCompile(`[^a-z0-9]+`)

// trashSlug guesses a custom format's file name from its name the way
// TRaSH usually names them ("Remux Tier 01" -> "remux-tier-01").
func trashSlug(name string) string {
	return strings.Trim(nonSlug.ReplaceAllString(strings.ToLower(name), "-"), "-")
}
