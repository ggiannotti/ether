package newznab

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/braedonsaunders/ether/internal/config"
	"github.com/braedonsaunders/ether/internal/core"
	"github.com/braedonsaunders/ether/internal/releaseparse"
)

const (
	defaultMaxNZBBytes = 50 << 20
	defaultSearchLimit = 100
	defaultHTTPTimeout = 30 * time.Second
	defaultCacheTTL    = 6 * time.Hour
	// capsRetry spaces caps requests after a failure. Every caps request
	// spends a daily API hit, and searches work (ungated) without caps.
	capsRetry = time.Hour
)

var errCapsUnavailable = errors.New("newznab: capabilities unavailable; retrying later")

// redactURLError drops the request URL from a *url.Error: indexer URLs
// carry the API key, and callers log and surface these errors.
func redactURLError(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return fmt.Errorf("newznab: %s request: %w", ue.Op, ue.Err)
	}
	return err
}

// Options tunes a Client or Manager. Zero values get sensible defaults.
type Options struct {
	HTTPClient     *http.Client
	Logger         *slog.Logger
	SearchCacheTTL time.Duration // <0 disables cache, 0 = default 6h
	NZBDir         string        // directory for downloaded NZBs; "" keeps them in memory only
	MaxNZBBytes    int64
	Now            func() time.Time // injected clock (tests)
	MaxParallel    int              // Manager fan-out; 0 dispatches every configured indexer
	// Backend builds the search backend for an indexer whose Kind is not
	// Newznab (built-in sites, internal/cardigann). Nil disables them.
	Backend func(config.Indexer) (Backend, error)
}

// Backend searches a site that is not a Newznab API, such as a built-in
// site run from an owner-installed definition. The Client keeps budgets,
// the result cache and events around it: a backend calls reserve with
// "hit" or "grab" before each request that counts against a daily budget.
type Backend interface {
	Search(ctx context.Context, req core.SearchRequest, reserve func(context.Context, string) error) ([]core.Release, error)
	FetchNZB(ctx context.Context, r core.Release, reserve func(context.Context, string) error) ([]byte, error)
	ResolveTorrent(ctx context.Context, r core.Release, reserve func(context.Context, string) error) (core.Release, error)
}

// Client is one configured Newznab indexer. It implements core.Indexer.
type Client struct {
	cfg      config.Indexer
	db       *sql.DB
	http     *http.Client
	log      *slog.Logger
	cacheTTL time.Duration
	nzbDir   string
	maxNZB   int64
	now      func() time.Time
	backends func(config.Indexer) (Backend, error)

	feedMu   sync.Mutex
	feedMode recentFeedMode
	budgetMu sync.Mutex
	mem      memBudget
	rate     rateLimitState

	cacheMu  sync.RWMutex
	memCache map[string]memCacheEntry
	torrents torrentDescriptorCache

	capsMu     sync.Mutex
	caps       *Caps
	capsAt     time.Time
	capsEP     string    // capsEndpoint the cached caps came from
	capsFailAt time.Time // last failed caps request; searches skip caps for capsRetry

	cfgMu sync.RWMutex
}

var _ core.Indexer = (*Client)(nil)

// New builds a Client for one indexer. db may be nil (budgets and cache
// then live in memory).
func New(cfg config.Indexer, db *sql.DB, opts Options) *Client {
	c := &Client{cfg: cfg, db: db, nzbDir: opts.NZBDir, backends: opts.Backend}
	c.http = opts.HTTPClient
	if c.http == nil {
		c.http = &http.Client{Timeout: defaultHTTPTimeout}
	}
	c.log = opts.Logger
	if c.log == nil {
		c.log = slog.Default()
	}
	c.now = opts.Now
	if c.now == nil {
		c.now = time.Now
	}
	switch {
	case opts.SearchCacheTTL < 0:
		c.cacheTTL = 0
	case opts.SearchCacheTTL == 0:
		c.cacheTTL = defaultCacheTTL
	default:
		c.cacheTTL = opts.SearchCacheTTL
	}
	c.maxNZB = opts.MaxNZBBytes
	if c.maxNZB <= 0 {
		c.maxNZB = defaultMaxNZBBytes
	}
	return c
}

// NewClient is an alias for New.
func NewClient(cfg config.Indexer, db *sql.DB, opts Options) *Client { return New(cfg, db, opts) }

// Config returns the indexer's current config.
func (c *Client) Config() config.Indexer {
	c.cfgMu.RLock()
	defer c.cfgMu.RUnlock()
	return c.cfg
}

// UpdateConfig swaps the indexer config (called on config reload).
func (c *Client) UpdateConfig(cfg config.Indexer) {
	c.cfgMu.Lock()
	changed := c.cfg.URL != cfg.URL || c.cfg.APIKey != cfg.APIKey
	c.cfg = cfg
	c.cfgMu.Unlock()
	if changed {
		// A corrected URL or key deserves a fresh caps attempt, and the
		// old endpoint's caps no longer describe this indexer.
		c.capsMu.Lock()
		c.capsFailAt = time.Time{}
		c.caps, c.capsAt, c.capsEP = nil, time.Time{}, ""
		c.capsMu.Unlock()
	}
}

// SetCacheTTL changes the result-cache TTL (called on config reload).
// A negative value disables the cache.
func (c *Client) SetCacheTTL(d time.Duration) {
	c.cfgMu.Lock()
	defer c.cfgMu.Unlock()
	if d < 0 {
		c.cacheTTL = 0
	} else {
		c.cacheTTL = d
	}
}

// Name implements core.Indexer.
func (c *Client) Name() string {
	cfg := c.Config()
	if cfg.Name != "" {
		return cfg.Name
	}
	if cfg.ID != "" {
		return cfg.ID
	}
	if u, err := url.Parse(cfg.URL); err == nil && u.Host != "" {
		return u.Host
	}
	return cfg.URL
}

func (c *Client) apiURL(params url.Values) (string, error) {
	cfg := c.Config()
	base := strings.TrimRight(cfg.URL, "/")
	if base == "" {
		return "", fmt.Errorf("newznab: indexer %q has no URL", c.Name())
	}
	if !strings.HasSuffix(base, "/api") {
		base += "/api"
	}
	q := url.Values{}
	for k, vs := range params {
		q[k] = vs
	}
	if cfg.APIKey != "" && q.Get("apikey") == "" {
		q.Set("apikey", cfg.APIKey)
	}
	return base + "?" + q.Encode(), nil
}

// get performs a GET and returns the body, mapping HTTP failures to
// typed API errors. Responses are size-limited.
func (c *Client) get(ctx context.Context, rawURL string, limit int64) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, redactURLError(err)
	}
	req.Header.Set("User-Agent", "Ether/1.0")
	resp, err := c.http.Do(req)
	if err != nil {
		// url.Error embeds the request URL, which carries the indexer API
		// key; callers log and surface this error.
		var ue *url.Error
		if errors.As(err, &ue) {
			return nil, fmt.Errorf("newznab: request to %q failed: %w", c.Name(), ue.Err)
		}
		return nil, err
	}
	defer resp.Body.Close()
	// Record throttling before reading the body: even a broken error response
	// must not cause every new play request to hammer a throttled indexer.
	if resp.StatusCode == http.StatusTooManyRequests {
		return nil, c.recordRateLimit(ctx, resp.Header.Get("Retry-After"), apiError(429, "too many requests"))
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, fmt.Errorf("newznab: response exceeds %d bytes", limit)
	}
	// Newznab quota errors can arrive with HTTP 200, including NZB grabs.
	// Detect these here so caps, searches and downloads share the same backoff.
	if bytes.Contains(bytes.ToLower(body[:min(len(body), 4096)]), []byte("error")) {
		if _, perr := parseFeed(body, c.Name()); perr != nil {
			var ae *APIError
			if errors.As(perr, &ae) {
				if ae.Budget {
					return nil, c.recordRateLimit(ctx, resp.Header.Get("Retry-After"), perr)
				}
				return nil, perr
			}
		}
	}
	if resp.StatusCode != http.StatusOK {
		// Indexers often still return a Newznab error document.
		if _, perr := parseFeed(body, c.Name()); perr != nil {
			var ae *APIError
			if errors.As(perr, &ae) {
				return nil, perr
			}
		}
		return nil, apiError(resp.StatusCode, resp.Status)
	}
	return body, nil
}

// searchIdentity fingerprints every indexer setting that changes what a
// search returns.
func searchIdentity(cfg config.Indexer) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s\x00%s\x00%s\x00%s\x00%s\x00%v\x00%v", cfg.URL, cfg.APIKey, cfg.Protocol, cfg.Kind, cfg.Definition, cfg.MovieCategories, cfg.TVCategories)
	for _, m := range []map[string]string{cfg.Settings, cfg.SecretSettings} {
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(h, "\x00%s=%s", k, m[k])
		}
		h.Write([]byte{1})
	}
	return hex.EncodeToString(h.Sum(nil)[:12])
}

// capsEndpoint fingerprints the URL and API key caps are fetched from.
func (c *Client) capsEndpoint() string {
	cfg := c.Config()
	sum := sha256.Sum256([]byte(cfg.URL + "\x00" + cfg.APIKey))
	return hex.EncodeToString(sum[:12])
}

// Caps returns the indexer's capabilities, cached for 24h per endpoint
// (URL and API key): an indexer edited to point elsewhere fetches anew,
// and a response from the old endpoint still in flight is not kept.
func (c *Client) Caps(ctx context.Context) (*Caps, error) {
	ep := c.capsEndpoint()
	c.capsMu.Lock()
	if c.caps != nil && c.capsEP == ep && c.now().Sub(c.capsAt) < 24*time.Hour {
		defer c.capsMu.Unlock()
		return c.caps, nil
	}
	if !c.capsFailAt.IsZero() && c.now().Sub(c.capsFailAt) < capsRetry {
		c.capsMu.Unlock()
		return nil, errCapsUnavailable
	}
	c.capsMu.Unlock()
	if caps, ok := c.capsFromDB(ctx, ep); ok {
		c.capsMu.Lock()
		if c.capsEndpoint() == ep {
			c.caps, c.capsAt, c.capsEP = caps, c.now(), ep
		}
		c.capsMu.Unlock()
		return caps, nil
	}
	if err := c.reserve(ctx, "hit"); err != nil {
		return nil, err
	}
	caps, err := c.fetchCaps(ctx)
	c.capsMu.Lock()
	current := c.capsEndpoint() == ep // the config did not change meanwhile
	if err != nil {
		if current && ctx.Err() == nil && !errors.Is(err, core.ErrBudgetExhausted) {
			c.capsFailAt = c.now()
		}
		c.capsMu.Unlock()
		return nil, err
	}
	if current {
		c.caps, c.capsAt, c.capsEP, c.capsFailAt = caps, c.now(), ep, time.Time{}
	}
	c.capsMu.Unlock()
	if current {
		c.capsToDB(ctx, caps, ep)
	}
	return caps, nil
}

func (c *Client) fetchCaps(ctx context.Context) (*Caps, error) {
	u, err := c.apiURL(url.Values{"t": {"caps"}})
	if err != nil {
		return nil, err
	}
	body, err := c.get(ctx, u, 5<<20)
	if err != nil {
		return nil, err
	}
	if trimmed := bytes.TrimSpace(body); len(trimmed) > 0 && trimmed[0] != '{' {
		// Error documents reach here with HTTP 200.
		if _, perr := parseFeed(body, c.Name()); perr != nil {
			return nil, perr
		}
	}
	caps, err := parseCaps(body)
	if err != nil {
		return nil, fmt.Errorf("newznab: parse caps: %w", err)
	}
	return caps, nil
}

func cacheKey(c *Client, req core.SearchRequest) string {
	h := sha1.New()
	// Cached releases include parsed audio-language facts. Earlier names
	// with regional tags were persisted as unknown language; refresh them
	// instead of applying corrected ranking only after their TTL expires.
	// v2: title boundaries and year-like titles parse correctly.
	// v3: cam variants, groups, UHD-sourced 1080p, title words before
	// the episode marker and remaster years parse correctly.
	fmt.Fprint(h, "release-parse-v3|")
	if c.Config().IsBuiltin() && (req.Type == core.TypeEpisode || req.Type == core.TypeShow || req.Season > 0) {
		// Built-in TV keywords no longer require the series premiere year.
		// Do not reuse persisted empty answers from that narrower search.
		fmt.Fprint(h, "tv-keywords-v2|")
	}
	// The indexer's search identity: an edited URL, key, category list or
	// built-in site setting must not be answered from the old config's
	// results (in memory, or persisted for a fresh client).
	fmt.Fprintf(h, "%s|", searchIdentity(c.Config()))
	fmt.Fprintf(h, "%s|%s|%d|%s|%d|%s|%d|%d|%d",
		c.indexerID(), req.Type, req.IDs.TMDB, req.IDs.IMDB, req.IDs.TVDB,
		strings.ToLower(strings.TrimSpace(req.Title)), req.Year, req.Season, req.Episode)
	if orig := strings.ToLower(strings.TrimSpace(req.OriginalTitle)); orig != "" && orig != strings.ToLower(strings.TrimSpace(req.Title)) {
		fmt.Fprintf(h, "|%s", orig)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func joinCats(cats []int) string {
	parts := make([]string, len(cats))
	for i, n := range cats {
		parts[i] = strconv.Itoa(n)
	}
	return strings.Join(parts, ",")
}

// queryTitles returns the distinct search titles to try for req: Title
// first, then OriginalTitle (when distinct), then structural aliases.
func queryTitles(req core.SearchRequest) []string {
	var out []string
	seen := map[string]bool{}
	add := func(s string) {
		s = strings.TrimSpace(s)
		if s == "" {
			return
		}
		k := strings.ToLower(s)
		if seen[k] {
			return
		}
		seen[k] = true
		out = append(out, s)
	}
	add(req.Title)
	if req.OriginalTitle != "" && !releaseparse.TitlesMatch(req.OriginalTitle, req.Title) {
		add(req.OriginalTitle)
	}
	for _, a := range releaseparse.TitleAliases(req.Title) {
		add(a)
	}
	if req.OriginalTitle != "" {
		for _, a := range releaseparse.TitleAliases(req.OriginalTitle) {
			add(a)
		}
	}
	return out
}

// searchAttempts builds the ordered fallback queries for a request.
func (c *Client) searchAttempts(req core.SearchRequest, caps *Caps) []url.Values {
	cfg := c.Config()
	isTV := req.Type == core.TypeEpisode || req.Season > 0 || (req.Type == core.TypeShow)
	base := func() url.Values {
		v := url.Values{"extended": {"1"}, "limit": {strconv.Itoa(defaultSearchLimit)}}
		if isTV {
			cats := cfg.TVCategories
			if len(cats) == 0 {
				cats = []int{5000}
			}
			v.Set("cat", joinCats(cats))
		} else {
			cats := cfg.MovieCategories
			if len(cats) == 0 {
				cats = []int{2000}
			}
			v.Set("cat", joinCats(cats))
		}
		return v
	}
	imdb := strings.TrimPrefix(req.IDs.IMDB, "tt")
	titles := queryTitles(req)
	var attempts []url.Values
	if isTV {
		mk := func(idKey, idVal string) url.Values {
			v := base()
			v.Set("t", "tvsearch")
			if idKey != "" {
				v.Set(idKey, idVal)
			}
			if req.Season > 0 {
				v.Set("season", strconv.Itoa(req.Season))
			}
			if req.Episode > 0 {
				v.Set("ep", strconv.Itoa(req.Episode))
			}
			return v
		}
		if req.IDs.TVDB > 0 && (caps == nil || (!caps.TVAvail || hasParam(caps.TVParams, "tvdbid"))) {
			attempts = append(attempts, mk("tvdbid", strconv.FormatInt(req.IDs.TVDB, 10)))
		}
		if req.IDs.TMDB > 0 && (caps == nil || (!caps.TVAvail || hasParam(caps.TVParams, "tmdbid"))) {
			attempts = append(attempts, mk("tmdbid", strconv.FormatInt(req.IDs.TMDB, 10)))
		}
		if imdb != "" && (caps == nil || (!caps.TVAvail || hasParam(caps.TVParams, "imdbid"))) {
			attempts = append(attempts, mk("imdbid", imdb))
		}
		for _, title := range titles {
			q := title
			switch {
			case req.Season > 0 && req.Episode > 0:
				q = fmt.Sprintf("%s S%02dE%02d", title, req.Season, req.Episode)
			case req.Season > 0:
				q = fmt.Sprintf("%s S%02d", title, req.Season)
			}
			v := mk("", "")
			v.Set("q", q)
			attempts = append(attempts, v)
			v2 := base()
			v2.Set("t", "search")
			v2.Set("q", q)
			attempts = append(attempts, v2)
		}
		// Torznab season-pack and complete-series fallback: older TV shows
		// on torrent indexers often exist only as full-season or complete
		// packs, which an episode-specific query (ep=N / SxxExx) misses.
		if cfg.IsTorrent() && req.Season > 0 {
			mkSeason := func(idKey, idVal string) url.Values {
				v := base()
				v.Set("t", "tvsearch")
				if idKey != "" {
					v.Set(idKey, idVal)
				}
				v.Set("season", strconv.Itoa(req.Season))
				return v
			}
			if req.Episode > 0 {
				if req.IDs.TVDB > 0 && (caps == nil || (!caps.TVAvail || hasParam(caps.TVParams, "tvdbid"))) {
					attempts = append(attempts, mkSeason("tvdbid", strconv.FormatInt(req.IDs.TVDB, 10)))
				}
				if req.IDs.TMDB > 0 && (caps == nil || (!caps.TVAvail || hasParam(caps.TVParams, "tmdbid"))) {
					attempts = append(attempts, mkSeason("tmdbid", strconv.FormatInt(req.IDs.TMDB, 10)))
				}
				if imdb != "" && (caps == nil || (!caps.TVAvail || hasParam(caps.TVParams, "imdbid"))) {
					attempts = append(attempts, mkSeason("imdbid", imdb))
				}
				for _, title := range titles {
					q := fmt.Sprintf("%s S%02d", title, req.Season)
					v := mkSeason("", "")
					v.Set("q", q)
					attempts = append(attempts, v)
					v2 := base()
					v2.Set("t", "search")
					v2.Set("q", q)
					attempts = append(attempts, v2)
				}
			}
			for _, title := range titles {
				v := base()
				v.Set("t", "search")
				v.Set("q", fmt.Sprintf("%s Season %d", title, req.Season))
				attempts = append(attempts, v)
				vc := base()
				vc.Set("t", "search")
				vc.Set("q", title+" Complete")
				attempts = append(attempts, vc)
			}
		}
		return attempts
	}
	if imdb != "" && (caps == nil || (!caps.MovieAvail || hasParam(caps.MovieParams, "imdbid"))) {
		v := base()
		v.Set("t", "movie")
		v.Set("imdbid", imdb)
		attempts = append(attempts, v)
	}
	if req.IDs.TMDB > 0 && (caps == nil || (!caps.MovieAvail || hasParam(caps.MovieParams, "tmdbid"))) {
		v := base()
		v.Set("t", "movie")
		v.Set("tmdbid", strconv.FormatInt(req.IDs.TMDB, 10))
		attempts = append(attempts, v)
	}
	for _, title := range titles {
		q := title
		if req.Year > 0 {
			q = fmt.Sprintf("%s %d", title, req.Year)
		}
		v := base()
		v.Set("t", "search")
		v.Set("q", q)
		attempts = append(attempts, v)
	}
	return attempts
}

func (c *Client) doSearch(ctx context.Context, params url.Values) ([]core.Release, error) {
	if err := c.reserve(ctx, "hit"); err != nil {
		return nil, err
	}
	u, err := c.apiURL(params)
	if err != nil {
		return nil, err
	}
	var results []core.Release
	if emit, ok := ctx.Value(searchArrivalKey{}).(func([]core.Release)); ok {
		results, err = c.getSearchStream(ctx, u, emit)
	} else {
		var body []byte
		body, err = c.get(ctx, u, 20<<20)
		if err == nil {
			results, err = parseFeed(body, c.Name())
		}
	}
	if err == nil && c.Config().IsTorrent() {
		// A Torznab indexer's results are torrents even when an item
		// carries only a .torrent download link.
		for i := range results {
			results[i].Protocol = core.ProtocolTorrent
		}
	}
	return results, err
}

// Search implements core.Indexer.
func (c *Client) Search(ctx context.Context, req core.SearchRequest) ([]core.Release, error) {
	if cfg := c.Config(); !cfg.Enabled {
		return nil, fmt.Errorf("indexer %q is disabled", c.Name())
	}
	key := cacheKey(c, req)
	if cached, ok := c.cacheGet(ctx, key); ok {
		return cached, nil
	}
	// Exhausted limits prohibit new requests, not reuse of already fetched
	// results and NZBs. The manager must reach this cache before this check.
	if c.Exhausted(ctx) {
		return nil, fmt.Errorf("indexer %q: %w", c.Name(), core.ErrBudgetExhausted)
	}
	if c.Config().UsesBackend() {
		results, err := c.backendSearch(ctx, req)
		if err != nil {
			return nil, err
		}
		c.cachePut(ctx, key, results)
		return results, nil
	}
	caps, _ := c.Caps(ctx) // best effort; failures just mean no param gating
	attempts := c.searchAttempts(req, caps)
	if len(attempts) == 0 {
		return nil, fmt.Errorf("newznab: search request has no usable identifiers")
	}
	var firstErr error
	results, err := c.doSearch(ctx, attempts[0])
	if err != nil {
		if errors.Is(err, core.ErrBudgetExhausted) {
			return nil, err
		}
		firstErr = err
		c.log.Debug("newznab search attempt failed", "indexer", c.Name(), "t", attempts[0].Get("t"), "err", err)
	} else if len(results) > 0 {
		c.cachePut(ctx, key, results)
		return results, nil
	}
	rest := attempts[1:]
	if len(rest) == 0 {
		if firstErr != nil {
			return nil, firstErr
		}
		c.cachePut(ctx, key, nil)
		return nil, nil
	}
	// When the primary attempt misses on an uncapped indexer, fan out the
	// fallback queries (alternate IDs, keyword search, OriginalTitle/aliases,
	// season packs) in bounded parallel bursts so obscure titles resolve in
	// a single round-trip instead of N serial timeouts.
	if c.Config().DailyLimit == 0 && firstErr == nil {
		const burstWidth = 4
		for base := 0; base < len(rest); base += burstWidth {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			end := min(base+burstWidth, len(rest))
			batch := rest[base:end]
			type outSlot struct {
				res []core.Release
				err error
			}
			slots := make([]outSlot, len(batch))
			var wg sync.WaitGroup
			for i, p := range batch {
				wg.Add(1)
				go func(i int, p url.Values) {
					defer wg.Done()
					slots[i].res, slots[i].err = c.doSearch(ctx, p)
				}(i, p)
			}
			wg.Wait()
			var merged []core.Release
			seen := map[string]bool{}
			var budgetErr error
			for _, s := range slots {
				if s.err != nil {
					if errors.Is(s.err, core.ErrBudgetExhausted) && budgetErr == nil {
						budgetErr = s.err
					} else if firstErr == nil {
						firstErr = s.err
					}
					continue
				}
				for _, r := range s.res {
					k := r.ID
					if r.InfoHash != "" {
						k = r.InfoHash
					}
					if !seen[k] {
						seen[k] = true
						merged = append(merged, r)
					}
				}
			}
			if len(merged) > 0 {
				c.cachePut(ctx, key, merged)
				return merged, nil
			}
			if budgetErr != nil {
				return nil, budgetErr
			}
		}
	} else {
		for _, params := range rest {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			results, err := c.doSearch(ctx, params)
			if err != nil {
				if errors.Is(err, core.ErrBudgetExhausted) {
					return nil, err
				}
				if firstErr == nil {
					firstErr = err
				}
				c.log.Debug("newznab search attempt failed", "indexer", c.Name(), "t", params.Get("t"), "err", err)
				continue
			}
			if len(results) > 0 {
				c.cachePut(ctx, key, results)
				return results, nil
			}
		}
	}
	if firstErr != nil {
		return nil, firstErr
	}
	c.cachePut(ctx, key, nil)
	return nil, nil
}

// rescueAttempts builds broader keyword, alias, season-pack and miniseries
// queries for a second-pass rescue when the primary search returned only
// dead/unhealthy or non-matching releases.
func (c *Client) rescueAttempts(req core.SearchRequest, caps *Caps) []url.Values {
	cfg := c.Config()
	isTV := req.Type == core.TypeEpisode || req.Season > 0 || req.Type == core.TypeShow
	base := func() url.Values {
		v := url.Values{"extended": {"1"}, "limit": {strconv.Itoa(defaultSearchLimit)}}
		if isTV {
			cats := cfg.TVCategories
			if len(cats) == 0 {
				cats = []int{5000}
			}
			v.Set("cat", joinCats(cats))
		} else {
			cats := cfg.MovieCategories
			if len(cats) == 0 {
				cats = []int{2000}
			}
			v.Set("cat", joinCats(cats))
		}
		return v
	}
	primary := c.searchAttempts(req, caps)
	alreadyFirst := ""
	if len(primary) > 0 {
		alreadyFirst = primary[0].Encode()
	}
	seen := map[string]bool{}
	if alreadyFirst != "" {
		seen[alreadyFirst] = true
	}
	var out []url.Values
	add := func(v url.Values) {
		enc := v.Encode()
		if enc == "" || seen[enc] {
			return
		}
		seen[enc] = true
		out = append(out, v)
	}
	for _, p := range primary[min(1, len(primary)):] {
		add(p)
	}
	titles := queryTitles(req)
	if isTV {
		for _, title := range titles {
			if req.Season > 0 && req.Episode > 0 {
				// Season pack queries (S01 / Season 1 / Complete).
				vS := base()
				vS.Set("t", "search")
				vS.Set("q", fmt.Sprintf("%s S%02d", title, req.Season))
				add(vS)
				vSeason := base()
				vSeason.Set("t", "search")
				vSeason.Set("q", fmt.Sprintf("%s Season %d", title, req.Season))
				add(vSeason)
				if req.Season == 1 && req.Episode <= 20 {
					vPart := base()
					vPart.Set("t", "search")
					vPart.Set("q", fmt.Sprintf("%s Part %d", title, req.Episode))
					add(vPart)
					vEp := base()
					vEp.Set("t", "search")
					vEp.Set("q", fmt.Sprintf("%s E%02d", title, req.Episode))
					add(vEp)
				}
			}
			vComp := base()
			vComp.Set("t", "search")
			vComp.Set("q", title+" Complete")
			add(vComp)
		}
		return out
	}
	// Movies: also try bare title without year for uploads whose indexed
	// release name omitted the year or used a festival release year.
	for _, title := range titles {
		v := base()
		v.Set("t", "search")
		v.Set("q", title)
		add(v)
	}
	return out
}

// SearchRescue runs broader keyword, alias, OriginalTitle, miniseries and
// season-pack queries when the primary Search results had no playable
// candidate. Results are cached under a separate rescue key and respect
// the indexer's daily budget and rate-limit state.
func (c *Client) SearchRescue(ctx context.Context, req core.SearchRequest) ([]core.Release, error) {
	if cfg := c.Config(); !cfg.Enabled {
		return nil, fmt.Errorf("indexer %q is disabled", c.Name())
	}
	key := cacheKey(c, req) + "|rescue"
	if cached, ok := c.cacheGet(ctx, key); ok {
		return cached, nil
	}
	if c.Exhausted(ctx) {
		return nil, fmt.Errorf("indexer %q: %w", c.Name(), core.ErrBudgetExhausted)
	}
	if cfg := c.Config(); cfg.UsesBackend() {
		// Built-in sites and addons: if Episode > 0, rescue via season-level
		// search; built-in sites also via OriginalTitle/aliases (an addon
		// searches by identifier, so another title asks the same question).
		var merged []core.Release
		seen := map[string]bool{}
		appendUnique := func(rs []core.Release) {
			for _, r := range rs {
				k := r.ID
				if r.InfoHash != "" {
					k = r.InfoHash
				}
				if !seen[k] {
					seen[k] = true
					merged = append(merged, r)
				}
			}
		}
		if !cfg.IsStremio() && req.OriginalTitle != "" && !releaseparse.TitlesMatch(req.OriginalTitle, req.Title) {
			alt := req
			alt.Title, alt.OriginalTitle = req.OriginalTitle, ""
			if rs, err := c.backendSearch(ctx, alt); err == nil {
				appendUnique(rs)
			}
		}
		if req.Season > 0 && req.Episode > 0 {
			packReq := req
			packReq.Episode = 0
			if rs, err := c.backendSearch(ctx, packReq); err == nil {
				appendUnique(rs)
			}
		}
		c.cachePut(ctx, key, merged)
		return merged, nil
	}
	caps, _ := c.Caps(ctx)
	attempts := c.rescueAttempts(req, caps)
	if len(attempts) == 0 {
		return nil, nil
	}
	if len(attempts) > 6 {
		attempts = attempts[:6]
	}
	var merged []core.Release
	seen := map[string]bool{}
	var firstErr error
	if c.Config().DailyLimit == 0 {
		type slot struct {
			res []core.Release
			err error
		}
		slots := make([]slot, len(attempts))
		var wg sync.WaitGroup
		for i, p := range attempts {
			wg.Add(1)
			go func(i int, p url.Values) {
				defer wg.Done()
				slots[i].res, slots[i].err = c.doSearch(ctx, p)
			}(i, p)
		}
		wg.Wait()
		for _, s := range slots {
			if s.err != nil {
				if firstErr == nil {
					firstErr = s.err
				}
				continue
			}
			for _, r := range s.res {
				k := r.ID
				if r.InfoHash != "" {
					k = r.InfoHash
				}
				if !seen[k] {
					seen[k] = true
					merged = append(merged, r)
				}
			}
		}
	} else {
		for _, p := range attempts {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			res, err := c.doSearch(ctx, p)
			if err != nil {
				if errors.Is(err, core.ErrBudgetExhausted) {
					break
				}
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			for _, r := range res {
				k := r.ID
				if r.InfoHash != "" {
					k = r.InfoHash
				}
				if !seen[k] {
					seen[k] = true
					merged = append(merged, r)
				}
			}
			if len(merged) > 0 {
				break
			}
		}
	}
	if len(merged) == 0 && firstErr != nil {
		return nil, firstErr
	}
	c.cachePut(ctx, key, merged)
	return merged, nil
}

// nzbDiskPath returns the on-disk NZB path for a release id, or "".
func (c *Client) nzbDiskPath(releaseID string) string {
	if c.nzbDir == "" || releaseID == "" {
		return ""
	}
	safe := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, releaseID)
	return filepath.Join(c.nzbDir, safe+".nzb")
}

// FetchNZB implements core.Indexer.
func (c *Client) FetchNZB(ctx context.Context, r core.Release) ([]byte, error) {
	if r.Protocol == core.ProtocolTorrent {
		return nil, fmt.Errorf("newznab: %q is a torrent release; it streams through a debrid provider", r.Name)
	}
	if path := c.nzbDiskPath(r.ID); path != "" {
		if b, err := os.ReadFile(path); err == nil && len(b) > 0 {
			return b, nil
		}
	}
	if c.Config().UsesBackend() {
		b, err := c.backend()
		if err != nil {
			return nil, err
		}
		body, err := b.FetchNZB(ctx, r, c.reserve)
		if err != nil {
			return nil, err
		}
		c.saveNZB(r.ID, body)
		return body, nil
	}
	if err := c.reserve(ctx, "grab"); err != nil {
		return nil, err
	}
	rawURL := r.NZBURL
	if rawURL == "" {
		if r.GUID == "" {
			return nil, fmt.Errorf("newznab: release %q has no NZB URL or GUID", r.Name)
		}
		u, err := c.apiURL(url.Values{"t": {"get"}, "id": {r.GUID}})
		if err != nil {
			return nil, err
		}
		rawURL = u
	} else if !strings.HasPrefix(rawURL, "http://") && !strings.HasPrefix(rawURL, "https://") {
		base, err := url.Parse(strings.TrimRight(c.Config().URL, "/") + "/")
		if err != nil {
			return nil, redactURLError(err)
		}
		ref, err := url.Parse(rawURL)
		if err != nil {
			return nil, redactURLError(err)
		}
		rawURL = base.ResolveReference(ref).String()
	}
	body, err := c.get(ctx, rawURL, c.maxNZB)
	if err != nil {
		return nil, err
	}
	// Gzipped NZB payload (magic bytes) — Go's transport only undoes
	// Content-Encoding, not a stored .nzb.gz body.
	if len(body) >= 2 && body[0] == 0x1f && body[1] == 0x8b {
		gz, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("newznab: gunzip NZB: %w", err)
		}
		body, err = io.ReadAll(io.LimitReader(gz, c.maxNZB+1))
		gz.Close()
		if err != nil {
			return nil, err
		}
		if int64(len(body)) > c.maxNZB {
			return nil, fmt.Errorf("newznab: NZB exceeds %d bytes", c.maxNZB)
		}
	}
	trimmed := bytes.TrimSpace(body)
	if !bytes.Contains(bytes.ToLower(trimmed[:min(len(trimmed), 2048)]), []byte("<nzb")) {
		if _, perr := parseFeed(body, c.Name()); perr != nil {
			return nil, perr
		}
		return nil, fmt.Errorf("newznab: response for %q is not an NZB", r.Name)
	}
	c.saveNZB(r.ID, body)
	return body, nil
}

// saveNZB keeps a fetched NZB on disk so later plays need no grab.
func (c *Client) saveNZB(releaseID string, body []byte) {
	if path := c.nzbDiskPath(releaseID); path != "" {
		if err := os.MkdirAll(c.nzbDir, 0o755); err == nil {
			tmp := path + ".tmp"
			if os.WriteFile(tmp, body, 0o644) == nil {
				if err := os.Rename(tmp, path); err != nil {
					c.log.Debug("newznab save NZB failed", "indexer", c.Name(), "err", err)
				}
			}
		}
	}
}

// backend returns the search backend of a built-in site or addon.
func (c *Client) backend() (Backend, error) {
	if c.backends == nil {
		return nil, fmt.Errorf("indexer %q: built-in sites and addons are not available", c.Name())
	}
	return c.backends(c.Config())
}

// backendSearch runs a built-in indexer's search and stamps the results
// the way Newznab results are stamped.
func (c *Client) backendSearch(ctx context.Context, req core.SearchRequest) ([]core.Release, error) {
	b, err := c.backend()
	if err != nil {
		return nil, err
	}
	results, err := b.Search(ctx, req, c.reserve)
	if err != nil {
		var rl *RateLimitError
		if errors.As(err, &rl) {
			return nil, c.recordRateLimit(ctx, rl.RetryAfter, err)
		}
		return nil, err
	}
	name := c.Name()
	for i := range results {
		r := &results[i]
		r.Indexer = name
		if r.ID == "" {
			r.ID = releaseID(name, r.GUID, r.NZBURL+r.MagnetURL+r.Name)
		}
		if r.Info.Title == "" && r.Name != "" {
			r.Info = releaseparse.Parse(r.Name)
		}
	}
	return results, nil
}

// Test verifies an indexer config for the setup wizard: caps plus one
// search, with human-readable errors.
func Test(ctx context.Context, cfg config.Indexer) error {
	return TestWithOptions(ctx, cfg, Options{})
}

// TestWithOptions is Test with injectable transport/clock (tests).
func TestWithOptions(ctx context.Context, cfg config.Indexer, opts Options) error {
	if cfg.URL == "" {
		return fmt.Errorf("indexer %q: URL is empty", cfg.Name)
	}
	// Test always runs, even for a not-yet-enabled config.
	testCfg := cfg
	testCfg.Enabled = true
	c := New(testCfg, nil, opts)
	name := cfg.Name
	if name == "" {
		name = c.Name()
	}
	if _, err := c.Caps(ctx); err != nil {
		return humanError(name, err)
	}
	params := url.Values{"t": {"search"}, "q": {"test"}, "limit": {"1"}, "extended": {"1"}}
	if _, err := c.doSearch(ctx, params); err != nil {
		return humanError(name, err)
	}
	return nil
}
