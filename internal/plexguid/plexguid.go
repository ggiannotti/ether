// Package plexguid maps verified external identities through Plex's metadata
// provider. It never invents plex:// IDs or grants Plex account entitlements.
package plexguid

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/braedonsaunders/ether/internal/core"
	"github.com/braedonsaunders/ether/internal/store"
)

func init() {
	store.Register("plexguid", 1, `CREATE TABLE plexguid_mappings(rating_key INTEGER PRIMARY KEY,guid TEXT NOT NULL,checked_at INTEGER NOT NULL);`)
}

const (
	cacheTTL        = 30 * 24 * time.Hour
	recheckInterval = 24 * time.Hour
	bgWorkers       = 2
	bgQueueSize     = 256
	bgLookupTimeout = 3 * time.Second
)

type Options struct {
	HTTP       *http.Client
	BaseURL    string
	Token      func() string
	Background bool
}

type Service struct {
	db *sql.DB
	o  Options

	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	closeOne sync.Once

	mu      sync.Mutex
	idle    *sync.Cond
	pending map[core.RatingKey]bool
	jobs    chan core.RatingKey
}

func New(db *sql.DB, o Options) *Service {
	if o.HTTP == nil {
		o.HTTP = &http.Client{Timeout: 5 * time.Second}
	}
	if o.BaseURL == "" {
		o.BaseURL = "https://metadata.provider.plex.tv"
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Service{
		db:      db,
		o:       o,
		ctx:     ctx,
		cancel:  cancel,
		pending: map[core.RatingKey]bool{},
	}
	s.idle = sync.NewCond(&s.mu)
	if o.Background {
		s.jobs = make(chan core.RatingKey, bgQueueSize)
		for i := 0; i < bgWorkers; i++ {
			s.wg.Add(1)
			go s.worker()
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			<-ctx.Done()
			s.mu.Lock()
			s.idle.Broadcast()
			s.mu.Unlock()
		}()
	}
	return s
}

func (s *Service) Close() {
	s.closeOne.Do(func() {
		s.cancel()
		s.wg.Wait()
	})
}

// Wait blocks until all queued background GUID resolutions finish.
func (s *Service) Wait() {
	if s.jobs == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for len(s.pending) > 0 && s.ctx.Err() == nil {
		s.idle.Wait()
	}
}

// Schedule queues keys for asynchronous plex:// GUID resolution when
// Background is enabled. Safe to call on hot render paths.
func (s *Service) Schedule(keys ...core.RatingKey) {
	if s == nil || s.jobs == nil || s.ctx.Err() != nil {
		return
	}
	if s.o.Token == nil || s.o.Token() == "" {
		return
	}
	for _, k := range keys {
		if !resolvableKey(k) {
			continue
		}
		s.mu.Lock()
		if s.pending[k] {
			s.mu.Unlock()
			continue
		}
		s.pending[k] = true
		s.mu.Unlock()
		select {
		case s.jobs <- k:
		default:
			s.done(k)
		}
	}
}

func (s *Service) worker() {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case k := <-s.jobs:
			func() {
				defer s.done(k)
				rctx, cancel := context.WithTimeout(s.ctx, bgLookupTimeout)
				defer cancel()
				_, _ = s.Resolve(rctx, k)
			}()
		}
	}
}

func (s *Service) done(k core.RatingKey) {
	s.mu.Lock()
	delete(s.pending, k)
	if len(s.pending) == 0 {
		s.idle.Broadcast()
	}
	s.mu.Unlock()
}

var validGUID = regexp.MustCompile(`^plex://(movie|show|season|episode)/[a-f0-9]{24}$`)

func resolvableKey(k core.RatingKey) bool {
	switch k.Kind() {
	case core.KindMovie, core.KindShow:
		return k.TMDB() > 0
	case core.KindSeason:
		return k.TMDB() > 0 && k.Season() >= 0 && k.Season() < 9000
	case core.KindEpisode:
		return k.TMDB() > 0 && k.Season() >= 0 && k.Season() < 9000 && k.Episode() > 0
	default:
		return false
	}
}

// Cached is safe for lists: no network requests or credentials are needed.
func (s *Service) Cached(ctx context.Context, k core.RatingKey) string {
	var guid string
	_ = s.db.QueryRowContext(ctx, `SELECT guid FROM plexguid_mappings WHERE rating_key=? AND checked_at>?`, int64(k), time.Now().Add(-cacheTTL).Unix()).Scan(&guid)
	return guid
}

// CachedMany batch-loads cached non-empty plex:// GUIDs for keys without
// network activity.
func (s *Service) CachedMany(ctx context.Context, keys []core.RatingKey) map[core.RatingKey]string {
	if s == nil || s.db == nil || len(keys) == 0 {
		return nil
	}
	uniq := make([]core.RatingKey, 0, len(keys))
	seen := make(map[core.RatingKey]struct{}, len(keys))
	for _, k := range keys {
		if k == 0 {
			continue
		}
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		uniq = append(uniq, k)
	}
	if len(uniq) == 0 {
		return nil
	}
	cutoff := time.Now().Add(-cacheTTL).Unix()
	out := make(map[core.RatingKey]string, len(uniq))
	const chunkSize = 200
	for start := 0; start < len(uniq); start += chunkSize {
		end := min(start+chunkSize, len(uniq))
		chunk := uniq[start:end]
		var b strings.Builder
		b.WriteString(`SELECT rating_key,guid FROM plexguid_mappings WHERE rating_key IN (`)
		args := make([]any, 0, len(chunk)+1)
		for i, k := range chunk {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteByte('?')
			args = append(args, int64(k))
		}
		b.WriteString(`) AND checked_at>?`)
		args = append(args, cutoff)
		rows, err := s.db.QueryContext(ctx, b.String(), args...)
		if err != nil {
			continue
		}
		for rows.Next() {
			var rk int64
			var guid string
			if err := rows.Scan(&rk, &guid); err == nil && guid != "" {
				out[core.RatingKey(rk)] = guid
			}
		}
		if err := rows.Err(); err != nil {
			slog.Default().Warn("plexguid: cached mappings not fully loaded", "err", err)
		}
		_ = rows.Close()
	}
	return out
}

func (s *Service) freshCached(ctx context.Context, k core.RatingKey) (string, bool) {
	var cached string
	var at int64
	if e := s.db.QueryRowContext(ctx, `SELECT guid,checked_at FROM plexguid_mappings WHERE rating_key=?`, int64(k)).Scan(&cached, &at); e == nil && time.Since(time.Unix(at, 0)) < recheckInterval {
		return cached, true
	}
	return "", false
}

func (s *Service) storeMapping(ctx context.Context, k core.RatingKey, guid string, now int64) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO plexguid_mappings(rating_key,guid,checked_at) VALUES(?,?,?) ON CONFLICT(rating_key) DO UPDATE SET guid=excluded.guid,checked_at=excluded.checked_at`, int64(k), guid, now)
	return err
}

func (s *Service) Resolve(ctx context.Context, k core.RatingKey) (string, error) {
	if !resolvableKey(k) {
		return "", nil
	}
	if cached, ok := s.freshCached(ctx, k); ok {
		return cached, nil
	}
	if s.o.Token == nil || s.o.Token() == "" {
		return "", core.ErrUnauthorized
	}
	switch k.Kind() {
	case core.KindMovie:
		return s.resolveMovieOrShow(ctx, k, "movie", "1")
	case core.KindShow:
		return s.resolveMovieOrShow(ctx, k, "show", "2")
	case core.KindSeason:
		return s.resolveSeason(ctx, k)
	case core.KindEpisode:
		return s.resolveEpisode(ctx, k)
	default:
		return "", nil
	}
}

type providerItem struct {
	GUID            string `json:"guid"`
	RatingKey       string `json:"ratingKey"`
	Type            string `json:"type"`
	Index           *int   `json:"index"`
	ParentIndex     *int   `json:"parentIndex"`
	ParentGUID      string `json:"parentGuid"`
	GrandparentGUID string `json:"grandparentGuid"`
	IDs             []struct {
		ID string `json:"id"`
	} `json:"Guid"`
}

func (s *Service) getItems(ctx context.Context, path string, q url.Values) ([]providerItem, error) {
	u := strings.TrimRight(s.o.BaseURL, "/") + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	req, e := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if e != nil {
		return nil, fmt.Errorf("Plex metadata request invalid")
	}
	req.Header.Set("X-Plex-Token", s.o.Token())
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Plex-Product", "Ether")
	req.Header.Set("X-Plex-Client-Identifier", "ether-metadata")
	// Never forward account credentials to a redirect target.
	client := *s.o.HTTP
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, e := client.Do(req)
	if e != nil {
		return nil, fmt.Errorf("Plex metadata unavailable")
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("Plex metadata HTTP %d", resp.StatusCode)
	}
	var body struct {
		Container struct {
			Metadata []providerItem `json:"Metadata"`
		} `json:"MediaContainer"`
	}
	if e = json.NewDecoder(io.LimitReader(resp.Body, 2<<20)).Decode(&body); e != nil {
		return nil, fmt.Errorf("Plex metadata response invalid")
	}
	return body.Container.Metadata, nil
}

func validKindGUID(guid, kind string) bool {
	return validGUID.MatchString(guid) && strings.HasPrefix(guid, "plex://"+kind+"/")
}

func guidHex(guid, kind string) string {
	prefix := "plex://" + kind + "/"
	if !validKindGUID(guid, kind) {
		return ""
	}
	return strings.TrimPrefix(guid, prefix)
}

func hasExternalID(item providerItem, want ...string) bool {
	for _, id := range item.IDs {
		for _, w := range want {
			if w != "" && id.ID == w {
				return true
			}
		}
	}
	return false
}

func (s *Service) resolveMovieOrShow(ctx context.Context, k core.RatingKey, kind, typ string) (string, error) {
	external := fmt.Sprintf("tmdb://%d", k.TMDB())
	items, err := s.getItems(ctx, "/library/metadata/matches", url.Values{"type": {typ}, "guid": {external}, "includeGuids": {"1"}})
	if err != nil {
		return "", err
	}
	found := ""
	for _, item := range items {
		if item.Type != kind || !validKindGUID(item.GUID, kind) {
			continue
		}
		if hasExternalID(item, external) {
			if found != "" && found != item.GUID {
				return "", fmt.Errorf("ambiguous Plex identity")
			}
			found = item.GUID
		}
	}
	now := time.Now().Unix()
	err = s.storeMapping(ctx, k, found, now)
	return found, err
}

func (s *Service) resolveSeason(ctx context.Context, k core.RatingKey) (string, error) {
	showID := k.TMDB()
	seasonNum := k.Season()
	showKey := core.ShowKey(showID)
	showExternal := fmt.Sprintf("tmdb://%d", showID)
	seasonExternal := fmt.Sprintf("tmdb://%d/%d", showID, seasonNum)

	// 1. Try direct season match first.
	q := url.Values{
		"type":         {"3"},
		"parentIndex":  {strconv.Itoa(seasonNum)},
		"guid":         {showExternal},
		"includeGuids": {"1"},
	}
	if items, err := s.getItems(ctx, "/library/metadata/matches", q); err == nil {
		found := ""
		parentShow := ""
		for _, item := range items {
			if item.Type != "season" || !validKindGUID(item.GUID, "season") {
				continue
			}
			indexOK := item.Index == nil || *item.Index == seasonNum
			extOK := hasExternalID(item, seasonExternal) || (item.Index != nil && hasExternalID(item, showExternal))
			if !indexOK || !extOK {
				continue
			}
			if found != "" && found != item.GUID {
				return "", fmt.Errorf("ambiguous Plex identity")
			}
			found = item.GUID
			if validKindGUID(item.ParentGUID, "show") {
				parentShow = item.ParentGUID
			}
		}
		if found != "" {
			now := time.Now().Unix()
			if parentShow != "" {
				_ = s.storeMapping(ctx, showKey, parentShow, now)
			}
			return found, s.storeMapping(ctx, k, found, now)
		}
	}

	// 2. Fallback: resolve the parent show and fetch its season children.
	showGUID, err := s.Resolve(ctx, showKey)
	if err != nil {
		return "", err
	}
	showHex := guidHex(showGUID, "show")
	if showHex == "" {
		_ = s.storeMapping(ctx, k, "", time.Now().Unix())
		return "", nil
	}
	children, err := s.getItems(ctx, "/library/metadata/"+url.PathEscape(showHex)+"/children", nil)
	if err != nil {
		return "", err
	}
	now := time.Now().Unix()
	found := ""
	for _, item := range children {
		if item.Type != "season" || !validKindGUID(item.GUID, "season") || item.Index == nil || *item.Index < 0 || *item.Index >= 9000 {
			continue
		}
		sk := core.SeasonKey(showID, *item.Index)
		_ = s.storeMapping(ctx, sk, item.GUID, now)
		if *item.Index == seasonNum {
			found = item.GUID
		}
	}
	if found == "" {
		_ = s.storeMapping(ctx, k, "", now)
	}
	return found, nil
}

func (s *Service) resolveEpisode(ctx context.Context, k core.RatingKey) (string, error) {
	showID := k.TMDB()
	seasonNum := k.Season()
	epNum := k.Episode()
	showKey := core.ShowKey(showID)
	seasonKey := core.SeasonKey(showID, seasonNum)
	showExternal := fmt.Sprintf("tmdb://%d", showID)
	epExternal := fmt.Sprintf("tmdb://%d/%d/%d", showID, seasonNum, epNum)

	// 1. Try direct episode match first.
	q := url.Values{
		"type":         {"4"},
		"parentIndex":  {strconv.Itoa(seasonNum)},
		"index":        {strconv.Itoa(epNum)},
		"guid":         {showExternal},
		"includeGuids": {"1"},
	}
	if items, err := s.getItems(ctx, "/library/metadata/matches", q); err == nil {
		found := ""
		parentSeason := ""
		grandparentShow := ""
		for _, item := range items {
			if item.Type != "episode" || !validKindGUID(item.GUID, "episode") {
				continue
			}
			indexOK := (item.Index == nil || *item.Index == epNum) && (item.ParentIndex == nil || *item.ParentIndex == seasonNum)
			extOK := hasExternalID(item, epExternal) || (item.Index != nil && item.ParentIndex != nil && hasExternalID(item, showExternal))
			if !indexOK || !extOK {
				continue
			}
			if found != "" && found != item.GUID {
				return "", fmt.Errorf("ambiguous Plex identity")
			}
			found = item.GUID
			if validKindGUID(item.ParentGUID, "season") {
				parentSeason = item.ParentGUID
			}
			if validKindGUID(item.GrandparentGUID, "show") {
				grandparentShow = item.GrandparentGUID
			}
		}
		if found != "" {
			now := time.Now().Unix()
			if grandparentShow != "" {
				_ = s.storeMapping(ctx, showKey, grandparentShow, now)
			}
			if parentSeason != "" {
				_ = s.storeMapping(ctx, seasonKey, parentSeason, now)
			}
			return found, s.storeMapping(ctx, k, found, now)
		}
	}

	// 2. Fallback: resolve the parent season and fetch its episode children.
	seasonGUID, err := s.Resolve(ctx, seasonKey)
	if err != nil {
		return "", err
	}
	seasonHex := guidHex(seasonGUID, "season")
	if seasonHex == "" {
		_ = s.storeMapping(ctx, k, "", time.Now().Unix())
		return "", nil
	}
	children, err := s.getItems(ctx, "/library/metadata/"+url.PathEscape(seasonHex)+"/children", nil)
	if err != nil {
		return "", err
	}
	now := time.Now().Unix()
	found := ""
	for _, item := range children {
		if item.Type != "episode" || !validKindGUID(item.GUID, "episode") || item.Index == nil || *item.Index <= 0 {
			continue
		}
		ek := core.EpisodeKey(showID, seasonNum, *item.Index)
		_ = s.storeMapping(ctx, ek, item.GUID, now)
		if *item.Index == epNum {
			found = item.GUID
		}
	}
	if found == "" {
		_ = s.storeMapping(ctx, k, "", now)
	}
	return found, nil
}
