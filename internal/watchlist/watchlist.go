// Package watchlist ingests authorized viewers' watchlists and lazily checks
// their release availability. It never adds content or writes to Plex/Trakt.
package watchlist

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/braedonsaunders/ether/internal/catalog"
	"github.com/braedonsaunders/ether/internal/core"
	"github.com/braedonsaunders/ether/internal/schedule"
	"github.com/braedonsaunders/ether/internal/store"
)

func init() {
	store.Register("watchlist", 1, `
CREATE TABLE watchlist_settings(id INTEGER PRIMARY KEY CHECK(id=1),value TEXT NOT NULL);
CREATE TABLE watchlist_items(user_id INTEGER NOT NULL,item_key INTEGER NOT NULL,source TEXT NOT NULL,title TEXT NOT NULL,added INTEGER NOT NULL,checked INTEGER NOT NULL DEFAULT 0,available INTEGER NOT NULL DEFAULT 0,notified INTEGER NOT NULL DEFAULT 0,warmed INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(user_id,item_key,source));
CREATE INDEX watchlist_viewer ON watchlist_items(user_id,available,added);
`)
	// Cluster-wide record of the last scheduled pass, so a new leader
	// continues the schedule rather than re-running at once.
	store.Register("watchlist", 2, `CREATE TABLE watchlist_meta(k TEXT PRIMARY KEY, v TEXT NOT NULL DEFAULT '');`)
}

type Source interface {
	Watchlist(context.Context, int64, int) ([]core.Title, error)
}
type Notifier interface {
	TitleAvailable(context.Context, string) error
}
type Settings struct {
	Enabled       bool `json:"enabled"`
	Plex          bool `json:"plex"`
	Trakt         bool `json:"trakt"`
	Notify        bool `json:"notify"`
	Prewarm       bool `json:"prewarm"`
	CheckHours    int  `json:"checkHours"`
	NaturalSearch bool `json:"naturalSearch"`
	Recaps        bool `json:"recaps"`
}
type Options struct {
	Catalog      core.Catalog
	Resolver     core.Resolver
	Engine       core.Engine
	Trakt        Source
	Tokens       func(context.Context, int64) (string, error)
	Users        func(context.Context) ([]core.User, error)
	Allowed      func(*core.User, *core.Title) bool
	Notifier     Notifier
	Client       *http.Client
	PlexBaseURL  string
	Now          func() time.Time
	PrewarmBytes int64
	// SyncInterval is how often Run passes over every viewer (30m).
	SyncInterval time.Duration
	// ResumeSchedule makes Run wait until SyncInterval has passed since the
	// last pass recorded in the database (by any node) before its first
	// pass, instead of passing at once. A cluster sets it because Run
	// restarts on every leader handoff.
	ResumeSchedule bool
}
type Service struct {
	db *sql.DB
	o  Options
	mu sync.Mutex
}
type Item struct {
	Title       core.Title `json:"title"`
	Sources     []string   `json:"sources"`
	AddedAt     time.Time  `json:"addedAt"`
	CheckedAt   time.Time  `json:"checkedAt,omitempty"`
	AvailableAt time.Time  `json:"availableAt,omitempty"`
}

func New(db *sql.DB, o Options) *Service {
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Client == nil {
		o.Client = &http.Client{Timeout: 20 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	if o.PlexBaseURL == "" {
		o.PlexBaseURL = "https://discover.provider.plex.tv"
	}
	if o.PrewarmBytes <= 0 {
		o.PrewarmBytes = 8 << 20
	}
	if o.SyncInterval <= 0 {
		o.SyncInterval = 30 * time.Minute
	}
	return &Service{db: db, o: o}
}
func (s *Service) Settings(ctx context.Context) (Settings, error) {
	var raw string
	v := Settings{Plex: true, Trakt: true, CheckHours: 6}
	err := s.db.QueryRowContext(ctx, `SELECT value FROM watchlist_settings WHERE id=1`).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return v, nil
	}
	if err != nil {
		return v, err
	}
	err = json.Unmarshal([]byte(raw), &v)
	return v, err
}
func (s *Service) SaveSettings(ctx context.Context, v Settings) error {
	if v.CheckHours < 1 || v.CheckHours > 168 {
		return errors.New("check interval must be between 1 and 168 hours")
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO watchlist_settings(id,value) VALUES(1,?) ON CONFLICT(id) DO UPDATE SET value=excluded.value`, string(raw))
	return err
}
func (s *Service) allowed(u *core.User, t *core.Title) bool {
	if u == nil || t == nil || t.Adult || catalog.RatingExceeds(t.ContentRating, u.MaxContentRating) {
		return false
	}
	section := 1
	if t.Type != core.TypeMovie {
		section = 2
	}
	if len(u.AllowedSections) > 0 {
		ok := false
		for _, id := range u.AllowedSections {
			ok = ok || id == section
		}
		if !ok {
			return false
		}
	}
	return s.o.Allowed == nil || s.o.Allowed(u, t)
}
func (s *Service) Remind(ctx context.Context, u *core.User, key core.RatingKey) error {
	t, err := s.o.Catalog.Get(ctx, key)
	if err != nil {
		return err
	}
	if !s.allowed(u, t) {
		return core.ErrNotFound
	}
	return s.upsert(ctx, u.ID, "manual", *t)
}
func (s *Service) Remove(ctx context.Context, userID int64, key core.RatingKey) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM watchlist_items WHERE user_id=? AND item_key=?`, userID, key)
	return err
}
func (s *Service) upsert(ctx context.Context, uid int64, source string, t core.Title) error {
	raw, err := json.Marshal(t)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO watchlist_items(user_id,item_key,source,title,added) VALUES(?,?,?,?,?) ON CONFLICT(user_id,item_key,source) DO UPDATE SET title=excluded.title`, uid, t.Key, source, string(raw), s.o.Now().Unix())
	return err
}
func (s *Service) replace(ctx context.Context, u *core.User, source string, ts []core.Title) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	keys := map[core.RatingKey]bool{}
	for _, t := range ts {
		if !s.allowed(u, &t) {
			continue
		}
		keys[t.Key] = true
		raw, err := json.Marshal(t)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO watchlist_items(user_id,item_key,source,title,added) VALUES(?,?,?,?,?) ON CONFLICT(user_id,item_key,source) DO UPDATE SET title=excluded.title`, u.ID, t.Key, source, string(raw), s.o.Now().Unix()); err != nil {
			return err
		}
	}
	rows, err := tx.QueryContext(ctx, `SELECT item_key FROM watchlist_items WHERE user_id=? AND source=?`, u.ID, source)
	if err != nil {
		return err
	}
	var removed []core.RatingKey
	for rows.Next() {
		var k core.RatingKey
		if err = rows.Scan(&k); err != nil {
			rows.Close()
			return err
		}
		if !keys[k] {
			removed = append(removed, k)
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, k := range removed {
		if _, err = tx.ExecContext(ctx, `DELETE FROM watchlist_items WHERE user_id=? AND source=? AND item_key=?`, u.ID, source, k); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// Sync preserves the last successful snapshot when an upstream fails.
func (s *Service) Sync(ctx context.Context, u *core.User) error {
	cfg, err := s.Settings(ctx)
	if err != nil {
		return err
	}
	if u == nil {
		return core.ErrUnauthorized
	}
	var errs []error
	if !cfg.Enabled {
		return nil
	}
	if cfg.Plex && s.o.Tokens != nil {
		token, e := s.o.Tokens(ctx, u.ID)
		if e != nil {
			errs = append(errs, errors.New("Plex watchlist requires this viewer's account token"))
		} else if token != "" {
			ts, e := s.plex(ctx, token)
			if e == nil {
				e = s.replace(ctx, u, "plex", ts)
			}
			errs = append(errs, e)
		}
	}
	if cfg.Trakt && s.o.Trakt != nil {
		ts, e := s.o.Trakt.Watchlist(ctx, u.ID, 1000)
		if e == nil {
			e = s.replace(ctx, u, "trakt", ts)
		}
		errs = append(errs, e)
	}
	return errors.Join(errs...)
}
func (s *Service) Items(ctx context.Context, u *core.User) ([]Item, error) {
	out := []Item{}
	if u == nil {
		return out, core.ErrUnauthorized
	}
	rows, err := s.db.QueryContext(ctx, `SELECT title,source,added,checked,available FROM watchlist_items WHERE user_id=? ORDER BY added DESC,item_key`, u.ID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	byKey := map[core.RatingKey]int{}
	for rows.Next() {
		var raw, source string
		var added, checked, available int64
		if err := rows.Scan(&raw, &source, &added, &checked, &available); err != nil {
			return nil, err
		}
		var t core.Title
		if json.Unmarshal([]byte(raw), &t) != nil || !t.IsReleased(s.o.Now()) || !s.allowed(u, &t) {
			continue
		}
		if i, ok := byKey[t.Key]; ok {
			out[i].Sources = append(out[i].Sources, source)
			continue
		}
		it := Item{Title: t, Sources: []string{source}, AddedAt: time.Unix(added, 0)}
		if checked > 0 {
			it.CheckedAt = time.Unix(checked, 0)
		}
		if available > 0 {
			it.AvailableAt = time.Unix(available, 0)
		}
		byKey[t.Key] = len(out)
		out = append(out, it)
	}
	return out, rows.Err()
}

// Check resolves only explicitly watched-for, released titles, never the whole
// catalogue. A healthy resolver result is required before marking available.
func (s *Service) Check(ctx context.Context, u *core.User) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cfg, err := s.Settings(ctx)
	if err != nil || !cfg.Enabled || s.o.Resolver == nil {
		return err
	}
	items, err := s.Items(ctx, u)
	if err != nil {
		return err
	}
	checked := 0
	for _, it := range items {
		if checked >= 20 || ctx.Err() != nil {
			break
		}
		t := it.Title
		if fresh, e := s.o.Catalog.Get(ctx, t.Key); e == nil && fresh != nil {
			t = *fresh
		}
		if !s.allowed(u, &t) || !t.IsReleased(s.o.Now()) || (!it.CheckedAt.IsZero() && s.o.Now().Sub(it.CheckedAt) < time.Duration(cfg.CheckHours)*time.Hour) {
			continue
		}
		checked++
		resolveTitle := &t
		if t.Type == core.TypeShow || t.Type == core.TypeSeason {
			var episodes []core.Title
			var e error
			if t.Type == core.TypeSeason {
				episodes, e = s.o.Catalog.Children(ctx, t.Key)
			} else {
				episodes, e = s.o.Catalog.Grandchildren(ctx, t.Key)
			}
			if e != nil {
				continue
			}
			resolveTitle = nil
			for i := range episodes {
				e := &episodes[i]
				if e.ContentRating == "" {
					e.ContentRating = t.ContentRating
				}
				if t.Adult {
					e.Adult = true
				}
				if e.IsReleased(s.o.Now()) && s.allowed(u, e) && (resolveTitle == nil || e.Season < resolveTitle.Season || (e.Season == resolveTitle.Season && e.Episode < resolveTitle.Episode)) {
					resolveTitle = e
				}
			}
			if resolveTitle == nil {
				continue
			}
		}
		caps := core.ClientCaps{MaxHeight: 1080, VideoCodecs: []string{"h264", "hevc"}, AudioCodecs: []string{"aac", "ac3", "eac3"}, Containers: []string{"mkv", "mp4"}}
		job, cancel := context.WithTimeout(ctx, 45*time.Second)
		res, e := s.o.Resolver.Resolve(job, core.ResolveRequest{Title: resolveTitle, User: u, Client: caps})
		cancel()
		now := s.o.Now().Unix()
		if _, err = s.db.ExecContext(ctx, `UPDATE watchlist_items SET checked=? WHERE user_id=? AND item_key=?`, now, u.ID, t.Key); err != nil {
			return err
		}
		if errors.Is(e, core.ErrNotAvailable) {
			if _, err = s.db.ExecContext(ctx, `UPDATE watchlist_items SET available=0,notified=0,warmed=0 WHERE user_id=? AND item_key=?`, u.ID, t.Key); err != nil {
				return err
			}
		}
		if e != nil || res == nil || len(res.NZB) == 0 || res.Release.ID == "" {
			continue
		}
		if _, err = s.db.ExecContext(ctx, `UPDATE watchlist_items SET available=CASE WHEN available=0 THEN ? ELSE available END,title=? WHERE user_id=? AND item_key=?`, now, mustTitle(t), u.ID, t.Key); err != nil {
			return err
		}
		var notified, warmed int
		if err = s.db.QueryRowContext(ctx, `SELECT MAX(notified),MAX(warmed) FROM watchlist_items WHERE user_id=? AND item_key=?`, u.ID, t.Key).Scan(&notified, &warmed); err != nil {
			return err
		}
		if cfg.Notify && notified == 0 && s.o.Notifier != nil {
			if e := s.o.Notifier.TitleAvailable(ctx, t.Title); e == nil {
				_, err = s.db.ExecContext(ctx, `UPDATE watchlist_items SET notified=1 WHERE user_id=? AND item_key=?`, u.ID, t.Key)
				if err != nil {
					return err
				}
			}
		}
		if cfg.Prewarm && warmed == 0 && s.o.Engine != nil {
			job, cancel := context.WithTimeout(ctx, time.Minute)
			e := s.o.Engine.Prefetch(job, res.Release.ID, res.NZB, 0, s.o.PrewarmBytes)
			cancel()
			if e == nil {
				_, err = s.db.ExecContext(ctx, `UPDATE watchlist_items SET warmed=1 WHERE user_id=? AND item_key=?`, u.ID, t.Key)
				if err != nil {
					return err
				}
			}
		}
	}
	return ctx.Err()
}
func mustTitle(t core.Title) string { raw, _ := json.Marshal(t); return string(raw) }
func (s *Service) Hubs(ctx context.Context, u *core.User, limit int) ([]core.Hub, error) {
	cfg, err := s.Settings(ctx)
	if err != nil || !cfg.Enabled {
		return nil, err
	}
	items, err := s.Items(ctx, u)
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 12
	}
	soon, ready := core.Hub{ID: "watchlist.coming", Title: "Waiting for a stream", Context: "hub.home.watchlist", Promoted: true}, core.Hub{ID: "watchlist.available", Title: "Now available", Context: "hub.home.watchlist.available", Promoted: true}
	for _, it := range items {
		if !it.AvailableAt.IsZero() {
			if len(ready.Items) < limit {
				ready.Items = append(ready.Items, it.Title)
			}
		} else if len(soon.Items) < limit {
			soon.Items = append(soon.Items, it.Title)
		}
	}
	var out []core.Hub
	if len(ready.Items) > 0 {
		out = append(out, ready)
	}
	if len(soon.Items) > 0 {
		out = append(out, soon)
	}
	return out, nil
}

// Run is cancelled by the app; individual viewers are isolated and bounded.
// Run passes over every viewer (watchlist sync and availability check) at
// once and then every SyncInterval; with ResumeSchedule the first pass
// waits out the interval since the last recorded pass.
func (s *Service) Run(ctx context.Context) {
	if !schedule.Wait(ctx, s.o.Now, s.untilDue(ctx)) {
		return
	}

	run := func() {
		defer s.recordPass(ctx)
		if s.o.Users == nil {
			return
		}
		us, err := s.o.Users(ctx)
		if err != nil {
			return
		}
		for i := range us {
			if ctx.Err() != nil {
				return
			}
			job, cancel := context.WithTimeout(ctx, 3*time.Minute)
			if err := s.Sync(job, &us[i]); err != nil {
				slog.Default().Warn("watchlist sync failed", "user", us[i].ID, "err", err)
			}
			if err := s.Check(job, &us[i]); err != nil {
				slog.Default().Warn("watchlist check failed", "user", us[i].ID, "err", err)
			}
			cancel()
		}
	}
	for ctx.Err() == nil {
		run()
		if !schedule.Wait(ctx, s.o.Now, s.o.SyncInterval) {
			return
		}
	}
}

// untilDue is how long Run waits before its first pass.
func (s *Service) untilDue(ctx context.Context) time.Duration {
	if !s.o.ResumeSchedule {
		return 0
	}
	var v string
	if err := s.db.QueryRowContext(ctx, `SELECT v FROM watchlist_meta WHERE k='last_pass'`).Scan(&v); err != nil {
		return 0
	}
	ms, err := strconv.ParseInt(v, 10, 64)
	if err != nil || ms <= 0 {
		return 0
	}
	return min(s.o.SyncInterval, max(time.UnixMilli(ms).Add(s.o.SyncInterval).Sub(s.o.Now().Round(0)), 0))
}

// recordPass notes a finished (or attempted) pass for ResumeSchedule.
func (s *Service) recordPass(ctx context.Context) {
	if ctx.Err() != nil {
		return // interrupted (leadership lost): let the next leader redo it
	}
	if _, err := s.db.ExecContext(ctx, `INSERT INTO watchlist_meta(k,v) VALUES('last_pass',?) ON CONFLICT(k) DO UPDATE SET v=excluded.v`,
		strconv.FormatInt(s.o.Now().UnixMilli(), 10)); err != nil {
		slog.Default().Warn("watchlist: record pass failed", "err", err)
	}
}
func (s *Service) user(ctx context.Context, id int64) (*core.User, error) {
	if s.o.Users == nil {
		return nil, core.ErrNotFound
	}
	users, err := s.o.Users(ctx)
	if err != nil {
		return nil, err
	}
	for i := range users {
		if users[i].ID == id {
			return &users[i], nil
		}
	}
	return nil, fmt.Errorf("viewer: %w", core.ErrNotFound)
}
