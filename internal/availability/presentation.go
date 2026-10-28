package availability

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"time"

	"github.com/braedonsaunders/ether/internal/core"
)

type FeedStatus struct {
	core.ReleaseFeed
	Selected    bool   `json:"selected"`
	LastAttempt int64  `json:"lastAttempt"`
	LastSync    int64  `json:"lastSync"`
	Gap         bool   `json:"gap"`
	Message     string `json:"message"`
	Matched     int    `json:"matched"`
}
type Observation struct {
	Key           core.RatingKey `json:"key"`
	Title         string         `json:"title"`
	Release       string         `json:"release"`
	Protocol      string         `json:"protocol"`
	State         string         `json:"state"`
	Reason        string         `json:"reason"`
	FirstSeen     int64          `json:"firstSeen"`
	FirstVerified int64          `json:"firstVerified"`
	VerifiedAt    int64          `json:"verifiedAt"`
	NextCheck     int64          `json:"nextCheck"`
}
type Status struct {
	Mode    string           `json:"mode"`
	Message string           `json:"message"`
	Feeds   []FeedStatus     `json:"feeds"`
	Usage   map[string]int64 `json:"usage"`
	Counts  map[string]int   `json:"counts"`
	Recent  []Observation    `json:"recent"`
	Day     string           `json:"day"`
}

func (s *Service) Status(ctx context.Context) (Status, error) {
	c := s.cfg().Availability
	out := Status{Mode: "off", Message: "Background monitoring and checks are off. Recent titles still require local release evidence.", Feeds: []FeedStatus{}, Usage: map[string]int64{}, Counts: map[string]int{}, Recent: []Observation{}, Day: s.now().UTC().Format("2006-01-02")}
	if c.Monitor {
		out.Mode = "monitoring"
		out.Message = "Watching configured indexers for eligible releases. Movies need evidence for 60 days; episodes for 24 hours."
	}
	if c.Verify {
		out.Mode = "checking"
		out.Message = "Checking discovered releases within the background allowance."
	}
	if !s.usable("") && !s.usable(core.ProtocolTorrent) {
		out.Message = "No playback provider is enabled. Older catalogue entries remain visible; recent titles need usable source evidence."
	}
	if s.d.Active != nil && s.d.Active() && c.Verify {
		out.Message = "Source checks are paused while playback is active; feed monitoring can continue."
	}
	for _, f := range s.d.Feeds.FeedSources() {
		x := FeedStatus{ReleaseFeed: f, Selected: selected(c.IndexerIDs, f.ID)}
		var gap int
		err := s.db.QueryRowContext(ctx, `SELECT checked_at,synced_at,gap,message,discovered FROM availability_feeds WHERE source_id=? AND generation=?`, f.ID, f.Generation).Scan(&x.LastAttempt, &x.LastSync, &gap, &x.Message, &x.Matched)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return out, err
		}
		x.Gap = gap != 0
		out.Feeds = append(out.Feeds, x)
	}
	rows, err := s.db.QueryContext(ctx, `SELECT resource,used FROM availability_budget WHERE day=?`, out.Day)
	if err != nil {
		return out, err
	}
	for rows.Next() {
		var k string
		var n int64
		if err = rows.Scan(&k, &n); err != nil {
			rows.Close()
			return out, err
		}
		out.Usage[k] = n
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return out, err
	}
	rows, err = s.db.QueryContext(ctx, `SELECT CASE WHEN state='verified' AND (verified_at<? OR policy<>?) THEN 'stale' ELSE state END AS label,COUNT(*) FROM availability_releases GROUP BY label`, s.now().Add(-6*time.Hour).Unix(), s.policy())
	if err != nil {
		return out, err
	}
	for rows.Next() {
		var k string
		var n int
		if err = rows.Scan(&k, &n); err != nil {
			rows.Close()
			return out, err
		}
		out.Counts[k] = n
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return out, err
	}
	rows, err = s.db.QueryContext(ctx, `SELECT rating_key,title,name,protocol,state,reason,first_seen,first_verified,verified_at,next_check,policy FROM availability_releases ORDER BY last_seen DESC LIMIT 60`)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	policy := s.policy()
	for rows.Next() {
		var r Observation
		var p string
		if err = rows.Scan(&r.Key, &r.Title, &r.Release, &r.Protocol, &r.State, &r.Reason, &r.FirstSeen, &r.FirstVerified, &r.VerifiedAt, &r.NextCheck, &p); err != nil {
			return out, err
		}
		if r.State == "verified" && (p != policy || r.VerifiedAt < s.now().Add(-6*time.Hour).Unix()) {
			r.State = "stale"
			r.Reason = "Earlier verification needs refreshing"
		}
		out.Recent = append(out.Recent, r)
	}
	return out, rows.Err()
}

// Handler is mounted only inside the authenticated owner API.
func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		v, err := s.Status(r.Context())
		if err != nil {
			http.Error(w, "Availability status could not be loaded", 503)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(v)
	})
	mux.HandleFunc("POST /sync", func(w http.ResponseWriter, r *http.Request) {
		if !s.cfg().Availability.Monitor {
			http.Error(w, "Enable release monitoring first", 409)
			return
		}
		// Durable request: the cluster leader sees it on its next scheduler tick.
		if _, err := s.db.ExecContext(r.Context(), `UPDATE availability_feeds SET checked_at=0`); err != nil {
			http.Error(w, "Could not schedule feed sync", 503)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	})
	return mux
}
func (s *Service) verifiedKeys(ctx context.Context) []core.RatingKey {
	rows, err := s.db.QueryContext(ctx, `SELECT rating_key,MAX(first_verified) AS first_at FROM availability_releases WHERE verified_at>=? AND policy=? AND state='verified' GROUP BY rating_key ORDER BY first_at DESC LIMIT 1000`, s.now().Add(-6*time.Hour).Unix(), s.policy())
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []core.RatingKey
	for rows.Next() {
		var k core.RatingKey
		var at int64
		if rows.Scan(&k, &at) == nil {
			out = append(out, k)
		}
	}
	if err := rows.Err(); err != nil {
		s.log.Warn("availability verified keys not fully loaded", "err", err)
	}
	return out
}
func (s *Service) Prioritize(ctx context.Context, items []core.Title) []core.Title {
	items = s.FilterDiscovery(ctx, items)
	if !s.cfg().Availability.PreferVerified {
		return items
	}
	known := map[core.RatingKey]bool{}
	for _, k := range s.verifiedKeys(ctx) {
		known[k] = true
	}
	out := append([]core.Title(nil), items...)
	sort.SliceStable(out, func(i, j int) bool { return known[out[i].Key] && !known[out[j].Key] })
	return out
}
func (s *Service) Hubs(ctx context.Context, limit int) []core.Hub {
	if !s.cfg().Availability.NewlyAvailable {
		return nil
	}
	keys := s.verifiedKeys(ctx)
	if len(keys) == 0 {
		return nil
	}
	items, err := s.d.Catalog.CachedTitles(ctx, keys)
	if err != nil {
		return nil
	}
	items = s.FilterDiscovery(ctx, items)
	limit = min(200, max(1, limit))
	movie := core.Hub{ID: "availability.movies", Title: "Newly available movies", Type: core.TypeMovie}
	tv := core.Hub{ID: "availability.episodes", Title: "Newly available episodes", Type: core.TypeEpisode}
	for _, t := range items {
		if !t.IsReleased(s.now()) {
			continue
		}
		switch t.Type {
		case core.TypeMovie:
			if len(movie.Items) < limit {
				movie.Items = append(movie.Items, t)
			}
		case core.TypeEpisode:
			if len(tv.Items) < limit {
				tv.Items = append(tv.Items, t)
			}
		}
	}
	var out []core.Hub
	if len(movie.Items) > 0 {
		out = append(out, movie)
	}
	if len(tv.Items) > 0 {
		out = append(out, tv)
	}
	return out
}
