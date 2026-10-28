package availability

import (
	"context"
	"github.com/braedonsaunders/ether/internal/core"
	"github.com/braedonsaunders/ether/internal/releaseparse"
	"strings"
	"time"
)

// EvidenceWindow is a proof window, not a delay: an eligible observed release
// unlocks a title immediately. Older catalogue entries do not need a scan.
func EvidenceWindow(t core.Title, now time.Time) bool {
	if t.Released.IsZero() || t.Key.Kind() == core.KindLocal {
		return false
	}
	age := now.Sub(t.Released)
	switch t.Type {
	case core.TypeMovie:
		return age < 60*24*time.Hour
	case core.TypeEpisode, core.TypeShow:
		return age < 24*time.Hour
	}
	return false
}

// FilterDiscovery reads local observations only. A feed listing establishes
// release existence, not guaranteed playback; playback still checks the source.
func (s *Service) filterReleaseDiscovery(ctx context.Context, items []core.Title) []core.Title {
	titles := map[core.RatingKey]*core.Title{}
	keys := []core.RatingKey{}
	for i := range items {
		t := &items[i]
		if EvidenceWindow(*t, s.now()) && titles[t.Key] == nil {
			keys = append(keys, t.Key)
			titles[t.Key] = t
		}
	}
	if len(keys) == 0 {
		return items
	}
	feeds := map[string]core.ReleaseFeed{}
	for _, f := range s.d.Feeds.FeedSources() {
		feeds[f.ID] = f
	}
	s.attachRetained(ctx, titles, feeds)
	unavailable := map[core.RatingKey]time.Time{}
	if source, ok := s.d.Validator.(interface {
		KnownUnavailableSince(context.Context, []core.RatingKey) map[core.RatingKey]time.Time
	}); ok {
		unavailable = source.KnownUnavailableSince(ctx, keys)
	}
	acceptable := map[core.RatingKey]bool{}
	type observation struct {
		key                                core.RatingKey
		source, generation, payload, state string
		firstSeen                          int64
		revision                           int64
	}
	var observations []observation
	for start := 0; start < len(keys); start += 500 {
		batch := keys[start:min(start+500, len(keys))]
		marks := make([]string, len(batch))
		args := make([]any, 0, len(batch)+1)
		for i, k := range batch {
			marks[i] = "?"
			args = append(args, k)
		}
		args = append(args, s.now().Add(-61*24*time.Hour).Unix())
		rows, err := s.db.QueryContext(ctx, `SELECT rating_key,source_id,generation,payload,state,first_seen,revision FROM availability_releases WHERE rating_key IN (`+strings.Join(marks, ",")+`) AND last_seen>=?`, args...)
		if err != nil {
			continue
		}
		for rows.Next() {
			var o observation
			if rows.Scan(&o.key, &o.source, &o.generation, &o.payload, &o.state, &o.firstSeen, &o.revision) == nil {
				observations = append(observations, o)
			}
		}
		if err := rows.Err(); err != nil {
			s.log.Warn("availability observations not fully loaded", "err", err)
		}
		rows.Close()
	}
	// Close the cursor before policy checks, which may read the same database.
	for _, o := range observations {
		if acceptable[o.key] || (o.state == "unknown" && o.revision > 0) || o.state == "unavailable" {
			continue
		}
		f, ok := feeds[o.source]
		if !ok || f.Generation != o.generation || !s.usable(f.Protocol) {
			continue
		}
		if since, ok := unavailable[o.key]; ok && o.firstSeen <= since.Unix() {
			continue
		}
		r, err := s.decode(o.payload)
		if err != nil {
			continue
		}
		r.Info = releaseparse.Parse(r.Name)
		_, reason, err := s.d.Validator.FeedCandidate(ctx, titles[o.key], r)
		if err == nil && reason == "" {
			acceptable[o.key] = true
		}
	}
	out := make([]core.Title, 0, len(items))
	for _, t := range items {
		if titles[t.Key] == nil || (!t.Released.After(s.now()) && acceptable[t.Key]) {
			out = append(out, t)
		}
	}
	return out
}

// HiddenMovies supplies SQL pagination with exclusions from the recent local
// mirror only. No per-title provider lookup or historical catalogue walk occurs.
func (s *Service) HiddenMovies(ctx context.Context) []core.RatingKey {
	c, ok := s.d.Catalog.(interface {
		RecentMovies(context.Context, time.Time) ([]core.Title, error)
	})
	if !ok {
		return nil
	}
	items, err := c.RecentMovies(ctx, s.now().Add(-60*24*time.Hour))
	if err != nil {
		return nil
	}
	visible := map[core.RatingKey]bool{}
	for _, t := range s.FilterDiscovery(ctx, items) {
		visible[t.Key] = true
	}
	var hidden []core.RatingKey
	for _, t := range items {
		if !visible[t.Key] {
			hidden = append(hidden, t.Key)
		}
	}
	return hidden
}

// HiddenShows excludes first-day series before SQL pagination, just as movies.
func (s *Service) HiddenShows(ctx context.Context) []core.RatingKey {
	c, ok := s.d.Catalog.(interface {
		RecentShows(context.Context, time.Time) ([]core.Title, error)
	})
	if !ok {
		return nil
	}
	items, err := c.RecentShows(ctx, s.now().Add(-24*time.Hour))
	if err != nil {
		return nil
	}
	visible := map[core.RatingKey]bool{}
	for _, t := range s.FilterDiscovery(ctx, items) {
		visible[t.Key] = true
	}
	var hidden []core.RatingKey
	for _, t := range items {
		if !visible[t.Key] {
			hidden = append(hidden, t.Key)
		}
	}
	return hidden
}
