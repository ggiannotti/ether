package availability

import (
	"context"
	"github.com/braedonsaunders/ether/internal/core"
	"github.com/braedonsaunders/ether/internal/releaseparse"
	"strings"
	"time"
)

// RecordSearch retains descriptors from an actual requested search. It does not
// initiate searches, grab media or claim health. Policy is reevaluated on reads.
func (s *Service) RecordSearch(ctx context.Context, t *core.Title, rs []core.Release) {
	if t == nil || !EvidenceWindow(*t, s.now()) || !t.IsReleased(s.now()) {
		return
	}
	feeds := map[string]core.ReleaseFeed{}
	for _, f := range s.d.Feeds.FeedSources() {
		feeds[f.Name] = f
	}
	policy := s.policy()
	for _, r := range rs {
		if ctx.Err() != nil {
			return
		}
		f, ok := feeds[r.Indexer]
		if !ok || r.ID == "" {
			continue
		}
		if err := s.remember(ctx, f, *t, r, policy); err != nil {
			return
		}
	}
}

func (s *Service) seedCached(ctx context.Context) error {
	source, ok := s.d.Feeds.(interface {
		CachedReleases(context.Context) ([]core.Release, error)
	})
	if !ok {
		return nil
	}
	rs, err := source.CachedReleases(ctx)
	if err != nil {
		return err
	}
	if len(rs) == 0 {
		return nil
	}
	for i := range rs {
		rs[i].Info = releaseparse.Parse(rs[i].Name)
	}
	feeds := map[string]core.ReleaseFeed{}
	for _, f := range s.d.Feeds.FeedSources() {
		feeds[f.Name] = f
	}
	bySource := map[string][]core.Release{}
	for _, r := range rs {
		bySource[r.Indexer] = append(bySource[r.Indexer], r)
	}
	for name, batch := range bySource {
		if f, ok := feeds[name]; ok {
			if err := s.retainFeed(ctx, f, batch); err != nil {
				return err
			}
		}
	}
	matches, err := s.d.Catalog.ReleaseMatches(ctx, rs)
	if err != nil {
		return err
	}
	// One local catalogue match pass, no search for the old library or missing items.
	for _, r := range rs {
		for _, t := range matches[r.ID] {
			s.RecordSearch(ctx, &t, []core.Release{r})
		}
	}
	return ctx.Err()
}

// Preserve unmatched names as well: an episode may enter the local catalogue
// after its upload leaves the feed's first page. Matching later is indexed and
// local; it never fetches every episode of a show.
func (s *Service) retainFeed(ctx context.Context, f core.ReleaseFeed, rs []core.Release) error {
	defer s.clearVisibility()
	for _, r := range rs {
		if r.ID == "" {
			continue
		}
		name := releaseparse.NormalizeTitle(releaseparse.Parse(r.Name).Title)
		if name == "" {
			continue
		}
		raw, err := s.encode(r)
		if err != nil {
			return err
		}
		_, err = s.db.ExecContext(ctx, `INSERT INTO availability_feed_items(id,source_id,generation,match_title,payload,seen_at) VALUES(?,?,?,?,?,?) ON CONFLICT(id,source_id) DO UPDATE SET generation=excluded.generation,match_title=excluded.match_title,payload=excluded.payload,seen_at=excluded.seen_at`, r.ID, f.ID, f.Generation, name, raw, s.now().Unix())
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) attachRetained(ctx context.Context, titles map[core.RatingKey]*core.Title, feeds map[string]core.ReleaseFeed) {
	byName := map[string][]*core.Title{}
	for _, t := range titles {
		names := []string{t.Title, t.OriginalTitle}
		if t.Type == core.TypeEpisode {
			names = []string{t.ShowTitle, t.ShowOriginalTitle}
		}
		for _, name := range names {
			n := releaseparse.NormalizeTitle(name)
			if n != "" {
				byName[n] = append(byName[n], t)
			}
		}
	}
	names := make([]string, 0, len(byName))
	for n := range byName {
		names = append(names, n)
	}
	type pending struct {
		source, gen, raw string
		seen             int64
	}
	var work []pending
	for start := 0; start < len(names); start += 500 {
		ns := names[start:min(start+500, len(names))]
		args := make([]any, 0, len(ns)+1)
		marks := make([]string, len(ns))
		for i, n := range ns {
			marks[i] = "?"
			args = append(args, n)
		}
		args = append(args, s.now().Add(-61*24*time.Hour).Unix())
		rows, err := s.db.QueryContext(ctx, `SELECT source_id,generation,payload,seen_at FROM availability_feed_items WHERE match_title IN (`+strings.Join(marks, ",")+`) AND seen_at>=? ORDER BY seen_at DESC LIMIT 2000`, args...)
		if err != nil {
			continue
		}
		for rows.Next() {
			var p pending
			if rows.Scan(&p.source, &p.gen, &p.raw, &p.seen) == nil {
				work = append(work, p)
			}
		}
		if err := rows.Err(); err != nil {
			s.log.Warn("availability retained releases not fully loaded", "err", err)
		}
		rows.Close()
	}
	policy := s.policy()
	for _, p := range work {
		f, ok := feeds[p.source]
		if !ok || f.Generation != p.gen || !s.usable(f.Protocol) {
			continue
		}
		r, err := s.decode(p.raw)
		if err != nil {
			continue
		}
		r.Info = releaseparse.Parse(r.Name)
		for _, t := range byName[releaseparse.NormalizeTitle(r.Info.Title)] {
			if t.Type == core.TypeEpisode && !releaseparse.EpisodeMatches(r.Info, t.Season, t.Episode) {
				season := false
				for _, n := range r.Info.Seasons {
					if n == t.Season {
						season = true
					}
				}
				if !r.Info.FullSeason || !season {
					continue
				}
			}
			if t.Type == core.TypeMovie && (len(r.Info.Seasons) > 0 || len(r.Info.Episodes) > 0) {
				continue
			}
			if _, reason, err := s.d.Validator.FeedCandidate(ctx, t, r); err != nil || reason != "" {
				continue
			}
			// Same-generation observations preserve invalidation and their original date.
			_, _ = s.db.ExecContext(ctx, `INSERT INTO availability_releases(id,rating_key,source_id,generation,payload,title,name,protocol,first_seen,last_seen,priority,policy) VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id,rating_key,source_id) DO UPDATE SET generation=excluded.generation,payload=excluded.payload,first_seen=excluded.first_seen,last_seen=excluded.last_seen,state='observed',verified_at=0,first_verified=0,next_check=0,revision=availability_releases.revision+1 WHERE availability_releases.generation<>excluded.generation`, r.ID, t.Key, f.ID, f.Generation, p.raw, t.Title, r.Name, r.Protocol, p.seen, p.seen, t.Popularity, policy)
		}
	}
}
