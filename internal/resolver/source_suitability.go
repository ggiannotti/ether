package resolver

import (
	"context"
	"strings"
	"time"

	"github.com/braedonsaunders/ether/internal/config"
	"github.com/braedonsaunders/ether/internal/core"
	"github.com/braedonsaunders/ether/internal/releaseparse"
)

// DiscoveryCandidate is local, name-based negative evidence only. It does not
// certify quality or fetch a provider. Feed and playback share this policy.
func (s *Service) DiscoveryCandidate(t *core.Title, r core.Release) string {
	return sourceSuitability(r, t, s.ProfileFor(core.ClientCaps{HDR10: true, DolbyVision: true}, nil), s.resolverCfg(), false)
}

func sourceSuitability(r core.Release, t *core.Title, p core.QualityProfile, c config.Resolver, explicit bool) string {
	// Reparse the name: saved feed payloads can contain older parser results.
	info := releaseparse.Parse(r.Name)
	if (info.Source == core.SourceCam || r.Info.Source == core.SourceCam) && !(c.AllowCam && p.AllowCam) {
		return "cam not allowed"
	}
	if explicit || !releaseparse.ExplicitDub(r.Name) || len(info.Languages) == 0 {
		return ""
	}
	want := append([]string(nil), p.PreferredLanguages...)
	if t != nil && t.Language != "" {
		want = append(want, assumedLanguage(t))
	}
	if len(want) == 0 {
		return ""
	}
	for _, lang := range info.Languages {
		if lang == "multi" {
			return ""
		}
		for _, pref := range want {
			if strings.EqualFold(lang, assumedLanguage(&core.Title{Language: pref})) {
				return ""
			}
		}
	}
	return "dub audio does not match the original or preferred languages"
}

// KnownUnavailable returns only unexpired resolver negatives, in one local
// batch. It never searches, probes, or turns missing evidence into a negative.
func (s *Service) KnownUnavailable(ctx context.Context, keys []core.RatingKey) map[core.RatingKey]bool {
	out := map[core.RatingKey]bool{}
	for k := range s.KnownUnavailableSince(ctx, keys) {
		out[k] = true
	}
	return out
}
func (s *Service) KnownUnavailableSince(ctx context.Context, keys []core.RatingKey) map[core.RatingKey]time.Time {
	out := map[core.RatingKey]time.Time{}
	now := s.now()
	if s.db != nil {
		for start := 0; start < len(keys); start += 500 {
			batch := keys[start:min(start+500, len(keys))]
			marks := make([]string, len(batch))
			args := make([]any, 0, len(batch)+1)
			for i, k := range batch {
				marks[i] = "?"
				args = append(args, k)
			}
			args = append(args, now.Unix())
			rows, err := s.db.QueryContext(ctx, `SELECT rating_key,checked_at FROM resolver_availability WHERE rating_key IN (`+strings.Join(marks, ",")+`) AND available=0 AND expires_at>?`, args...)
			if err != nil {
				continue
			}
			for rows.Next() {
				var k core.RatingKey
				var checked int64
				if rows.Scan(&k, &checked) == nil {
					out[k] = time.Unix(checked, 0)
				}
			}
			if err := rows.Err(); err != nil {
				s.log.Debug("resolver availability lookup failed", "err", err)
			}
			rows.Close()
		}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, k := range keys {
		if e, ok := s.avail[int64(k)]; ok {
			if !e.available && now.Before(e.expires) {
				out[k] = e.checked
			} else {
				delete(out, k)
			}
		}
	}
	return out
}
