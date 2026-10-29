package subtitles

import (
	"context"
	"strings"

	"github.com/braedonsaunders/ether/internal/core"
)

// Enrich is playback.Deps.AugmentMedia. The source is exclusively owned by the
// preparing generation, so local hashing cannot race disposal during failover.
// Remote media uses release matching until a cache-only edge reader exists.
func (s *Service) Enrich(ctx context.Context, title *core.Title, user *core.User, res *core.Resolution, src core.MediaSource, info *core.MediaInfo, prefs ...Preferences) {
	if !s.Enabled() || info == nil || res == nil {
		return
	}
	release := res.Release.Name
	hash := ""
	if src != nil && strings.HasPrefix(res.Release.ID, "local:") {
		var err error
		hash, err = MovieHash(src, src.Size())
		if err != nil {
			s.log.Debug("local subtitle hash unavailable", "error", err)
		}
	}
	sidecars, err := s.SidecarsWithHash(ctx, title, user, info.Streams, release, hash, prefs...)
	if err != nil {
		s.log.Debug("subtitle enrichment unavailable", "error", err)
	}
	info.Streams = append(info.Streams, sidecars...)
	// A session is opening with known failover alternates: warm dialogue
	// alignments now so a later mid-stream failover (which only allows a
	// sub-second AlignTime) can land on the exact moment in the new cut.
	for i, alt := range res.Alternates {
		if i >= 3 {
			break
		}
		s.TimelineAligner().Prewarm(core.AlignRequest{
			Key: title.Key, From: res.Release, To: alt, FromDuration: info.Duration,
		})
	}
}
