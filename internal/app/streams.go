package app

import (
	"context"
	"io"
	"math"
	"strings"
	"time"

	"github.com/braedonsaunders/ether/internal/core"
	"github.com/braedonsaunders/ether/internal/markers"
	"github.com/braedonsaunders/ether/internal/plex"
	"github.com/braedonsaunders/ether/internal/subtitles"
)

// subtitleUser is u with the subtitle languages of the viewer's stream
// preferences (Viewers > Defaults or their override), which win over the
// account's own list.
func (a *App) subtitleUser(ctx context.Context, u *core.User) *core.User {
	if u == nil {
		return nil
	}
	copy := *u
	if a.trackprefs != nil {
		rules, err := a.trackprefs.GetRules(ctx, u.ID)
		if err == nil && len(rules.SubtitleLanguages) > 0 {
			copy.SubtitleLanguages = rules.SubtitleLanguages
		}
	}
	return &copy
}

// subtitlePrefs is the viewer's subtitle mode/style for automatic fetching.
func (a *App) subtitlePrefs(ctx context.Context, u *core.User) subtitles.Preferences {
	if u == nil || a.trackprefs == nil {
		return subtitles.Preferences{}
	}
	rules, err := a.trackprefs.GetRules(ctx, u.ID)
	if err != nil {
		return subtitles.Preferences{}
	}
	return subtitles.Preferences{Mode: rules.SubtitleMode, Style: rules.SubtitleStyle}
}

func (a *App) enrichSubtitles(ctx context.Context, t *core.Title, u *core.User, res *core.Resolution, src core.MediaSource, info *core.MediaInfo) {
	a.subtitles.Enrich(ctx, t, a.subtitleUser(ctx, u), res, src, info, a.subtitlePrefs(ctx, u))
}

// plexSubtitles is plex.Deps.Subtitles: the OpenSubtitles service with the
// viewer's preferred languages applied, as for playback enrichment.
type plexSubtitles struct{ a *App }

var (
	_ plex.SubtitleSource     = plexSubtitles{}
	_ plex.SubtitleCatalog    = plexSubtitles{}
	_ plex.SubtitlePrefetcher = plexSubtitles{}
	_ plex.SubtitleSearch     = plexSubtitles{}
	_ plex.SubtitleTimeline   = plexSubtitles{}
)

func (p plexSubtitles) OpenFormat(id, format string) (io.ReadCloser, string, error) {
	return p.a.subtitles.OpenFormat(id, format)
}

func (p plexSubtitles) OpenFormatAt(id, format string, offset time.Duration) (io.ReadCloser, string, error) {
	return p.a.subtitles.OpenFormatAt(id, format, offset)
}

func (p plexSubtitles) CachedSidecars(ctx context.Context, t *core.Title, u *core.User, embedded []core.StreamInfo) []core.StreamInfo {
	return p.a.subtitles.CachedSidecars(t, p.a.subtitleUser(ctx, u), embedded)
}

func (p plexSubtitles) PrefetchSidecars(ctx context.Context, t *core.Title, u *core.User) bool {
	return p.a.subtitles.PrefetchSidecars(t, p.a.subtitleUser(ctx, u), p.a.subtitlePrefs(ctx, u))
}

func (p plexSubtitles) SearchSubtitles(ctx context.Context, t *core.Title, _ *core.User, lang, release string) ([]plex.SubtitleOffer, error) {
	cands, err := p.a.subtitles.SearchOnDemand(ctx, t, lang, release)
	if err != nil {
		return nil, err
	}
	out := make([]plex.SubtitleOffer, 0, len(cands))
	for _, c := range cands {
		if c.Language != "" {
			lang = c.Language
		}
		title := c.Release
		if title == "" {
			title = strings.TrimSuffix(c.FileName, ".srt")
		}
		out = append(out, plex.SubtitleOffer{
			ID: c.FileID, Language: lang, Title: title, Provider: "OpenSubtitles",
			Score: int(math.Round(c.Score)), HearingImpaired: c.HearingImp, Forced: c.Forced,
			PerfectMatch: c.HashMatch || (release != "" && strings.EqualFold(c.Release, release)),
		})
	}
	return out, nil
}

func (p plexSubtitles) DownloadSubtitle(ctx context.Context, t *core.Title, _ *core.User, id int64) (core.StreamInfo, error) {
	return p.a.subtitles.DownloadOnDemand(ctx, t, id)
}

// markerDialogue is markers.Options.Dialogue: cue times from subtitle text
// the system already holds for the played release (cached sidecars), for
// stinger detection. It never fetches video or touches the network.
func (a *App) markerDialogue(_ context.Context, _ core.RatingKey, info core.MediaInfo) ([]markers.DialogueCue, bool) {
	cues, ok := a.subtitles.CuesForInfo(info)
	if !ok {
		return nil, false
	}
	out := make([]markers.DialogueCue, len(cues))
	for i, c := range cues {
		out[i] = markers.DialogueCue{Start: c.Start, End: c.End}
	}
	return out, true
}
