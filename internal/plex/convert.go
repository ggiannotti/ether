package plex

import (
	"context"
	"fmt"
	"hash/fnv"
	"math"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/braedonsaunders/ether/internal/core"
	"github.com/braedonsaunders/ether/internal/langtag"
	"github.com/braedonsaunders/ether/internal/plex/model"
)

// renderOpts controls how much of an item is rendered.
type renderOpts struct {
	detail bool // full tags, Guid children, streams
	media  bool // Media/Part for playable items
	// inSection: the container already carries the section attributes
	// (section listings, children), so items omit librarySection*.
	inSection bool
	// detailEpisodes renders episodes in full (hub rows, like PMS).
	detailEpisodes bool
	// guids adds the external Guid children (related hubs, like PMS).
	guids bool
	// state holds the user's watch status for the rendered keys.
	state map[core.RatingKey]core.WatchStatus
}

// metadataKey is the item's "key": children list for containers, the item
// itself for playables.
func metadataKey(k core.RatingKey, t core.MediaType) string {
	switch t {
	case core.TypeShow, core.TypeSeason:
		return "/library/metadata/" + k.String() + "/children"
	}
	return "/library/metadata/" + k.String()
}

func isPlayable(t core.MediaType) bool { return t == core.TypeMovie || t == core.TypeEpisode }

func unixOrZero(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.Unix()
}

func ms(d time.Duration) int64 { return d.Milliseconds() }

// guidFor returns the primary guid attribute. Ether has no plex:// guids, so
// it uses the legacy TMDB agent form, which every client understands as a
// legacy-agent item.
func guidFor(t *core.Title) string {
	switch t.Key.Kind() {
	case core.KindLocal:
		h := fnv.New64a()
		fmt.Fprintf(h, "%d", int64(t.Key))
		return fmt.Sprintf("com.plexapp.agents.none://%016x?lang=xn", h.Sum64())
	}
	switch t.Type {
	case core.TypeMovie:
		if t.IDs.TMDB > 0 {
			return fmt.Sprintf("com.plexapp.agents.themoviedb://%d?lang=en", t.IDs.TMDB)
		}
	case core.TypeShow:
		if t.IDs.TMDB > 0 {
			return fmt.Sprintf("com.plexapp.agents.themoviedb://%d?lang=en", t.IDs.TMDB)
		}
	case core.TypeSeason:
		return fmt.Sprintf("com.plexapp.agents.themoviedb://%d/%d?lang=en", t.Key.TMDB(), t.Season)
	case core.TypeEpisode:
		return fmt.Sprintf("com.plexapp.agents.themoviedb://%d/%d/%d?lang=en", t.Key.TMDB(), t.Season, t.Episode)
	}
	return "com.plexapp.agents.none://" + t.Key.String() + "?lang=xn"
}

func showGUID(show core.RatingKey) string {
	return fmt.Sprintf("com.plexapp.agents.themoviedb://%d?lang=en", show.TMDB())
}

// externalGuids are the <Guid id="..."/> children.
func externalGuids(t *core.Title) []model.GUID {
	var out []model.GUID
	if t.IDs.IMDB != "" {
		out = append(out, model.GUID{ID: "imdb://" + t.IDs.IMDB})
	}
	if t.IDs.TMDB > 0 {
		out = append(out, model.GUID{ID: "tmdb://" + strconv.FormatInt(t.IDs.TMDB, 10)})
	}
	if t.IDs.TVDB > 0 {
		out = append(out, model.GUID{ID: "tvdb://" + strconv.FormatInt(t.IDs.TVDB, 10)})
	}
	return out
}

func tagID(kind, name string) int64 {
	h := fnv.New32a()
	h.Write([]byte(kind + ":" + strings.ToLower(name)))
	return int64(h.Sum32()&0x7fffffff) + 1
}

func imageStamp(t *core.Title) string {
	if ts := unixOrZero(t.UpdatedAt); ts > 0 {
		return strconv.FormatInt(ts, 10)
	}
	return "1"
}

// thumbURL / artURL are our own artwork endpoints for an item.
func thumbURL(t *core.Title) string {
	if t.PosterPath == "" && t.StillPath == "" {
		return ""
	}
	return "/library/metadata/" + t.Key.String() + "/thumb/" + imageStamp(t)
}

func artURL(t *core.Title) string {
	if t.BackdropPath == "" {
		return ""
	}
	return "/library/metadata/" + t.Key.String() + "/art/" + imageStamp(t)
}

func seasonTitle(t *core.Title) string {
	if t.SeasonTitle != "" {
		return t.SeasonTitle
	}
	if t.Season == 0 {
		return "Specials"
	}
	return "Season " + strconv.Itoa(t.Season)
}

// metadata renders one title.
func (s *Server) metadata(ctx context.Context, pr *Request, t *core.Title, o renderOpts) model.Metadata {
	t = s.enrich(t)
	show := s.showOf(t)
	m := model.Metadata{
		RatingKey:     t.Key.String(),
		Key:           metadataKey(t.Key, t.Type),
		GUID:          guidFor(t),
		Type:          string(t.Type),
		Title:         t.Title,
		Summary:       t.Summary,
		Tagline:       t.Tagline,
		Year:          t.Year,
		ContentRating: t.ContentRating,
		Thumb:         thumbURL(t),
		Art:           artURL(t),
		AddedAt:       unixOrZero(t.UpdatedAt),
		UpdatedAt:     unixOrZero(t.UpdatedAt),
	}
	if t.SortTitle != "" && t.SortTitle != t.Title {
		m.TitleSort = t.SortTitle
	}
	if t.OriginalTitle != "" && t.OriginalTitle != t.Title {
		m.OriginalTitle = t.OriginalTitle
	}
	if !t.Released.IsZero() {
		m.OriginallyAvailableAt = t.Released.Format("2006-01-02")
		if m.AddedAt == 0 {
			m.AddedAt = t.Released.Unix()
		}
		if m.Year == 0 {
			m.Year = t.Released.Year()
		}
	}
	if t.Runtime > 0 {
		m.Duration = ms(t.Runtime)
	}
	if t.Rating > 0 {
		m.AudienceRating = math.Round(t.Rating*10) / 10
		m.AudienceRatingImage = "themoviedb://image.rating"
	}
	if len(t.Studios) > 0 {
		m.Studio = t.Studios[0]
	}
	if sec, ok := s.sectionFor(t.Type); ok && !o.inSection && t.Key.Kind() != core.KindCollection {
		m.LibrarySectionID = sec.ID
		m.LibrarySectionKey = "/library/sections/" + strconv.Itoa(sec.ID)
		m.LibrarySectionTitle = sec.Title
	}

	switch t.Type {
	case core.TypeMovie:
		m.Slug = slugify(t.Title)
	case core.TypeShow:
		m.Slug = slugify(t.Title)
		m.Index = model.Ptr(1)
		if t.ChildCount > 0 {
			m.ChildCount = model.Ptr(t.ChildCount)
		}
		m.LeafCount = model.Ptr(t.LeafCount)
		m.ViewedLeafCount = model.Ptr(0)
	case core.TypeSeason:
		// PMS seasons carry the show's slug/studio/year as parent*, and no
		// dates, ratings, sort title or genre/country tags of their own.
		m.Index = model.Ptr(t.Season)
		if t.Title == "" {
			m.Title = seasonTitle(t)
		}
		showKey := t.ShowKey
		if showKey == 0 {
			showKey = t.Key.Parent()
		}
		m.ParentRatingKey = showKey.String()
		m.ParentGUID = showGUID(showKey)
		m.ParentKey = "/library/metadata/" + showKey.String()
		m.ParentTitle = t.ShowTitle
		m.ParentSlug = slugify(t.ShowTitle)
		m.ParentIndex = model.Ptr(1)
		m.ParentThumb = "/library/metadata/" + showKey.String() + "/thumb/" + imageStamp(t)
		if m.Art == "" {
			m.Art = "/library/metadata/" + showKey.String() + "/art/" + imageStamp(t)
		}
		if m.Thumb == "" {
			m.Thumb = m.ParentThumb
		}
		if show != nil {
			if len(show.Studios) > 0 {
				m.ParentStudio = show.Studios[0]
			}
			m.ParentYear = show.Year
			if m.ParentTitle == "" {
				m.ParentTitle = show.Title
				m.ParentSlug = slugify(show.Title)
			}
		}
		m.LeafCount = model.Ptr(t.ChildCount)
		m.ViewedLeafCount = model.Ptr(0)
		m.Year, m.OriginallyAvailableAt, m.TitleSort = 0, "", ""
		m.ContentRating, m.AudienceRating, m.AudienceRatingImage = "", 0, ""
		m.Studio = ""
	case core.TypeEpisode:
		// PMS episodes have no year, sort title or genre/country tags; the
		// show's slug and content rating are carried over.
		m.Index = model.Ptr(t.Episode)
		m.ParentIndex = model.Ptr(t.Season)
		season := t.Key.Parent()
		showKey := t.ShowKey
		if showKey == 0 {
			showKey = season.Parent()
		}
		m.ParentRatingKey = season.String()
		m.ParentGUID = fmt.Sprintf("com.plexapp.agents.themoviedb://%d/%d?lang=en", showKey.TMDB(), t.Season)
		m.GrandparentGUID = showGUID(showKey)
		m.ParentKey = "/library/metadata/" + season.String()
		m.ParentTitle = seasonTitle(t)
		m.ParentThumb = "/library/metadata/" + season.String() + "/thumb/" + imageStamp(t)
		m.GrandparentRatingKey = showKey.String()
		m.GrandparentKey = "/library/metadata/" + showKey.String()
		m.GrandparentTitle = t.ShowTitle
		m.GrandparentThumb = "/library/metadata/" + showKey.String() + "/thumb/" + imageStamp(t)
		m.GrandparentArt = "/library/metadata/" + showKey.String() + "/art/" + imageStamp(t)
		if m.Art == "" {
			m.Art = m.GrandparentArt
		}
		if show != nil {
			if m.GrandparentTitle == "" {
				m.GrandparentTitle = show.Title
			}
			if m.ContentRating == "" {
				m.ContentRating = show.ContentRating
			}
		}
		m.GrandparentSlug = slugify(m.GrandparentTitle)
		m.Year, m.TitleSort, m.Studio = 0, "", ""
	}

	// Per-user state.
	if st, ok := o.state[t.Key]; ok {
		m.ViewCount = st.ViewCount
		if st.ViewOffset > 0 && isPlayable(t.Type) {
			m.ViewOffset = ms(st.ViewOffset)
		}
		m.LastViewedAt = unixOrZero(st.LastViewedAt)
		if st.UserRating > 0 {
			m.UserRating = st.UserRating
			m.LastRatedAt = unixOrZero(st.UpdatedAt)
		}
	}

	// Tags. PMS renders episodes inside hubs in full (tag ids, Guid,
	// streams), everything else in lists with three plain tags of a kind.
	detail := o.detail || (o.detailEpisodes && t.Type == core.TypeEpisode)
	maxTags := 3
	if detail {
		maxTags = math.MaxInt
	}
	genres, countries := t.Genres, t.Countries
	if t.Type == core.TypeSeason || t.Type == core.TypeEpisode {
		genres, countries = nil, nil
	}
	for i, g := range genres {
		if i >= maxTags {
			break
		}
		tag := model.Tag{Tag: g}
		if detail {
			tag.ID = tagID("genre", g)
			tag.Filter = "genre=" + url.QueryEscape(g)
		}
		m.Genre = append(m.Genre, tag)
	}
	for i, c := range countries {
		if i >= maxTags {
			break
		}
		tag := model.Tag{Tag: c}
		if detail {
			tag.ID = tagID("country", c)
			tag.Filter = "country=" + url.QueryEscape(c)
		}
		m.Country = append(m.Country, tag)
	}
	if t.Type != core.TypeShow || detail {
		// Show tiles in PMS lists carry Role tags only.
		m.Director = s.personTags(t.Directors, "director", maxTags, detail, false)
		m.Writer = s.personTags(t.Writers, "writer", maxTags, detail, false)
	}
	m.Role = s.personTags(t.Cast, "actor", maxTags, detail, true)
	if t.CollectionID > 0 && t.CollectionName != "" {
		tag := model.Tag{Tag: t.CollectionName}
		if detail {
			tag.ID = t.CollectionID
			tag.Filter = "collection=" + strconv.FormatInt(t.CollectionID, 10)
		}
		m.Collection = []model.Tag{tag}
	}
	if detail {
		m.Guid = externalGuids(t)
	} else if o.guids {
		m.GuidLead = externalGuids(t)
	}
	if detail {
		if t.Rating > 0 {
			m.Rating_ = []model.RatingTag{{Image: "themoviedb://image.rating", Value: math.Round(t.Rating*10) / 10, Type: "audience"}}
		}
	}
	// PMS sends Image (and, for movies and shows, UltraBlurColors) on
	// every item, in listings and hubs as well as detail pages.
	m.Image = s.imageTags(t, show)
	if ultraBlurType(t.Type) {
		m.UltraBlurColors = s.ultraBlurFor(t.Key, t.BackdropPath)
	}

	if o.media && isPlayable(t.Type) {
		var editionTitle string
		m.Media, editionTitle = s.mediaFor(ctx, pr, t, detail, o.detail)
		applyEditionTitle(&m, editionTitle)
		if s.downloadsAllowed(pr.User) {
			m.AllowSync = model.Ptr(true)
		}
		if detail {
			s.addStreamFeatures(ctx, pr, t, &m)
		}
		if m.Duration == 0 && len(m.Media) > 0 {
			m.Duration = m.Media[0].Duration
		}
	}
	s.decorateDiscoveryMetadata(ctx, pr, t, &m)
	return m
}

// imageTags are an item's <Image> children, composed like PMS's:
//
//	movie, show: coverPoster, background, clearLogo
//	season:      coverPoster (season, else show), background (show), clearLogo (show)
//	episode:     coverPoster (show), snapshot (still), background (show), clearLogo (show)
//
// Only artwork Ether can serve is listed; the show's clear logo needs the
// show's detail (show), which may not be cached on the first render.
func (s *Server) imageTags(t *core.Title, show *core.Title) []model.Image {
	alt := t.Title
	if alt == "" && t.Type == core.TypeSeason {
		alt = seasonTitle(t)
	}
	img := func(typ, url string) model.Image { return model.Image{Alt: alt, Type: typ, URL: url} }
	stamp := imageStamp(t)
	var out []model.Image
	switch t.Type {
	case core.TypeSeason, core.TypeEpisode:
		showKey := t.ShowKey
		if showKey == 0 {
			showKey = t.Key.Parent()
			if t.Type == core.TypeEpisode {
				showKey = showKey.Parent()
			}
		}
		showPath := "/library/metadata/" + showKey.String()
		if t.Type == core.TypeSeason {
			if th := thumbURL(t); th != "" {
				out = append(out, img("coverPoster", th))
			} else {
				out = append(out, img("coverPoster", showPath+"/thumb/"+stamp))
			}
		} else {
			out = append(out, img("coverPoster", showPath+"/thumb/"+stamp))
			if th := thumbURL(t); th != "" {
				out = append(out, img("snapshot", th))
			}
		}
		if a := artURL(t); a != "" {
			out = append(out, img("background", a))
		} else {
			out = append(out, img("background", showPath+"/art/"+stamp))
		}
		if show != nil && show.LogoPath != "" {
			out = append(out, img("clearLogo", showPath+"/clearLogo/"+stamp))
		}
	default:
		if th := thumbURL(t); th != "" {
			out = append(out, img("coverPoster", th))
		}
		if a := artURL(t); a != "" {
			out = append(out, img("background", a))
		}
		if t.LogoPath != "" {
			out = append(out, img("clearLogo", "/library/metadata/"+t.Key.String()+"/clearLogo/"+stamp))
		}
	}
	return out
}

func (s *Server) personTags(ps []core.Person, kind string, max int, detail, withRole bool) []model.Tag {
	var out []model.Tag
	for i, p := range ps {
		if i >= max {
			break
		}
		tag := model.Tag{Tag: p.Name}
		if detail {
			if p.TMDB > 0 {
				tag.ID = p.TMDB
				tag.Filter = kind + "=" + strconv.FormatInt(p.TMDB, 10)
				tag.TagKey = "tmdb-person-" + strconv.FormatInt(p.TMDB, 10)
			} else {
				tag.ID = tagID(kind, p.Name)
			}
			if withRole {
				tag.Role = p.Role
			}
			if p.ThumbPath != "" {
				tag.Thumb = s.d.ImageURL(p.ThumbPath, "w185")
			}
		}
		out = append(out, tag)
	}
	return out
}

// mediaInfoFor is the media info shown for a title: the live session's when
// one exists for this client, otherwise the playback estimate.
func (s *Server) mediaInfoFor(ctx context.Context, pr *Request, t *core.Title) (core.MediaInfo, *core.PlaySession) {
	if s.d.Playback == nil {
		return core.MediaInfo{Estimated: true, Duration: t.Runtime}, nil
	}
	if pr.User != nil {
		if ps, ok := s.d.Playback.SessionFor(pr.User.ID, pr.ClientID, t.Key); ok && ps != nil {
			return ps.Info, ps
		}
	}
	caps := pr.ClientCaps(s.cfg().Transcode.RemoteMaxBitrateKbps)
	return s.d.Playback.Estimate(ctx, t, caps), nil
}

// mediaFor renders the virtual Media/Part of a playable title: one per
// release variant on detail pages (withVariants, driving the "Play
// Version" picker), otherwise the single Media. The second return value
// is the item's editionTitle, or "". A live session keeps describing the
// version actually playing (its probed info), so variants are only
// offered before playback starts for this client.
func (s *Server) mediaFor(ctx context.Context, pr *Request, t *core.Title, withStreams bool, withVariants bool) ([]model.Media, string) {
	info, ps := s.mediaInfoFor(ctx, pr, t)
	stamp := unixOrZero(t.UpdatedAt)
	if ps != nil && !ps.StartedAt.IsZero() {
		stamp = ps.StartedAt.Unix()
	}
	if stamp <= 0 {
		stamp = 1
	}
	if withVariants && withStreams && ps == nil {
		if media, edition := s.mediaForVariants(ctx, pr, t, stamp); len(media) > 0 {
			return media, edition
		}
	}
	m := buildMedia(t, info, stamp)
	if ps != nil && ps.Resolution != nil && s.d.Trickplay != nil && s.d.Trickplay.TrickplayAvailable(ps.Resolution.Release.ID) {
		m.Part[0].Indexes = "sd"
	}
	if withStreams {
		// Before a session the info is an estimate without subtitle
		// tracks: list the viewer's cached sidecars (OpenSubtitles) so
		// detail pages and play queues offer them.
		info = s.withCachedSidecars(ctx, pr.User, t, info)
		var userID int64
		if pr.User != nil {
			userID = pr.User.ID
		}
		sel := resolveSelection(s.selections.get(userID, int64(t.Key)), info)
		sel = s.preferredSelection(ctx, pr.User, t.Key, info, t.Language, sel)
		m.Part[0].Stream = s.buildStreams(t.Key, info, sel, pr.User)
	}
	return []model.Media{m}, ""
}

// partFileName is the virtual file name shown in "Get Info".
func partFileName(t *core.Title, ext string) string {
	name := t.Title
	switch t.Type {
	case core.TypeEpisode:
		name = fmt.Sprintf("%s - s%02de%02d - %s", t.ShowTitle, t.Season, t.Episode, t.Title)
	default:
		if t.Year > 0 {
			name = fmt.Sprintf("%s (%d)", t.Title, t.Year)
		}
	}
	name = strings.Map(func(r rune) rune {
		if strings.ContainsRune(`/\:*?"<>|`, r) {
			return -1
		}
		return r
	}, name)
	return "/ether/" + name + "." + ext
}

func buildMedia(t *core.Title, info core.MediaInfo, stamp int64) model.Media {
	container := plexContainer(info.Container)
	ext := containerExt(container)
	dur := ms(info.Duration)
	if dur <= 0 {
		dur = ms(t.Runtime)
	}
	id := int64(t.Key)
	m := model.Media{
		ID:        id,
		Duration:  dur,
		Bitrate:   info.BitrateKbps,
		Container: container,
	}
	// PMS only reports the MP4 atoms for MP4/MOV files (an int on Media,
	// a bool on Part).
	isMP4 := container == "mp4" || container == "mov"
	if isMP4 {
		m.OptimizedForStreaming = model.Ptr(1)
		m.Has64bitOffsets = model.Ptr(false)
	}
	var firstAudio *core.StreamInfo
	for i := range info.Streams {
		if info.Streams[i].Type == "audio" {
			if firstAudio == nil || info.Streams[i].Default && !firstAudio.Default {
				firstAudio = &info.Streams[i]
			}
		}
	}
	if v := info.Video(); v != nil {
		m.Width, m.Height = v.Width, v.Height
		if v.Height > 0 {
			m.AspectRatio = math.Round(float64(v.Width)/float64(v.Height)*100) / 100
		}
		m.VideoCodec = plexCodec(v.Codec)
		m.VideoResolution = videoResolution(v.Width, v.Height)
		m.VideoFrameRate = frameRateName(v.FrameRate)
		m.VideoProfile = strings.ToLower(v.Profile)
	}
	if firstAudio != nil {
		m.AudioChannels = firstAudio.Channels
		m.AudioCodec = plexCodec(firstAudio.Codec)
		m.AudioProfile = audioProfile(firstAudio)
	}
	m.HasVoiceActivity = model.Ptr(false)
	p := model.Part{
		ID:           id,
		Key:          fmt.Sprintf("/library/parts/%d/%d/file.%s", id, stamp, ext),
		Duration:     dur,
		File:         partFileName(t, ext),
		Size:         info.Size,
		AudioProfile: m.AudioProfile,
		Container:    container,
		VideoProfile: m.VideoProfile,
	}
	if isMP4 {
		p.Has64bitOffsets = model.Ptr(false)
		p.OptimizedForStreaming = model.Ptr(true)
	}
	m.Part = []model.Part{p}
	return m
}

// audioProfile is PMS's audioProfile ("lc" for AAC, "dts"/"ma" for DTS...).
func audioProfile(st *core.StreamInfo) string {
	if st.Profile != "" {
		p := strings.ToLower(st.Profile)
		switch p {
		case "dts-hd ma":
			return "ma"
		case "dts-hd hra":
			return "hra"
		}
		return p
	}
	switch strings.ToLower(st.Codec) {
	case "aac":
		return "lc"
	case "dts":
		return "dts"
	case "dtshd":
		return "ma"
	}
	return ""
}

// buildStreams renders Stream elements and registers their ids.
func (s *Server) buildStreams(key core.RatingKey, info core.MediaInfo, sel selection, u *core.User) []model.Stream {
	var out []model.Stream
	audioSelected := -1
	if sel.audioSet {
		audioSelected = sel.audio
	}
	// Default audio: explicit selection, then preferred language, then the
	// default flag, then the first audio stream.
	if audioSelected < 0 {
		audioSelected = pickAudio(info, s.audioLanguages(u))
	}
	subSelected := -1
	if sel.subtitleSet {
		subSelected = sel.subtitle
	}
	for i, st := range info.Streams {
		id := s.streams.register(key, i, st)
		ps := model.Stream{
			ID:       id,
			Codec:    plexCodec(st.Codec),
			Bitrate:  st.BitrateKbps,
			Title:    st.Title,
			Default:  boolPtrIf(st.Default),
			Forced:   boolPtrIf(st.Forced),
			Language: languageName(st.Language),
		}
		if st.Language != "" {
			ps.LanguageCode = languageCode3(st.Language)
			ps.LanguageTag = languageTag(st.Language)
		}
		if !st.External {
			ps.Index = model.Ptr(st.Index)
		}
		switch st.Type {
		case "video":
			ps.StreamType = 1
			if info.Video() == &info.Streams[i] {
				ps.Default = model.Ptr(true) // PMS flags the primary video stream default
			}
			ps.Width, ps.Height = st.Width, st.Height
			ps.FrameRate = math.Round(st.FrameRate*1000) / 1000
			ps.BitDepth, ps.Level = st.BitDepth, st.Level
			ps.Profile = strings.ToLower(st.Profile)
			ps.ChromaSubsampling = "4:2:0"
			switch st.ColorTransfer {
			case "smpte2084", "arib-std-b67":
				ps.ColorTrc = st.ColorTransfer
				ps.ColorPrimaries = "bt2020"
				ps.ColorSpace = "bt2020nc"
			case "":
			default:
				ps.ColorTrc = st.ColorTransfer
			}
			if st.DolbyVision {
				ps.DOVIPresent = model.Ptr(true)
				ps.DOVIProfile, ps.DOVILevel = st.DolbyVisionProfile, st.DolbyVisionLevel
				if st.DolbyVisionProfile > 0 {
					ps.DOVIBLCompatID = model.Ptr(st.DolbyVisionCompatibilityID)
				}
				ps.DOVIBLPresent, ps.DOVIELPresent = st.DolbyVisionBLPresent, st.DolbyVisionELPresent
				ps.DOVIRPUPresent, ps.DOVIVersion = st.DolbyVisionRPUPresent, st.DolbyVisionVersion
			}
			ps.DisplayTitle = videoDisplayTitle(st)
			ps.ExtendedDisplayTitle = ps.DisplayTitle
		case "audio":
			ps.StreamType = 2
			ps.Channels = st.Channels
			ps.AudioChannelLayout = st.ChannelLayout
			ps.SamplingRate = st.SampleRate
			ps.Profile = strings.ToLower(st.Profile)
			if i == audioSelected {
				ps.Selected = model.Ptr(true)
			}
			ps.CanNormalizeLoudness = model.Ptr(false) // no loudness analysis
			ps.DisplayTitle = audioDisplayTitle(st)
			ps.ExtendedDisplayTitle = extendedTitle(st, ps.DisplayTitle, audioCodecLabel(st.Codec))
		case "subtitle":
			ps.StreamType = 3
			ps.CanAutoSync = model.Ptr(false) // no voice-activity data
			if st.External {
				ps.Key = "/library/streams/" + strconv.FormatInt(id, 10)
				ps.Format = ps.Codec
				ps.External = model.Ptr(true)
			}
			if i == subSelected {
				ps.Selected = model.Ptr(true)
			}
			ps.DisplayTitle = subtitleDisplayTitle(st)
			ps.ExtendedDisplayTitle = extendedTitle(st, ps.DisplayTitle, strings.ToUpper(plexCodec(st.Codec)))
		default:
			continue
		}
		out = append(out, ps)
	}
	return out
}

// extendedTitle is PMS's extendedDisplayTitle for audio and subtitle
// streams: the display title, or for a titled track "<title> (<language>
// <codec>)" ("Stereo (English DTS)", "SDH (English SRT)").
func extendedTitle(st core.StreamInfo, display, codec string) string {
	if st.Title == "" {
		return display
	}
	lang := languageName(st.Language)
	if lang == "" {
		lang = "Unknown"
	}
	return st.Title + " (" + lang + " " + codec + ")"
}

// pickAudio chooses the audio stream for languages in preference order
// (see Server.audioLanguages), then the stream flagged default, then the
// first. Multi-language releases often flag a dub as default.
func pickAudio(info core.MediaInfo, langs []string) int {
	for _, lang := range langs {
		// Within one wanted language the best tier wins (exact before
		// generic-want×regional before regional-want×generic); preference
		// order across languages still dominates.
		best, bestTier := -1, 0
		for i, st := range info.Streams {
			if st.Type != "audio" {
				continue
			}
			if tier := langtag.MatchTier(lang, st.Language); tier > bestTier {
				best, bestTier = i, tier
			}
		}
		if best >= 0 {
			return best
		}
	}
	first := -1
	for i, st := range info.Streams {
		if st.Type != "audio" {
			continue
		}
		if st.Default {
			return i
		}
		if first < 0 {
			first = i
		}
	}
	return first
}

func boolPtrIf(b bool) *bool {
	if b {
		return model.Ptr(true)
	}
	return nil
}

// plexCodec maps ffprobe/core codec names to the names PMS uses.
func plexCodec(c string) string {
	switch strings.ToLower(c) {
	case "dts":
		return "dca"
	case "dtshd", "dts-hd", "dts-hd ma":
		return "dca-ma"
	case "subrip":
		return "srt"
	case "hdmv_pgs_subtitle", "pgssub":
		return "pgs"
	case "dvd_subtitle", "dvdsub":
		return "vobsub"
	case "webvtt":
		return "vtt"
	case "h265":
		return "hevc"
	case "avc", "avc1":
		return "h264"
	case "e-ac3", "ec3":
		return "eac3"
	}
	return strings.ToLower(c)
}

// plexContainer maps container names to PMS's.
func plexContainer(c string) string {
	c = strings.ToLower(c)
	switch {
	case c == "":
		return "mkv"
	case strings.Contains(c, "matroska"), c == "mkv", c == "webm":
		if c == "webm" {
			return "webm"
		}
		return "mkv"
	case strings.Contains(c, "mp4"), c == "m4v":
		return "mp4"
	case c == "mov":
		return "mov"
	case c == "mpegts", c == "ts", c == "m2ts":
		return "mpegts"
	case c == "avi":
		return "avi"
	}
	return c
}

func containerExt(c string) string {
	switch c {
	case "mpegts":
		return "ts"
	case "":
		return "mkv"
	}
	return c
}

// videoResolution is PMS's videoResolution value.
func videoResolution(w, h int) string {
	switch {
	case w >= 3200 || h >= 1800:
		return "4k"
	case w >= 1700 || h >= 1000:
		return "1080"
	case w >= 1200 || h >= 700:
		return "720"
	case h >= 560:
		return "576"
	case h >= 400:
		return "480"
	case h > 0:
		return "sd"
	}
	return ""
}

func resolutionLabel(w, h int) string {
	switch r := videoResolution(w, h); r {
	case "4k":
		return "4K"
	case "sd", "":
		return "SD"
	default:
		return r + "p"
	}
}

// frameRateName is PMS's videoFrameRate value.
func frameRateName(f float64) string {
	switch {
	case f <= 0:
		return ""
	case f < 24.5:
		return "24p"
	case f < 25.5:
		return "PAL"
	case f < 30.5:
		return "NTSC"
	case f < 50.5:
		return "50p"
	default:
		return "60p"
	}
}

func videoDisplayTitle(st core.StreamInfo) string {
	res := resolutionLabel(st.Width, st.Height)
	parts := []string{}
	if st.DolbyVision {
		parts = append(parts, "DoVi")
	}
	switch st.ColorTransfer {
	case "smpte2084":
		parts = append(parts, "HDR10")
	case "arib-std-b67":
		parts = append(parts, "HLG")
	}
	if len(parts) > 0 {
		res += " " + strings.Join(parts, "/")
	}
	codec := strings.ToUpper(st.Codec)
	switch strings.ToLower(st.Codec) {
	case "h264":
		codec = "H.264"
	case "hevc":
		codec = "HEVC"
	}
	// PMS names the profile only where it matters ("HEVC Main 10"), not
	// for H.264 ("1080p (H.264)") or plain HEVC Main.
	if p := strings.ToLower(st.Profile); p != "" && p != "main" && strings.ToLower(st.Codec) != "h264" {
		codec += " " + titleCase(st.Profile)
	}
	return res + " (" + codec + ")"
}

// audioCodecLabel is the codec as PMS writes it in stream titles.
func audioCodecLabel(c string) string {
	codec := strings.ToUpper(plexCodec(c))
	switch codec {
	case "DCA":
		return "DTS"
	case "DCA-MA":
		return "DTS-HD MA"
	case "TRUEHD":
		return "TrueHD"
	}
	return codec
}

func titleCase(s string) string {
	words := strings.Fields(strings.ReplaceAll(s, "_", " "))
	for i, w := range words {
		words[i] = strings.ToUpper(w[:1]) + w[1:]
	}
	return strings.Join(words, " ")
}

func channelsLabel(n int, layout string) string {
	switch n {
	case 1:
		return "Mono"
	case 2:
		return "Stereo"
	case 6:
		return "5.1"
	case 8:
		return "7.1"
	case 0:
		return ""
	}
	if layout != "" {
		return layout
	}
	return strconv.Itoa(n) + "ch"
}

func audioDisplayTitle(st core.StreamInfo) string {
	lang := languageName(st.Language)
	if lang == "" {
		lang = "Unknown"
	}
	inner := audioCodecLabel(st.Codec)
	if ch := channelsLabel(st.Channels, st.ChannelLayout); ch != "" {
		inner += " " + ch
	}
	return lang + " (" + inner + ")"
}

func subtitleDisplayTitle(st core.StreamInfo) string {
	lang := languageName(st.Language)
	if lang == "" {
		lang = "Unknown"
	}
	inner := strings.ToUpper(plexCodec(st.Codec))
	if st.External {
		inner += " External"
	}
	if st.Forced {
		lang += " Forced"
	}
	return lang + " (" + inner + ")"
}

// Languages: ISO 639-1 / 639-2 codes to names. Covers what releases and
// OpenSubtitles commonly carry; unknown codes pass through.
var languages = map[string][3]string{
	// code1: {name, code3(639-2/B), tag}
	"en": {"English", "eng", "en"}, "fr": {"French", "fre", "fr"}, "de": {"German", "ger", "de"},
	"es": {"Spanish", "spa", "es"}, "it": {"Italian", "ita", "it"}, "pt": {"Portuguese", "por", "pt"},
	"nl": {"Dutch", "dut", "nl"}, "sv": {"Swedish", "swe", "sv"}, "no": {"Norwegian", "nor", "no"},
	"nb": {"Norwegian Bokmål", "nob", "nb"}, "da": {"Danish", "dan", "da"}, "fi": {"Finnish", "fin", "fi"},
	"pl": {"Polish", "pol", "pl"}, "cs": {"Czech", "cze", "cs"}, "hu": {"Hungarian", "hun", "hu"},
	"ru": {"Russian", "rus", "ru"}, "uk": {"Ukrainian", "ukr", "uk"}, "el": {"Greek", "gre", "el"},
	"tr": {"Turkish", "tur", "tr"}, "ar": {"Arabic", "ara", "ar"}, "he": {"Hebrew", "heb", "he"},
	"hi": {"Hindi", "hin", "hi"}, "ja": {"Japanese", "jpn", "ja"}, "ko": {"Korean", "kor", "ko"},
	"zh": {"Chinese", "chi", "zh"}, "th": {"Thai", "tha", "th"}, "vi": {"Vietnamese", "vie", "vi"},
	"id": {"Indonesian", "ind", "id"}, "ro": {"Romanian", "rum", "ro"}, "bg": {"Bulgarian", "bul", "bg"},
	"hr": {"Croatian", "hrv", "hr"}, "sr": {"Serbian", "srp", "sr"}, "sk": {"Slovak", "slo", "sk"},
	"sl": {"Slovenian", "slv", "sl"}, "et": {"Estonian", "est", "et"}, "lv": {"Latvian", "lav", "lv"},
	"lt": {"Lithuanian", "lit", "lt"}, "fa": {"Persian", "per", "fa"}, "ms": {"Malay", "may", "ms"},
	"ta": {"Tamil", "tam", "ta"}, "te": {"Telugu", "tel", "te"}, "is": {"Icelandic", "ice", "is"},
	"ca": {"Catalan", "cat", "ca"}, "eu": {"Basque", "baq", "eu"}, "gl": {"Galician", "glg", "gl"},
}

// primaryLang is the display/code lookup key for a language: the
// canonical primary subtag from langtag (region and script dropped).
func primaryLang(code string) string {
	if t, ok := langtag.ParseBCP47(code); ok {
		return t.Primary
	}
	return strings.ToLower(strings.TrimSpace(code))
}

// sameLanguage is the symmetric view of langtag.MatchTier: true when
// the tier is > 0 — exact, or the same primary with at least one
// generic side (pt × pt-br true; pt-br × pt-pt false).
func sameLanguage(a, b string) bool {
	return a != "" && b != "" && langtag.MatchTier(a, b) > 0
}

func languageName(code string) string {
	if code == "" {
		return ""
	}
	if l, ok := languages[primaryLang(code)]; ok {
		return l[0]
	}
	return code
}

func languageCode3(code string) string {
	if l, ok := languages[primaryLang(code)]; ok {
		return l[1]
	}
	return strings.ToLower(code)
}

func languageTag(code string) string {
	if l, ok := languages[primaryLang(code)]; ok {
		return l[2]
	}
	return strings.ToLower(code)
}

// renderTitles renders a list, loading the user's watch state in one call.
func (s *Server) renderTitles(ctx context.Context, pr *Request, ts []core.Title, o renderOpts) []model.Metadata {
	ts = s.visibleTitles(pr.User, ts)
	ts = s.availableForViewer(ctx, pr.User, ts)
	if len(ts) == 0 {
		return nil
	}
	if d := s.d.Discovery; d != nil {
		if d.Ratings != nil {
			ctx = d.Ratings.WithSettingsSnapshot(ctx)
		}
		if d.Overlays != nil {
			ctx = d.Overlays.WithSettingsSnapshot(ctx)
		}
	}
	if o.state == nil {
		o.state = s.stateFor(ctx, pr, ts)
	}
	s.preloadUltraBlur(ctx, ts)
	var guids map[core.RatingKey]string
	var missingGUIDs []core.RatingKey
	if s.d.PlexGUID != nil {
		lookup := make([]core.RatingKey, 0, len(ts)*2)
		for i := range ts {
			k := ts[i].Key
			lookup = append(lookup, k)
			switch ts[i].Type {
			case core.TypeSeason:
				if k.TMDB() > 0 {
					lookup = append(lookup, core.ShowKey(k.TMDB()))
				}
			case core.TypeEpisode:
				if k.TMDB() > 0 {
					lookup = append(lookup, core.ShowKey(k.TMDB()), core.SeasonKey(k.TMDB(), ts[i].Season))
				}
			}
		}
		guids = s.d.PlexGUID.CachedMany(ctx, lookup)
	}
	order := s.episodeOrderList(ctx, pr, ts, o.detail)
	out := make([]model.Metadata, 0, len(ts))
	for i := range ts {
		m := s.metadata(ctx, pr, &ts[i], o)
		s.applyEpisodeOrder(ctx, pr, &ts[i], &m, order)
		if s.d.PlexGUID != nil {
			k := ts[i].Key
			if guid := guids[k]; guid != "" {
				if !(ts[i].Type == core.TypeSeason && ts[i].Season >= 9000) {
					m.GUID = guid
				}
			} else {
				missingGUIDs = append(missingGUIDs, k)
			}
			switch ts[i].Type {
			case core.TypeSeason:
				if ts[i].Season < 9000 {
					if sg := guids[core.ShowKey(k.TMDB())]; sg != "" {
						m.ParentGUID = sg
					}
				}
			case core.TypeEpisode:
				if sg := guids[core.ShowKey(k.TMDB())]; sg != "" {
					m.GrandparentGUID = sg
				}
				if !strings.HasPrefix(m.ParentGUID, "local://episode-order/") {
					if seag := guids[core.SeasonKey(k.TMDB(), ts[i].Season)]; seag != "" {
						m.ParentGUID = seag
					}
				}
			}
		}
		out = append(out, m)
	}
	if s.d.PlexGUID != nil && len(missingGUIDs) > 0 {
		s.d.PlexGUID.Schedule(missingGUIDs...)
	}
	return out
}

func (s *Server) stateFor(ctx context.Context, pr *Request, ts []core.Title) map[core.RatingKey]core.WatchStatus {
	if s.d.State == nil || pr.User == nil || len(ts) == 0 {
		return nil
	}
	keys := make([]core.RatingKey, len(ts))
	for i := range ts {
		keys[i] = ts[i].Key
	}
	st, err := s.d.State.GetMany(ctx, pr.User.ID, keys)
	if err != nil {
		s.log.Warn("load watch state", "err", err)
		return nil
	}
	return st
}

// applyLeafCounts fills viewedLeafCount for shows and seasons by loading
// their episodes' watch state. Used on detail and children pages only. memo
// (may be nil) shares show episodes with the rest of the response.
func (s *Server) applyLeafCounts(ctx context.Context, pr *Request, items []model.Metadata, ts []core.Title, memo showEpisodes) {
	if s.d.State == nil || pr.User == nil {
		return
	}
	byKey := map[string]int{}
	for i := range items {
		byKey[items[i].RatingKey] = i
	}
	// Seasons listed together (a show's children) are counted from their
	// show's episodes, loaded once, rather than by loading every season.
	together := map[core.RatingKey]int{}
	for i := range ts {
		if ts[i].Type == core.TypeSeason {
			together[core.ShowKey(ts[i].Key.TMDB())]++
		}
	}
	bySeason := map[core.RatingKey]map[int][]core.Title{}
	for sourceIndex := range ts {
		i, ok := byKey[ts[sourceIndex].Key.String()]
		if !ok {
			continue
		}
		title := ts[sourceIndex]
		var eps []core.Title
		var err error
		switch title.Type {
		case core.TypeShow:
			eps, err = s.episodesOfShow(ctx, pr, memo, title.Key)
		case core.TypeSeason:
			show := core.ShowKey(title.Key.TMDB())
			seasons, loaded := bySeason[show]
			if !loaded && together[show] > 1 {
				if all, e := s.episodesOfShow(ctx, pr, memo, show); e == nil {
					seasons = s.presentedSeasons(ctx, pr, show, all)
				}
				bySeason[show] = seasons
			}
			if seasons != nil {
				eps = seasons[title.Key.Season()]
			} else {
				// A lone season, or its show's episodes failed to load.
				eps, err = s.metadataChildren(ctx, pr, title.Key)
			}
		default:
			continue
		}
		if err != nil || len(eps) == 0 {
			continue
		}
		eps = s.visibleTitles(pr.User, eps)
		if title.Type == core.TypeShow {
			// A show's totals are its regular episodes, as the catalogue
			// counts them on every list: Specials neither add to the badge
			// nor keep a finished show unwatched.
			regular := eps[:0:0]
			for _, e := range eps {
				if e.Season > 0 {
					regular = append(regular, e)
				}
			}
			eps = regular
			if len(eps) == 0 {
				continue
			}
		}
		st := s.stateFor(ctx, pr, eps)
		viewed := 0
		for _, e := range eps {
			if st[e.Key].ViewCount > 0 {
				viewed++
			}
		}
		// The episodes just counted are the ones the viewer can list and
		// play (aired, visible, in their episode order; regular seasons for
		// a show), so leafCount comes
		// from the same list as viewedLeafCount and a fully watched show
		// or season reads as watched.
		items[i].ViewedLeafCount = model.Ptr(viewed)
		items[i].LeafCount = model.Ptr(len(eps))
		if viewed > 0 && viewed == len(eps) {
			items[i].ViewCount = max(items[i].ViewCount, 1)
		}
	}
}

// audioLanguages is the user's audio preference, falling back to the
// server's catalogue language (e.g. "en-US" -> "en").
func (s *Server) audioLanguages(u *core.User) []string {
	var langs []string
	if u != nil {
		langs = append(langs, u.AudioLanguages...)
	}
	if s.d.Config != nil {
		if l := s.d.Config().Catalog.Language; l != "" {
			langs = append(langs, strings.SplitN(l, "-", 2)[0])
		}
	}
	return langs
}
