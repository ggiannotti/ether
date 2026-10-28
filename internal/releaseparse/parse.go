// Package releaseparse parses scene and P2P release names into
// core.ReleaseInfo. It is pure string work and safe for concurrent use.
package releaseparse

import (
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/braedonsaunders/ether/internal/core"
)

var (
	reYear        = regexp.MustCompile(`\b(188[89]|189\d|19\d{2}|20\d{2})\b`)
	reSE          = regexp.MustCompile(`(?i)\bS(\d{1,2})[\s-]*E(\d{1,3})((?:\s*E\d{1,3})*)(?:\s*-\s*E?(\d{1,3}))?\b`)
	reSeasonAndEp = regexp.MustCompile(`(?i)\b(?:Season|Series|Saison|Stagione|Temporada)\s*(\d{1,2})\b[\s-]*\b(?:Episode|Ep)\s*(\d{1,3})\b`)
	reSeasonOnly  = regexp.MustCompile(`(?i)\bS(\d{1,2})(?:\s*(?:-|to)\s*S?(\d{1,2}))?\b`)
	reSeasonWord  = regexp.MustCompile(`(?i)\b(?:Seasons?|Saison|Stagione|Temporada)\s*(\d{1,2})(?:\s*(?:-|to)\s*(?:Seasons?|S)?\s*(\d{1,2}))?\b`)
	reXEpisode    = regexp.MustCompile(`\b(\d{1,2})x(\d{1,3})\b`)
	reBareEpisode = regexp.MustCompile(`(?i)\b(?:Episode\s*(\d{1,3})|Ep\s*(\d{1,3})|E(\d{2,3}))\b`)
	reDashEpisode = regexp.MustCompile(`(?i)\s-\s*(\d{2,3})\s+(?:\d{3,4}[pi]\b|BD\b|BLURAY\b|WEB|HDTV\b|DVD|X264\b|X265\b|HEVC\b|H264\b|H265\b|AV1\b|XVID\b)`)
	reEpRange     = regexp.MustCompile(`(?i)E(\d{1,3})`)
	reChannels    = regexp.MustCompile(`([257])[\s.]([01])\b`)
	reRawChannels = regexp.MustCompile(`\b(5\.1|7\.1|2\.0|2\.1|6\.1)\b`)
	reGroupTail   = regexp.MustCompile(`-([A-Za-z0-9][A-Za-z0-9_]{1,})$`)
	reBracket     = regexp.MustCompile(`\[([^\]]+)\]`)
)

var containers = map[string]bool{
	"mkv": true, "mp4": true, "avi": true, "m4v": true, "mov": true,
	"wmv": true, "ts": true, "m2ts": true, "webm": true, "mpg": true, "mpeg": true,
}

var languages = []struct{ token, name string }{
	{"MULTI", "multi"}, {"FRENCH", "french"}, {"VOSTFR", "french"}, {"TRUEFRENCH", "french"},
	{"GERMAN", "german"}, {"SPANISH", "spanish"}, {"LATINO", "spanish"}, {"ITALIAN", "italian"},
	{"DUTCH", "dutch"}, {"PORTUGUESE", "portuguese"}, {"RUSSIAN", "russian"}, {"RUS", "russian"}, {"UKRAINIAN", "ukrainian"}, {"UKR", "ukrainian"}, {"POLISH", "polish"},
	{"KOREAN", "korean"}, {"JAPANESE", "japanese"}, {"CHINESE", "chinese"}, {"MANDARIN", "chinese"},
	{"HINDI", "hindi"}, {"TELUGU", "telugu"}, {"TAMIL", "tamil"}, {"TURKISH", "turkish"},
	{"ARABIC", "arabic"}, {"SWEDISH", "swedish"}, {"NORWEGIAN", "norwegian"}, {"DANISH", "danish"},
	{"FINNISH", "finnish"}, {"CZECH", "czech"}, {"HUNGARIAN", "hungarian"}, {"GREEK", "greek"},
	{"THAI", "thai"}, {"VIETNAMESE", "vietnamese"}, {"INDONESIAN", "indonesian"},
	{"ENGLISH", "english"}, {"ENG", "english"},
	// Explicit regional language tags are used by WEB release groups.
	// Do not treat bare country codes (BR/US/GB) as audio languages.
	{"PT-BR", "portuguese"}, {"PT-PT", "portuguese"},
	{"ES-ES", "spanish"}, {"ES-MX", "spanish"}, {"ES-419", "spanish"},
	{"EN-US", "english"}, {"EN-GB", "english"},
}

// Parse extracts structured info from a release name.
func Parse(name string) core.ReleaseInfo {
	var info core.ReleaseInfo
	raw := strings.TrimSpace(name)
	if raw == "" {
		return info
	}
	stem, group, container := parseGroup(raw)
	info.Group = group
	info.Container = container

	spaced := spacify(stem)
	upper := " " + strings.ToUpper(spaced) + " "

	// Season / episode.
	markerAt := parseEpisodes(spaced, upper, &info)
	var yearAt int
	info.Year, yearAt = parseYearAt(spaced)
	// Words before the release year or the episode/season marker are the
	// title ("The.Spanish.Princess.S01E01", "The.French.Dispatch.2021"):
	// release tags only count after the earlier of the two.
	floor := yearAt
	if markerAt >= 0 && (floor < 0 || markerAt < floor) {
		floor = markerAt
	}
	// "COMPLETE" before the release year is usually part of the title
	// ("A.Complete.Unknown.2024"), unless it names a pack. A string that
	// already names a specific episode ("Show.S01.COMPLETE/Show.S01E02.mkv")
	// is that episode, not a whole-season pack.
	if len(info.Episodes) == 0 {
		if idx := tokenIndex(spaced, "COMPLETE"); idx >= 0 && (idx > yearAt || len(info.Seasons) > 0 || namesPack(spaced[idx+len("COMPLETE"):])) {
			info.FullSeason = true
		}
	}
	info.Height = parseHeight(upper)
	info.Source = overrideSource(parseSource(upper), spaced, floor)
	info.VideoCodec = parseVideoCodec(upper)
	info.AudioCodec, info.Atmos = parseAudio(upper)
	if m := reChannels.FindStringSubmatch(strings.ToUpper(spaced)); m != nil {
		info.Channels = m[1] + "." + m[2]
	}
	// DDP5.1 style, where dot was a separator: DD 5 1 handled above; also "5.1" in raw.
	if info.Channels == "" {
		if m := reRawChannels.FindStringSubmatch(raw); m != nil {
			info.Channels = m[1]
		}
	}
	if strings.Contains(upper, "HDR10+") || strings.Contains(upper, "HDR10PLUS") || strings.Contains(upper, " HDR10 ") || strings.Contains(upper, " HDR ") || strings.Contains(upper, " HDR-") || strings.Contains(upper, "HDR10") {
		info.HDR10 = true
	}
	if strings.Contains(upper, "DOLBY VISION") || strings.Contains(upper, " DOLBYVISION ") || strings.Contains(upper, " DOVI ") || strings.Contains(upper, " DV ") || strings.Contains(upper, " DV-") || strings.HasSuffix(strings.TrimSpace(upper), " DV") {
		info.DolbyVision = true
	}
	info.Edition = ParseEdition(name)
	info.Proper = strings.Contains(upper, " PROPER ")
	info.Repack = strings.Contains(upper, " REPACK ")
	// Words of the title itself ("The.French.Dispatch.2021", "The Danish
	// Girl") are not audio languages.
	seen := map[string]bool{}
	for _, l := range languages {
		if languageTagAfter(spaced, l.token, floor) {
			if !seen[l.name] {
				seen[l.name] = true
				info.Languages = append(info.Languages, l.name)
			}
		}
	}
	info.Title = extractTitle(spaced, yearAt, markerAt, floor, &info)
	return info
}

// parseGroup extracts the release group and the container extension from
// raw, returning the name with the extension stripped (stem), the group and
// the container ("" when the suffix is not a known container).
func parseGroup(raw string) (stem, group, container string) {
	// Leading / trailing bracket group, e.g. [HorribleSubs] or [YTS.MX].
	// A leading bracket names the group (anime convention); a trailing one
	// is only used when there is no -GROUP suffix ("-SPARKS[rarbg]" names
	// SPARKS, and [rarbg] is the uploader).
	bracketGroup, leadingGroup := "", ""
	hasBracket := strings.IndexByte(raw, '[') >= 0
	if hasBracket {
		if ms := reBracket.FindAllStringSubmatchIndex(raw, -1); len(ms) > 0 {
			for i, m := range ms {
				v := strings.TrimSpace(raw[m[2]:m[3]])
				if v != "" && len(v) <= 20 && !looksLikeMeta(v) {
					bracketGroup = v
					if i == 0 && m[0] == 0 {
						leadingGroup = v
					}
				}
			}
		}
	}
	// Container extension.
	stem = raw
	if i := strings.LastIndex(raw, "."); i > 0 {
		ext := strings.ToLower(raw[i+1:])
		// Only treat as container if the suffix is a bare extension.
		if containers[ext] {
			container = ext
			stem = raw[:i]
		}
	}
	// Group: trailing -GROUP (after stripping bracket chunks).
	noBrackets := stem
	if hasBracket {
		noBrackets = reBracket.ReplaceAllString(stem, " ")
	}
	if leadingGroup != "" {
		group = leadingGroup
	} else if m := reGroupTail.FindStringSubmatch(strings.TrimSpace(noBrackets)); m != nil && !isKnownTail(m[1]) {
		group = m[1]
	} else {
		group = bracketGroup
	}
	return stem, group, container
}

// overrideSource applies the CAM override to source: an explicit
// low-quality tag outranks a contradictory WEB/BD claim. Only release
// metadata (spaced at or after floor) is inspected, never words in the
// film's title.
func overrideSource(source core.Source, spaced string, floor int) core.Source {
	metadata := " " + strings.ToUpper(spaced[max(floor, 0):]) + " "
	if containsAny(metadata, " HDCAM", " HD CAM", " CAM ", " CAM-", "CAMRIP", " HQCAM", " HDTS", " HD-TS", " TSRIP", " HDTC", " HD-TC", " TELESYNC", " TELECINE", " HQ PRE ", " HQ-PRE ") {
		return core.SourceCam
	}
	return source
}

func looksLikeMeta(v string) bool {
	u := strings.ToUpper(v)
	// An anime CRC32 checksum ("[ABCDEF12]") is not a release group.
	if len(u) == 8 && strings.Trim(u, "0123456789ABCDEF") == "" {
		return true
	}
	for _, k := range []string{"1080", "720", "2160", "480", "BLURAY", "WEB", "HDTV", "X264", "X265", "HEVC", "HDR"} {
		if strings.Contains(u, k) {
			return true
		}
	}
	return false
}

func isKnownTail(g string) bool {
	switch strings.ToUpper(g) {
	case "DL", "RIP", "CAM", "TS", "TC", "MA", "HD", "SD", "VISION", "DV", "HDR", "ATMOS":
		return true
	}
	return false
}

var spacifyReplacer = strings.NewReplacer(".", " ", "_", " ", "(", " ", ")", " ", "[", " ", "]", " ")

// spacify turns separators into spaces, keeping hyphens inside tokens.
func spacify(s string) string {
	s = spacifyReplacer.Replace(s)
	return strings.Join(strings.Fields(s), " ")
}

// parseEpisodes fills seasons and episodes and returns the offset of the
// marker it read them from, or -1.
func parseEpisodes(spaced, upper string, info *core.ReleaseInfo) int {
	hasS := strings.Contains(upper, " S")
	if hasS {
		if seasons, episodes, at, ok := parseSEEpisode(spaced); ok {
			info.Seasons = seasons
			info.Episodes = episodes
			return at
		}
	}
	hasSeasonWord := containsAny(upper, "SEASON", "SERIES", "SAISON", "STAGIONE", "TEMPORADA")
	if hasSeasonWord {
		if season, episode, at, ok := parseSeasonEpisode(spaced); ok {
			info.Seasons = []int{season}
			info.Episodes = []int{episode}
			return at
		}
		if loc := reSeasonWord.FindStringSubmatchIndex(spaced); loc != nil {
			m := submatches(spaced, loc)
			info.Seasons = seasonRange(m)
			info.FullSeason = true
			return loc[0]
		}
	}
	if hasS {
		if loc := reSeasonOnly.FindStringSubmatchIndex(spaced); loc != nil {
			m := submatches(spaced, loc)
			// Guard against matching the S in e.g. S01E02 (already handled) —
			// here no E followed, so it is a season pack.
			info.Seasons = seasonRange(m)
			info.FullSeason = true
			return loc[0]
		}
	}
	if strings.Contains(upper, "X") {
		if season, episode, at, ok := parseXEpisode(spaced); ok {
			info.Seasons = []int{season}
			info.Episodes = []int{episode}
			return at
		}
	}
	if strings.Contains(upper, " E") {
		if episode, at, ok := parseBareEpisode(spaced); ok {
			info.Episodes = []int{episode}
			return at
		}
	}
	if strings.Contains(spaced, " - ") {
		if episode, at, ok := parseDashEpisode(spaced); ok {
			info.Episodes = []int{episode}
			return at
		}
	}
	return -1
}

// parseSEEpisode reads the SxxEyy pattern (reSE), including chained E##
// tokens (S01E02E03) and a range tail (S01E02-E04).
func parseSEEpisode(spaced string) (seasons, episodes []int, at int, ok bool) {
	loc := reSE.FindStringSubmatchIndex(spaced)
	if loc == nil {
		return nil, nil, -1, false
	}
	m := submatches(spaced, loc)
	s, _ := strconv.Atoi(m[1])
	seasons = []int{s}
	first, _ := strconv.Atoi(m[2])
	eps := []int{first}
	// Additional E## tokens chained (S01E02E03).
	for _, em := range reEpRange.FindAllStringSubmatch(m[3], -1) {
		n, _ := strconv.Atoi(em[1])
		eps = append(eps, n)
	}
	// Range tail: S01E02-E04 or S01E02-04 (captured last group beyond chained).
	if m[4] != "" && len(eps) == 1 {
		last, _ := strconv.Atoi(m[4])
		// Only expand when it really looks like a range end and is
		// a small step forward.
		if last > first && last-first <= 30 && rangeTailPresent(spaced, m[0]) {
			for n := first + 1; n <= last; n++ {
				eps = append(eps, n)
			}
		} else if last != first {
			eps = append(eps, last)
		}
	}
	sort.Ints(eps)
	return seasons, dedupeInts(eps), loc[0], true
}

// parseSeasonEpisode reads the "Season N Episode M" pattern
// (reSeasonAndEp).
func parseSeasonEpisode(spaced string) (season, episode, at int, ok bool) {
	loc := reSeasonAndEp.FindStringSubmatchIndex(spaced)
	if loc == nil {
		return 0, 0, -1, false
	}
	m := submatches(spaced, loc)
	s, _ := strconv.Atoi(m[1])
	e, _ := strconv.Atoi(m[2])
	if s < 1 || s > 50 || e < 1 {
		return 0, 0, -1, false
	}
	return s, e, loc[0], true
}

// parseXEpisode reads the NxM pattern (reXEpisode).
func parseXEpisode(spaced string) (season, episode, at int, ok bool) {
	loc := reXEpisode.FindStringSubmatchIndex(spaced)
	if loc == nil {
		return 0, 0, -1, false
	}
	m := submatches(spaced, loc)
	s, _ := strconv.Atoi(m[1])
	e, _ := strconv.Atoi(m[2])
	// 1x02 is ambiguous with e.g. codec-ish tokens; season<=50 guard.
	if s < 1 || s > 50 {
		return 0, 0, -1, false
	}
	return s, e, loc[0], true
}

// parseBareEpisode reads a bare episode pattern (reBareEpisode):
// "Episode N", "Ep N" or "ENN" without a season.
func parseBareEpisode(spaced string) (episode, at int, ok bool) {
	loc := reBareEpisode.FindStringSubmatchIndex(spaced)
	if loc == nil {
		return 0, -1, false
	}
	m := submatches(spaced, loc)
	_, yearAt := parseYearAt(spaced)
	// "Star Wars Episode 1 1999" has the release year after "Episode 1";
	// miniseries and season-directory files ("Band.of.Brothers.E01",
	// "Episode 05 - Name.mkv") do not.
	if m[1] != "" && yearAt >= 0 && loc[0] <= yearAt {
		return 0, -1, false
	}
	tok := m[1]
	if tok == "" {
		tok = m[2]
	}
	if tok == "" {
		tok = m[3]
	}
	e, err := strconv.Atoi(tok)
	if err != nil || e < 1 || e > 100 {
		return 0, -1, false
	}
	return e, loc[0], true
}

// parseDashEpisode reads the dash episode pattern (reDashEpisode):
// "Title - 02 1080p ...".
func parseDashEpisode(spaced string) (episode, at int, ok bool) {
	loc := reDashEpisode.FindStringSubmatchIndex(spaced)
	if loc == nil {
		return 0, -1, false
	}
	m := submatches(spaced, loc)
	e, err := strconv.Atoi(m[1])
	if err != nil || e < 1 || e > 500 {
		return 0, -1, false
	}
	return e, loc[0], true
}

// seasonRange expands submatches carrying a season and an optional range
// end (group 2) into the season list, capped at a 30-season span.
func seasonRange(m []string) []int {
	s, _ := strconv.Atoi(m[1])
	seasons := []int{s}
	if m[2] != "" {
		end, _ := strconv.Atoi(m[2])
		for n := s + 1; n <= end && n-s <= 30; n++ {
			seasons = append(seasons, n)
		}
	}
	return seasons
}

// submatches turns FindStringSubmatchIndex output into strings ("" for a
// group that did not participate).
func submatches(s string, loc []int) []string {
	out := make([]string, len(loc)/2)
	for i := range out {
		if loc[2*i] >= 0 {
			out[i] = s[loc[2*i]:loc[2*i+1]]
		}
	}
	return out
}

func rangeTailPresent(spaced, match string) bool {
	return strings.Contains(strings.ToUpper(match), "-")
}

func dedupeInts(in []int) []int {
	out := in[:0:0]
	for i, v := range in {
		if i == 0 || v != in[i-1] {
			out = append(out, v)
		}
	}
	return out
}

var reResolution = regexp.MustCompile(`(?i)\b\d{3,4}[pi]\b`)

func parseYear(spaced string, info *core.ReleaseInfo) int {
	y, _ := parseYearAt(spaced)
	return y
}

// parseYearAt returns the release year and its offset in spaced, or (0, -1).
func parseYearAt(spaced string) (int, int) {
	locs := reYear.FindAllStringIndex(spaced, -1)
	if len(locs) == 0 {
		return 0, -1
	}
	// A year-like title ("1917", "Blade Runner 2049", "Wonder Woman 1984")
	// is followed by the release year: the last year before the episode
	// or resolution markers is the release year.
	limit := len(spaced)
	if loc := reSE.FindStringIndex(spaced); loc != nil {
		limit = loc[0]
	}
	if loc := reResolution.FindStringIndex(spaced); loc != nil && loc[0] < limit {
		limit = loc[0]
	}
	pick := locs[0]
	for _, l := range locs[1:] {
		// Release tags after a year ("Movie.1984.Remastered.2014") mean
		// the metadata has begun: that year was the release year, and
		// later ones are remaster or group years.
		if l[0] < limit && releaseMarkerIn(spaced[pick[1]:l[0]]) {
			break
		}
		// The end of a span ("1994-2004") is not a release year.
		rangeEnd := l[0] >= 2 && spaced[l[0]-1] == '-' && spaced[l[0]-2] >= '0' && spaced[l[0]-2] <= '9'
		if l[0] < limit && !rangeEnd {
			pick = l
		}
	}
	y, _ := strconv.Atoi(spaced[pick[0]:pick[1]])
	return y, pick[0]
}

// releaseMarkerIn reports whether seg carries a release tag (a boundary
// keyword or an edition label) rather than only title words.
func releaseMarkerIn(seg string) bool {
	for _, kw := range boundaryKeywords {
		if tokenIndex(seg, kw) >= 0 {
			return true
		}
	}
	return ParseEdition(seg) != ""
}

// namesPack reports whether the words after "COMPLETE" describe a pack
// ("Complete Series", "Complete S01-S05").
func namesPack(rest string) bool {
	rest = strings.TrimLeft(rest, " -")
	next, _, _ := strings.Cut(rest, " ")
	switch strings.ToUpper(next) {
	case "SERIES", "SEASON", "SEASONS", "PACK", "COLLECTION", "BOXSET":
		return true
	}
	loc := reSeasonOnly.FindStringIndex(rest)
	return loc != nil && loc[0] == 0
}

// languageTagAfter reports whether the ASCII tag occurs in spaced after
// offset floor, preceded by the start or a space and followed by the end, a
// space or a hyphen.
func languageTagAfter(spaced, tag string, floor int) bool {
	for i := max(floor+1, 0); i+len(tag) <= len(spaced); i++ {
		if i > 0 && spaced[i-1] != ' ' {
			continue
		}
		end := i + len(tag)
		if end < len(spaced) && spaced[end] != ' ' && spaced[end] != '-' {
			continue
		}
		if strings.EqualFold(spaced[i:end], tag) {
			return true
		}
	}
	return false
}

// tokenIndex returns the offset in s of the first case-insensitive match of
// the ASCII keyword kw that is a whole token (bounded by the string ends,
// spaces or hyphens), or -1.
func tokenIndex(s, kw string) int {
	for i := 0; i+len(kw) <= len(s); i++ {
		if i > 0 && s[i-1] != ' ' && s[i-1] != '-' {
			continue
		}
		end := i + len(kw)
		if end < len(s) && s[end] != ' ' && s[end] != '-' {
			continue
		}
		if strings.EqualFold(s[i:end], kw) {
			return i
		}
	}
	return -1
}

func parseHeight(upper string) int {
	// An explicit resolution wins over a source hint: "1080p.UHD.BluRay"
	// is a 1080p encode of a UHD disc.
	switch {
	case strings.Contains(upper, "2160P") || strings.Contains(upper, "2160I"):
		return 2160
	case strings.Contains(upper, "1080P") || strings.Contains(upper, "1080I"):
		return 1080
	case strings.Contains(upper, "720P") || strings.Contains(upper, "720I"):
		return 720
	case strings.Contains(upper, "576P") || strings.Contains(upper, "576I"):
		return 576
	case strings.Contains(upper, "480P") || strings.Contains(upper, "480I"):
		return 480
	case strings.Contains(upper, "360P"):
		return 360
	case strings.Contains(upper, " 4K ") || strings.Contains(upper, " 4K-") || strings.Contains(upper, " UHD ") || strings.HasSuffix(strings.TrimSpace(upper), "UHD") || strings.Contains(upper, "UHD "):
		return 2160
	}
	return 0
}

func parseSource(upper string) core.Source {
	switch {
	case strings.Contains(upper, " REMUX") || strings.Contains(upper, "BDREMUX"):
		return core.SourceRemux
	case containsAny(upper, "BLURAY", "BLU-RAY", "BDRIP", "BRRIP", "BD-RIP", "HDDVD", "HD-DVD"):
		return core.SourceBluRay
	case containsAny(upper, "WEBRIP", "WEB-RIP", " WEB RIP "):
		return core.SourceWEBRip
	case containsAny(upper, "WEB-DL", "WEBDL", " WEB DL", " WEB "):
		return core.SourceWEBDL
	case strings.Contains(upper, " HDTV") || strings.Contains(upper, "HDTV ") ||
		containsAny(upper, " PDTV ", " DSR ", " DSRIP ", " TVRIP ", " SATRIP ", " DTHRIP ", " SDTV "):
		return core.SourceHDTV
	case containsAny(upper, "DVDSCR", " SCR ", "SCREENER"):
		return core.SourceScreener
	case containsAny(upper, "DVDRIP", "DVD-RIP", " DVD ", " DVDR ", " DVD5 ", " DVD9 ", " DVD-5 ", " DVD-9 ", " VHSRIP ", " LDRIP ", " LASERDISC "):
		return core.SourceDVD
	case containsAny(upper, "HDCAM", " HD CAM", " CAM ", " CAM-", "CAMRIP", "HQCAM", "HDTS", "HD-TS", "TSRIP", "HDTC", "HD-TC", " TELESYNC", " TELECINE", " TS ", " TC ", "-TS ", "-TC "):
		return core.SourceCam
	case strings.HasSuffix(strings.TrimSpace(upper), "CAM"):
		return core.SourceCam
	}
	return core.SourceUnknown
}

func parseVideoCodec(upper string) string {
	switch {
	case containsAny(upper, "X265", "H265", "H 265", "HEVC"):
		return "hevc"
	case containsAny(upper, " AV1", "-AV1", " AV1 "):
		return "av1"
	case containsAny(upper, " VP9"):
		return "vp9"
	case containsAny(upper, "X264", "H264", "H 264", " AVC"):
		return "h264"
	case containsAny(upper, " VC-1", " VC1"):
		return "vc1"
	case containsAny(upper, "XVID", "DIVX"):
		return "xvid"
	case containsAny(upper, "MPEG2", "MPEG-2"):
		return "mpeg2"
	}
	return ""
}

func parseAudio(upper string) (string, bool) {
	atmos := strings.Contains(upper, "ATMOS")
	// Fix spaced variants: DTS-HD MA etc are contiguous enough.
	switch {
	case containsAny(upper, "TRUEHD", "TRUE-HD", "TRUE HD"):
		return "truehd", atmos
	case containsAny(upper, "DTS-HD", "DTSHD", "DTS HD", "DTS-HDMA", "DTSX", "DTS-X"):
		return "dtshd", atmos
	case containsAny(upper, "EAC3", "E-AC3", "E AC3", "DDP", "DD+", " DD PLUS"):
		return "eac3", atmos
	case strings.Contains(upper, " DTS") || strings.Contains(upper, "-DTS"):
		return "dts", atmos
	case containsAny(upper, " FLAC"):
		return "flac", atmos
	case containsAny(upper, " OPUS"):
		return "opus", atmos
	case containsAny(upper, "AC3", " DD5", " DD ", " DD-", "DOLBY DIGITAL"):
		return "ac3", atmos
	case containsAny(upper, " AAC"):
		return "aac", atmos
	case containsAny(upper, " MP3"):
		return "mp3", atmos
	}
	return "", atmos
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

// boundaryKeywords mark the end of the title portion.
var boundaryKeywords = []string{
	"2160P", "1080P", "1080I", "720P", "576P", "480P", "360P", "4K", "UHD",
	"BLURAY", "BLU-RAY", "BDRIP", "BRRIP", "HDDVD", "HD-DVD", "WEB-DL", "WEBDL", "WEBRIP", "WEB", "HDTV",
	"PDTV", "DSR", "DSRIP", "TVRIP", "SATRIP", "DTHRIP", "SDTV",
	"DVDRIP", "DVDR", "DVD5", "DVD9", "DVDSCR", "VHSRIP", "LDRIP", "LASERDISC",
	"REMUX", "X264", "X265", "H264", "H265", "HEVC", "AVC", "AV1", "VP9", "XVID", "DIVX", "MPEG2", "VC1", "VC-1",
	"CAM", "HDCAM", "HDTS", "TELESYNC", "TELECINE", "PROPER", "REPACK", "COMPLETE",
	"SEASON", "SEASONS", "MULTI", "FRENCH", "GERMAN", "SPANISH", "ITALIAN", "HDR", "HDR10",
	"INTERNAL", "LIMITED", "SUBBED", "DUBBED", "NTSC", "PAL",
}

// titleWordKeywords are boundary keywords that are also ordinary title
// words ("The French Dispatch", "Internal Affairs", "Charlottes Web",
// "Cam"); before the release year or episode marker they stay in the title.
var titleWordKeywords = map[string]bool{
	"COMPLETE": true, "SEASON": true, "SEASONS": true, "MULTI": true, "FRENCH": true, "GERMAN": true,
	"SPANISH": true, "ITALIAN": true, "INTERNAL": true, "LIMITED": true,
	"SUBBED": true, "DUBBED": true, "CAM": true, "WEB": true, "PAL": true,
}

func extractTitle(spaced string, yearAt, markerAt, floor int, info *core.ReleaseInfo) string {
	cut := len(spaced)
	consider := func(idx int) {
		if idx > 0 && idx < cut {
			cut = idx
		}
	}
	// Year boundary (the occurrence parseYearAt chose) and episode/season marker.
	consider(yearAt)
	consider(markerAt)
	for _, kw := range boundaryKeywords {
		// Keywords are whole tokens ("Multiverse" is not MULTI, "Scam" is
		// not CAM), and words before the release year or episode marker
		// belong to the title ("Charlottes Web 2006", "Internal Affairs
		// 1990", "The Spanish Princess S01E01").
		// Only words that also occur in titles get that protection:
		// resolutions, sources, codecs and PROPER/REPACK always end the
		// title ("Show.Name.720p.S01E01").
		if idx := tokenIndex(spaced, kw); idx > 0 && (idx > floor || !titleWordKeywords[kw] || kw == "COMPLETE" && namesPack(spaced[idx+len(kw):])) {
			consider(idx)
		}
	}
	title := spaced[:cut]
	title = strings.Trim(title, " -")
	// Hyphens between words in titles are usually separators too, except
	// keep words joined: turn "Spider-Man" stays, "Title -" trimmed above.
	fields := strings.Fields(title)
	// Drop a leading bracket-group token, e.g. [HorribleSubs] Title ...
	if info.Group != "" && len(fields) > 1 {
		gf := strings.Fields(spacify(info.Group))
		if len(gf) > 0 && len(fields) > len(gf) && strings.EqualFold(strings.Join(fields[:len(gf)], " "), strings.Join(gf, " ")) {
			fields = fields[len(gf):]
		}
	}
	// Drop a stray leading "-" separator left by "[Group] Title - S01E01".
	for len(fields) > 0 && fields[0] == "-" {
		fields = fields[1:]
	}
	title = strings.Trim(strings.Join(fields, " "), " -")
	return title
}

// SeriesYearMatches distinguishes same-named series and reboots. A year
// explicitly present in a TV release identifies the series, not the episode's
// air year. Releases omitting a year remain eligible.
func SeriesYearMatches(releaseYear, showYear int) bool {
	return releaseYear == 0 || showYear == 0 || releaseYear == showYear
}
