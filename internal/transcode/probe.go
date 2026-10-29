package transcode

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/braedonsaunders/ether/internal/core"
	"github.com/braedonsaunders/ether/internal/langtag"
)

// probeAttempt is one ffprobe pass. The first is small so a remote stream
// is probed in about a second; the second only runs when the first left
// stream parameters missing.
type probeAttempt struct {
	probesize       int64 // bytes
	analyzeDuration time.Duration
	timeout         time.Duration
}

var probeAttempts = []probeAttempt{
	{probesize: 2 << 20, analyzeDuration: 1500 * time.Millisecond, timeout: 20 * time.Second},
	{probesize: 50 << 20, analyzeDuration: 10 * time.Second, timeout: 60 * time.Second},
}

// isNetworkURL reports whether ffmpeg's HTTP protocol options apply.
func isNetworkURL(u string) bool {
	return strings.HasPrefix(u, "http://") || strings.HasPrefix(u, "https://")
}

// httpInputArgs are protocol options for loopback HTTP sources: generous
// read timeouts (Usenet fetches stall), reconnect on dropped connections.
func httpInputArgs(input string, reconnect bool) []string {
	if !isNetworkURL(input) {
		return nil
	}
	args := []string{"-rw_timeout", "30000000", "-user_agent", "Ether"}
	if reconnect {
		args = append(args, "-reconnect", "1", "-reconnect_on_network_error", "1", "-reconnect_delay_max", "4")
	}
	return args
}

func probeArgs(input string, a probeAttempt) []string {
	return probeArgsMode(input, a, false)
}

func probeArgsMode(input string, a probeAttempt, live bool) []string {
	args := []string{"-hide_banner", "-v", "error", "-print_format", "json", "-show_format", "-show_streams", "-show_chapters", "-show_data",
		"-probesize", strconv.FormatInt(a.probesize, 10),
		"-analyzeduration", strconv.FormatInt(a.analyzeDuration.Microseconds(), 10),
	}
	args = append(args, inputSafetyArgs(input, live)...)
	args = append(args, httpInputArgs(input, false)...)
	return append(args, "-i", input)
}

// probe runs ffprobe, retrying with a larger probesize when the quick pass
// leaves stream parameters unknown.
func probe(ctx context.Context, r Runner, ffprobe, input string) (*core.MediaInfo, error) {
	return probeMode(ctx, r, ffprobe, input, false)
}

func probeMode(ctx context.Context, r Runner, ffprobe, input string, live bool) (*core.MediaInfo, error) {
	var best *core.MediaInfo
	var lastErr error
	for i, a := range probeAttempts {
		info, err := probeOnceMode(ctx, r, ffprobe, input, a, live)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			lastErr = err
			continue
		}
		best = info
		if probeCompleteMode(info, live) {
			return info, nil
		}
		if i < len(probeAttempts)-1 {
			lastErr = errors.New("incomplete stream parameters")
		}
	}
	if best != nil {
		return best, nil
	}
	return nil, lastErr
}

func probeOnceMode(ctx context.Context, r Runner, ffprobe, input string, a probeAttempt, live bool) (*core.MediaInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, a.timeout)
	defer cancel()
	var stdout, stderr bytes.Buffer
	err := r.Run(ctx, Command{Path: ffprobe, Args: probeArgsMode(input, a, live), Stdout: &stdout, Stderr: &stderr})
	if err != nil {
		return nil, fmt.Errorf("transcode: ffprobe: %w: %s", err, tail(stderr.String(), 400))
	}
	return ParseProbe(stdout.Bytes())
}

// probeComplete reports whether every stream ffmpeg needs parameters for has
// them; otherwise a larger probe is worth the time.
func probeComplete(m *core.MediaInfo) bool {
	return probeCompleteMode(m, false)
}

func probeCompleteMode(m *core.MediaInfo, live bool) bool {
	if m.Duration <= 0 && !live {
		return false
	}
	hasAV := false
	for _, s := range m.Streams {
		switch s.Type {
		case "video":
			hasAV = true
			if s.Width == 0 || s.Height == 0 || s.Codec == "" {
				return false
			}
		case "audio":
			hasAV = true
			if s.Channels == 0 || s.SampleRate == 0 || s.Codec == "" {
				return false
			}
		}
	}
	return hasAV
}

// ffprobe JSON (only what Ether uses).
type ffprobeChapter struct {
	ID    int64             `json:"id"`
	Start string            `json:"start_time"`
	End   string            `json:"end_time"`
	Tags  map[string]string `json:"tags"`
}

type ffprobeOutput struct {
	Chapters []ffprobeChapter `json:"chapters"`
	Streams  []ffprobeStream  `json:"streams"`
	Format   ffprobeFormat    `json:"format"`
}

type ffprobeStream struct {
	FieldOrder     string            `json:"field_order"`
	Extradata      string            `json:"extradata"`
	Index          int               `json:"index"`
	Level          int               `json:"level"`
	CodecName      string            `json:"codec_name"`
	CodecType      string            `json:"codec_type"`
	CodecTagString string            `json:"codec_tag_string"`
	Profile        string            `json:"profile"`
	Width          int               `json:"width"`
	Height         int               `json:"height"`
	PixFmt         string            `json:"pix_fmt"`
	ColorTransfer  string            `json:"color_transfer"`
	ColorPrimaries string            `json:"color_primaries"`
	ColorSpace     string            `json:"color_space"`
	ColorRange     string            `json:"color_range"`
	SAR            string            `json:"sample_aspect_ratio"`
	RFrameRate     string            `json:"r_frame_rate"`
	AvgFrameRate   string            `json:"avg_frame_rate"`
	BitsPerRaw     string            `json:"bits_per_raw_sample"`
	SampleRate     string            `json:"sample_rate"`
	Channels       int               `json:"channels"`
	ChannelLayout  string            `json:"channel_layout"`
	BitRate        string            `json:"bit_rate"`
	Disposition    map[string]int    `json:"disposition"`
	Tags           map[string]string `json:"tags"`
	SideDataList   []map[string]any  `json:"side_data_list"`
}

type ffprobeFormat struct {
	FormatName string            `json:"format_name"`
	Duration   string            `json:"duration"`
	Size       string            `json:"size"`
	BitRate    string            `json:"bit_rate"`
	Tags       map[string]string `json:"tags"`
}

// ParseProbe maps ffprobe -print_format json -show_format -show_streams
// output to MediaInfo.
func ParseProbe(b []byte) (*core.MediaInfo, error) {
	var out ffprobeOutput
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, fmt.Errorf("transcode: parse ffprobe json: %w", err)
	}
	if out.Format.FormatName == "" && len(out.Streams) == 0 {
		return nil, errors.New("transcode: ffprobe returned no format or streams")
	}
	m := &core.MediaInfo{
		Container:   NormalizeContainer(out.Format.FormatName),
		Duration:    parseSeconds(out.Format.Duration),
		Size:        atoi64(out.Format.Size),
		BitrateKbps: int(atoi64(out.Format.BitRate) / 1000),
	}
	for _, c := range out.Chapters {
		start, e1 := strconv.ParseFloat(c.Start, 64)
		end, e2 := strconv.ParseFloat(c.End, 64)
		if e1 != nil || e2 != nil || math.IsNaN(start) || math.IsNaN(end) || math.IsInf(start, 0) || math.IsInf(end, 0) || start < 0 || end <= start || end >= float64(math.MaxInt64)/float64(time.Second) {
			continue
		}
		m.Chapters = append(m.Chapters, core.Chapter{ID: c.ID, Title: tag(c.Tags, "title"), Start: time.Duration(start * float64(time.Second)), End: time.Duration(end * float64(time.Second))})
	}
	for _, s := range out.Streams {
		si, ok := mapStream(s)
		if !ok {
			continue
		}
		m.Streams = append(m.Streams, si)
	}
	if m.BitrateKbps == 0 && m.Size > 0 && m.Duration > 0 {
		m.BitrateKbps = int(float64(m.Size*8) / m.Duration.Seconds() / 1000)
	}
	return m, nil
}

func mapStream(s ffprobeStream) (core.StreamInfo, bool) {
	si := core.StreamInfo{Index: s.Index, Type: s.CodecType}
	switch s.CodecType {
	case "video":
		if s.Disposition["attached_pic"] == 1 || isImageCodec(s.CodecName) {
			return si, false // cover art
		}
	case "audio", "subtitle":
	default:
		return si, false // data, attachment (fonts)
	}
	si.Codec = NormalizeCodec(s.CodecName)
	si.Profile = s.Profile
	si.Level = s.Level
	si.Language = normalizeLanguage(tag(s.Tags, "language"))
	si.Title = tag(s.Tags, "title")
	si.Default = s.Disposition["default"] == 1
	si.Forced = s.Disposition["forced"] == 1
	si.BitrateKbps = int(atoi64(s.BitRate) / 1000)
	if si.BitrateKbps == 0 {
		// mkvmerge statistics tags
		for _, k := range []string{"BPS", "BPS-eng"} {
			if v := tag(s.Tags, k); v != "" {
				si.BitrateKbps = int(atoi64(v) / 1000)
				break
			}
		}
	}
	switch s.CodecType {
	case "video":
		si.Width, si.Height = s.Width, s.Height
		si.Interlaced = s.FieldOrder != "" && s.FieldOrder != "unknown" && s.FieldOrder != "progressive"
		avg, nominal := parseRate(s.AvgFrameRate), parseRate(s.RFrameRate)
		// A material rate disagreement is evidence against a steady frame clock.
		si.VariableFrameRate = avg > 0 && nominal > 0 && (avg/nominal < 0.999 || avg/nominal > 1.001)
		si.FrameRate = avg
		if si.FrameRate == 0 {
			si.FrameRate = parseRate(s.RFrameRate)
		}
		si.BitDepth = bitDepth(s)
		si.ColorTransfer = s.ColorTransfer
		si.SampleAspectRatio = probedSAR(s.SAR)
		si.ColorRange = probedColour(s.ColorRange)
		si.ColorPrimaries = probedColour(s.ColorPrimaries)
		si.ColorSpace = probedColour(s.ColorSpace)
		si.DolbyVision, si.DolbyVisionProfile = dolbyVision(s)
		for _, sd := range s.SideDataList {
			if major, ok := sd["dv_version_major"].(float64); ok {
				minor, _ := sd["dv_version_minor"].(float64)
				si.DolbyVisionVersion = fmt.Sprintf("%d.%d", int(major), int(minor))
			}
			for _, field := range []struct {
				key string
				dst **bool
			}{
				{"bl_present_flag", &si.DolbyVisionBLPresent},
				{"el_present_flag", &si.DolbyVisionELPresent},
				{"rpu_present_flag", &si.DolbyVisionRPUPresent},
			} {
				if value, ok := sd[field.key].(float64); ok {
					present := value != 0
					*field.dst = &present
				}
			}
			if level, ok := sd["dv_level"].(float64); ok {
				si.DolbyVisionLevel = int(level)
			}
			if compat, ok := sd["dv_bl_signal_compatibility_id"].(float64); ok {
				si.DolbyVisionCompatibilityID = int(compat)
			}
		}
		extra := probeExtradata(s.Extradata)
		if si.Codec == "h264" && len(extra) >= 4 && extra[0] == 1 {
			si.CodecString = fmt.Sprintf("avc1.%02x%02x%02x", extra[1], extra[2], extra[3])
		}
		if si.Codec == "hevc" && len(extra) >= 13 && extra[0] == 1 {
			si.CodecString = "hvc1." + hevcCodecs(extra)
		}
	case "audio":
		si.Channels = s.Channels
		si.ChannelLayout = s.ChannelLayout
		si.SampleRate = int(atoi64(s.SampleRate))
	}
	return si, true
}

func tag(tags map[string]string, key string) string {
	if v, ok := tags[key]; ok {
		return strings.TrimSpace(v)
	}
	for k, v := range tags {
		if strings.EqualFold(k, key) {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

// normalizeLanguage keeps the form the probes have always reported in
// StreamInfo.Language (legacy ISO 639-2 where the source carries it),
// dropping undetermined/unknown and anything langtag rejects as
// garbage. It is the shared entry point for the ffprobe tag, the
// Matroska legacy Language element and the MP4 mdhd code.
func normalizeLanguage(l string) string {
	if langtag.Normalize(l) == "" {
		return ""
	}
	return strings.ToLower(strings.TrimSpace(l))
}

// normalizeBCP47Language converts a BCP 47 tag (Matroska
// LanguageBCP47) to the form stored in StreamInfo.Language via
// langtag: Portuguese and Chinese keep their region (pt-br, zh-tw —
// script is parsed, never read as region, so zh-Hant-TW folds to
// zh-tw), every other language folds to the ISO 639-2 form the probes
// have always reported. "" means the tag is garbage and the caller
// keeps its legacy value.
func normalizeBCP47Language(tag string) string {
	n := langtag.Normalize(tag)
	if n == "" {
		return ""
	}
	if strings.Contains(n, "-") {
		return n // pt/zh regional form
	}
	if three, ok := iso6391to2[n]; ok {
		return three
	}
	return n
}

func isImageCodec(c string) bool {
	switch c {
	case "mjpeg", "png", "bmp", "gif", "webp":
		return true
	}
	return false
}

// probedSAR keeps a signalled sample aspect ratio; ffprobe reports 0:1
// (or nothing) when neither stream nor container carries one.
func probedSAR(s string) string {
	n, d, ok := strings.Cut(s, ":")
	if !ok || atoi64(n) <= 0 || atoi64(d) <= 0 {
		return ""
	}
	a, b := reduce(int(atoi64(n)), int(atoi64(d)))
	return strconv.Itoa(a) + ":" + strconv.Itoa(b)
}

// probedColour drops ffprobe's placeholders for unsignalled colour facts.
func probedColour(s string) string {
	switch s {
	case "", "unknown", "unspecified", "reserved":
		return ""
	}
	return s
}

var pixDepthRE = regexp.MustCompile(`p(9|10|12|14|16)(le|be)?$`)

func bitDepth(s ffprobeStream) int {
	if n := int(atoi64(s.BitsPerRaw)); n > 0 {
		return n
	}
	if m := pixDepthRE.FindStringSubmatch(s.PixFmt); m != nil {
		n, _ := strconv.Atoi(m[1])
		return n
	}
	if s.PixFmt != "" {
		return 8
	}
	return 0
}

// dolbyVision detects DV from the DOVI configuration record side data, or
// from the mp4 sample entry tag (dvh1/dvhe/dva1/dvav) when the record is
// absent.
func dolbyVision(s ffprobeStream) (bool, int) {
	for _, sd := range s.SideDataList {
		t, _ := sd["side_data_type"].(string)
		if !strings.Contains(strings.ToLower(t), "dovi") && !strings.Contains(strings.ToLower(t), "dolby vision") {
			continue
		}
		if !strings.Contains(strings.ToLower(t), "configuration") {
			continue // per-frame RPU metadata
		}
		prof := 0
		if v, ok := sd["dv_profile"].(float64); ok {
			prof = int(v)
		}
		return true, prof
	}
	switch strings.ToLower(s.CodecTagString) {
	case "dvh1", "dvhe", "dva1", "dvav", "dav1":
		return true, 0
	}
	return false, 0
}

// NormalizeContainer maps an ffprobe format_name to Ether's container name.
func NormalizeContainer(formatName string) string {
	names := strings.Split(formatName, ",")
	first := ""
	if len(names) > 0 {
		first = names[0]
	}
	switch {
	case first == "matroska" || contains(names, "matroska"):
		return "mkv"
	case first == "webm":
		return "webm"
	case first == "mov" || contains(names, "mp4"):
		return "mp4"
	case first == "mpegts":
		return "mpegts"
	case first == "avi":
		return "avi"
	case first == "asf":
		return "wmv"
	case first == "mpeg" || first == "mpegvideo":
		return "mpeg"
	case first == "flv":
		return "flv"
	case first == "ogg":
		return "ogg"
	case first == "hls":
		return "hls"
	}
	return first
}

// NormalizeCodec maps ffprobe codec names to Ether/Plex codec names.
func NormalizeCodec(c string) string {
	switch c {
	case "h265":
		return "hevc"
	case "libdav1d", "libaom-av1":
		return "av1"
	case "mpeg2video":
		return "mpeg2video"
	case "dca":
		return "dts"
	case "mlp":
		return "truehd"
	case "hdmv_pgs_subtitle":
		return "pgs"
	case "dvd_subtitle":
		return "vobsub"
	case "dvb_subtitle":
		return "dvbsub"
	case "ssa":
		return "ass"
	case "text":
		return "subrip"
	}
	if strings.HasPrefix(c, "pcm_") {
		return "pcm"
	}
	return c
}

// IsImageSubtitle reports whether a subtitle codec is bitmap-based and so
// must be burned in to be shown in HLS.
func IsImageSubtitle(codec string) bool {
	switch codec {
	case "pgs", "vobsub", "dvbsub", "xsub", "hdmv_pgs_subtitle", "dvd_subtitle", "dvb_subtitle":
		return true
	}
	return false
}

// IsTextSubtitle reports whether a subtitle codec can be converted to
// WebVTT.
func IsTextSubtitle(codec string) bool {
	switch codec {
	case "subrip", "srt", "ass", "ssa", "webvtt", "mov_text", "text", "microdvd", "subviewer", "jacosub", "sami", "realtext", "stl", "ttml":
		return true
	}
	return false
}

// maxMediaSeconds bounds a probed duration: anything longer is a corrupt
// header, and as a Duration it would overflow (or, saturated, plan billions
// of segments).
const maxMediaSeconds = 30 * 24 * 3600

func parseSeconds(s string) time.Duration {
	f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil || f <= 0 || math.IsNaN(f) || math.IsInf(f, 0) || f > maxMediaSeconds {
		return 0
	}
	return time.Duration(f * float64(time.Second))
}

func parseRate(s string) float64 {
	num, den, ok := strings.Cut(s, "/")
	if !ok {
		f, _ := strconv.ParseFloat(s, 64)
		return f
	}
	n, err1 := strconv.ParseFloat(num, 64)
	d, err2 := strconv.ParseFloat(den, 64)
	if err1 != nil || err2 != nil || d == 0 {
		return 0
	}
	return math.Round(n/d*1000) / 1000
}

func atoi64(s string) int64 {
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
		if err != nil {
			return 0
		}
		return int64(f)
	}
	return n
}

func tail(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) > n {
		return "..." + s[len(s)-n:]
	}
	return s
}

// ffprobe prints extradata as offset-prefixed hex rows followed by ASCII.
func probeExtradata(raw string) []byte {
	if len(raw) > 256<<10 {
		return nil
	}
	var out []byte
	for _, line := range strings.Split(raw, "\n") {
		_, data, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		data = strings.TrimSpace(data)
		if hexPart, _, ok := strings.Cut(data, "  "); ok {
			data = hexPart
		}
		data = strings.ReplaceAll(data, " ", "")
		b, err := hex.DecodeString(data)
		if err != nil {
			return nil
		}
		out = append(out, b...)
	}
	return out
}
