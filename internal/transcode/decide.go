package transcode

import (
	"fmt"
	"strings"

	"github.com/braedonsaunders/ether/internal/core"
)

// HLS segment containers reported in core.Decision.Container.
const (
	ContainerMPEGTS   = "mpegts"
	ContainerFMP4     = "fmp4"
	ContainerMatroska = "mkv"
)

// Policy is the server-side input to Decide that is not per request.
type Policy struct {
	// RemoteMaxBitrateKbps caps streams to clients outside the LAN; 0 = none.
	RemoteMaxBitrateKbps int
	// HEVCEncoder is true when HEVC can be encoded in real time (Manager
	// sets it only for hardware encoders). It is required to keep HDR in a
	// transcode; without it HDR is tone-mapped to SDR H.264.
	HEVCEncoder bool
	// HEVCHardware is true when HEVC encoding is hardware accelerated, which
	// makes HEVC a good target for 4K transcodes.
	HEVCHardware bool
}

// Dynamic range of a video stream.
const (
	rangeSDR   = "sdr"
	rangeHDR10 = "hdr10"
	rangeHLG   = "hlg"
	rangeDV    = "dv"
)

// dynamicRange classifies a video stream.
func dynamicRange(v *core.StreamInfo) string {
	if v == nil {
		return rangeSDR
	}
	if v.DolbyVision {
		return rangeDV
	}
	switch v.ColorTransfer {
	case "smpte2084":
		return rangeHDR10
	case "arib-std-b67":
		return rangeHLG
	}
	return rangeSDR
}

// dvBaseLayer is the dynamic range a non-DV client sees when it plays a DV
// stream's base layer: profile 7/8 carry an HDR10/HLG/SDR compatible base;
// profile 5 (IPTPQc2) has none. ok is false when no usable base exists.
func dvBaseLayer(v *core.StreamInfo) (string, bool) {
	if v.DolbyVisionProfile == 5 {
		return "", false
	}
	switch v.ColorTransfer {
	case "smpte2084":
		return rangeHDR10, true
	case "arib-std-b67":
		return rangeHLG, true
	case "bt709", "bt470bg", "smpte170m", "bt2020-10", "bt2020-12", "iec61966-2-1", "gamma22", "gamma28":
		return rangeSDR, true
	}
	if v.DolbyVisionProfile == 7 || v.DolbyVisionProfile == 8 {
		return rangeHDR10, true // most common: 7.6 / 8.1 with HDR10 base
	}
	return "", false
}

// isHDR reports whether the stream needs an HDR display (or tone mapping).
func isHDR(v *core.StreamInfo) bool { return dynamicRange(v) != rangeSDR }

// encodedRange is the dynamic range of the picture ffmpeg decodes for an
// encode: a Dolby Vision stream decodes to its base layer (SDR, HDR10 or
// HLG); profile 5, which has none, stays rangeDV. Tone mapping an SDR base
// layer, or tagging it HDR10, would wreck its colours.
func encodedRange(v *core.StreamInfo) string {
	r := dynamicRange(v)
	if r == rangeDV {
		if base, ok := dvBaseLayer(v); ok {
			return base
		}
	}
	return r
}

// keepsHDR reports whether an encode can keep the decoded picture's HDR
// (HEVC with HDR10/HLG metadata) rather than tone map it.
func keepsHDR(v *core.StreamInfo) bool {
	r := encodedRange(v)
	return r == rangeHDR10 || r == rangeHLG
}

// rangeOK checks the video's dynamic range against the client. It returns
// "" when it can be shown as is, else the reason it cannot.
func rangeOK(v *core.StreamInfo, caps core.ClientCaps) string {
	switch dynamicRange(v) {
	case rangeSDR:
		return ""
	case rangeHDR10, rangeHLG:
		if caps.HDR10 {
			return ""
		}
		return fmt.Sprintf("client lacks HDR (%s source)", hdrLabel(v))
	case rangeDV:
		if SupportsDVProfile(caps, v.DolbyVisionProfile) {
			return ""
		}
		base, ok := dvBaseLayer(v)
		if !ok {
			return fmt.Sprintf("client lacks Dolby Vision (profile %d has no compatible base layer)", v.DolbyVisionProfile)
		}
		if base == rangeSDR || caps.HDR10 {
			return ""
		}
		return "client lacks Dolby Vision and HDR"
	}
	return ""
}

func hdrLabel(v *core.StreamInfo) string {
	switch dynamicRange(v) {
	case rangeHDR10:
		return "HDR10"
	case rangeHLG:
		return "HLG"
	case rangeDV:
		if v.DolbyVisionProfile > 0 {
			return fmt.Sprintf("Dolby Vision profile %d", v.DolbyVisionProfile)
		}
		return "Dolby Vision"
	}
	return "SDR"
}

// hlsVideoOK reports whether a video codec can be carried in HLS segments.
func hlsVideoOK(codec string) bool {
	switch codec {
	case "h264", "hevc", "av1", "vp9":
		return true
	}
	return false
}

// hlsAudioOK reports whether an audio codec can be carried in HLS segments
// (in at least one of TS or fMP4).
func hlsAudioOK(codec string) bool {
	switch codec {
	case "aac", "ac3", "eac3", "mp3", "flac", "opus", "alac":
		return true
	}
	return false
}

// segmentContainer picks TS or fMP4 segments. Apple requires fMP4 for HEVC
// in HLS; AV1/VP9 and FLAC/Opus only exist in fMP4.
func segmentContainer(videoCodec, audioCodec string) string {
	switch videoCodec {
	case "hevc", "av1", "vp9":
		return ContainerFMP4
	}
	switch audioCodec {
	case "flac", "opus", "alac":
		return ContainerFMP4
	}
	return ContainerMPEGTS
}

// pickAudio returns the selected audio stream: the requested index when it
// is an audio stream, else the default-flagged one, else the first.
func pickAudio(info core.MediaInfo, idx int) *core.StreamInfo {
	var first, def *core.StreamInfo
	for i := range info.Streams {
		s := &info.Streams[i]
		if s.Type != "audio" {
			continue
		}
		if idx >= 0 && s.Index == idx {
			return s
		}
		if first == nil {
			first = s
		}
		if s.Default && def == nil {
			def = s
		}
	}
	if def != nil {
		return def
	}
	return first
}

// pickSubtitle returns the requested subtitle stream, or nil.
func pickSubtitle(info core.MediaInfo, idx int) *core.StreamInfo {
	if idx < 0 {
		return nil
	}
	for i := range info.Streams {
		s := &info.Streams[i]
		if s.Type == "subtitle" && s.Index == idx {
			return s
		}
	}
	return nil
}

// audioTarget picks the codec audio is transcoded to: keep surround with
// E-AC-3/AC-3 when the client has it, else stereo AAC.
func audioTarget(caps core.ClientCaps, src *core.StreamInfo) string {
	ch := 2
	if src != nil && src.Channels > 0 {
		ch = src.Channels
	}
	if ch > 2 {
		switch {
		case caps.SupportsAudio("eac3"):
			return "eac3"
		case caps.SupportsAudio("ac3"):
			return "ac3"
		}
		return "aac"
	}
	switch {
	case caps.SupportsAudio("aac") || len(caps.AudioCodecs) == 0:
		return "aac"
	case caps.SupportsAudio("ac3"):
		return "ac3"
	case caps.SupportsAudio("eac3"):
		return "eac3"
	case caps.SupportsAudio("mp3"):
		return "mp3"
	case caps.SupportsAudio("opus"):
		return "opus"
	}
	return "aac"
}

// outputChannels is the channel count an audio encoder produces.
func outputChannels(codec string, src *core.StreamInfo) int {
	ch := 2
	if src != nil && src.Channels > 0 {
		ch = src.Channels
	}
	switch codec {
	case "ac3", "eac3":
		if ch > 6 {
			return 6
		}
		return ch
	}
	if ch > 2 {
		return 2
	}
	return ch
}

// audioBitrateKbps is the encoder bitrate for a target codec and channels.
func audioBitrateKbps(codec string, channels int) int {
	switch codec {
	case "ac3":
		if channels > 2 {
			return 640
		}
		return 192
	case "eac3":
		if channels > 2 {
			return 640
		}
		return 224
	case "opus":
		if channels > 2 {
			return 320
		}
		return 128
	case "mp3":
		return 192
	}
	if channels > 2 {
		return 384
	}
	return 192
}

// copiedAudioKbps estimates a copied audio stream's bitrate.
func copiedAudioKbps(a *core.StreamInfo) int {
	if a == nil {
		return 0
	}
	if a.BitrateKbps > 0 {
		return a.BitrateKbps
	}
	return audioBitrateKbps(a.Codec, a.Channels)
}

// heightForBitrate is the largest output height worth encoding at a video
// bitrate (the "ladder").
func heightForBitrate(kbps int, codec string) int {
	f := 1.0
	if codec == "hevc" {
		f = 0.6
	}
	switch {
	case float64(kbps) >= 16000*f:
		return 2160
	case float64(kbps) >= 5000*f:
		return 1080
	case float64(kbps) >= 2000*f:
		return 720
	case float64(kbps) >= 1000*f:
		return 480
	}
	return 360
}

// bitrateCeiling is the bitrate past which a height gains little.
func bitrateCeiling(height int, codec string) int {
	var k int
	switch {
	case height > 1440:
		k = 40000
	case height > 1080:
		k = 16000
	case height > 720:
		k = 10000
	case height > 480:
		k = 4000
	case height > 360:
		k = 2000
	default:
		k = 1000
	}
	if codec == "hevc" {
		k = k * 6 / 10
	}
	return k
}

func minPositive(xs ...int) int {
	m := 0
	for _, x := range xs {
		if x > 0 && (m == 0 || x < m) {
			m = x
		}
	}
	return m
}

func clientName(caps core.ClientCaps) string {
	if caps.Product != "" {
		return caps.Product
	}
	if caps.Platform != "" {
		return caps.Platform
	}
	return "client"
}

// issueContext groups the already-computed limits and flags collectIssues
// reads, so the helper stays pure without a long parameter list.
type issueContext struct {
	caps         core.ClientCaps
	heightCap    int
	bitrateCap   int
	burn         bool
	audioFilters []string
}

// collectIssues lists, in order, what blocks keeping the video, the audio
// and the stream as they are.
func collectIssues(info core.MediaInfo, v, a, sub *core.StreamInfo, ctx issueContext) (videoIssues, audioIssues, otherIssues []string) {
	caps := ctx.caps
	if v != nil {
		if len(caps.VideoCodecs) > 0 && !caps.SupportsVideo(v.Codec) {
			videoIssues = append(videoIssues, fmt.Sprintf("video codec %s not supported", v.Codec))
		}
		if profiles := caps.VideoProfiles[v.Codec]; len(profiles) > 0 {
			supported := false
			for _, p := range profiles {
				if strings.EqualFold(p, v.Profile) {
					supported = true
				}
			}
			if !supported {
				videoIssues = append(videoIssues, fmt.Sprintf("video profile %s not supported", v.Profile))
			}
		}
		if level := caps.VideoLevels[v.Codec]; level > 0 && v.Level > level {
			videoIssues = append(videoIssues, fmt.Sprintf("video level %d exceeds %d", v.Level, level))
		}
		if ctx.heightCap > 0 && v.Height > ctx.heightCap {
			videoIssues = append(videoIssues, fmt.Sprintf("%dp exceeds %dp limit", v.Height, ctx.heightCap))
		}
		if r := rangeOK(v, caps); r != "" {
			videoIssues = append(videoIssues, r)
		}
	}
	if a != nil && len(caps.AudioCodecs) > 0 && !caps.SupportsAudio(a.Codec) {
		audioIssues = append(audioIssues, fmt.Sprintf("audio codec %s not supported", a.Codec))
	}
	if a != nil && len(ctx.audioFilters) > 0 {
		audioIssues = append(audioIssues, "audio processing requested")
	}
	if ctx.bitrateCap > 0 && info.BitrateKbps > ctx.bitrateCap {
		otherIssues = append(otherIssues, fmt.Sprintf("bitrate %d kbps exceeds %d kbps limit", info.BitrateKbps, ctx.bitrateCap))
	}
	if ctx.burn {
		otherIssues = append(otherIssues, fmt.Sprintf("%s subtitles must be burned in", sub.Codec))
	}
	return videoIssues, audioIssues, otherIssues
}

// planAudio is the audio plan shared by remux and transcode: copy when the
// codec is supported and carried in HLS, else transcode to the client's
// target; on a narrow connection, high-bitrate audio is converted to
// stereo AAC to preserve room for video.
func planAudio(a *core.StreamInfo, caps core.ClientCaps, audioIssues []string, bitrateCap int) (audioCopy bool, audioCodec string, audioKbps int, audioNote string) {
	audioCopy = a == nil || (len(audioIssues) == 0 && hlsAudioOK(a.Codec))
	if a != nil {
		if audioCopy {
			audioCodec = a.Codec
			audioKbps = copiedAudioKbps(a)
		} else {
			audioCodec = audioTarget(caps, a)
			ch := outputChannels(audioCodec, a)
			audioKbps = audioBitrateKbps(audioCodec, ch)
			audioNote = fmt.Sprintf("audio %s %s -> %s %dch", a.Codec, channelLabel(a), audioCodec, ch)
			if len(audioIssues) == 0 {
				audioNote += " (not carried in HLS)"
			}
		}
	}
	if a != nil && bitrateCap > 0 && bitrateCap < 3000 && audioKbps > bitrateCap/4 &&
		(caps.SupportsAudio("aac") || len(caps.AudioCodecs) == 0) {
		audioCopy, audioCodec = false, "aac"
		audioKbps = audioBitrateKbps(audioCodec, 2)
		audioNote = "audio converted to stereo AAC to fit connection bitrate"
	}
	return audioCopy, audioCodec, audioKbps, audioNote
}

// Decide chooses direct play, remux into HLS, or full transcode, in that
// order of preference, and explains why in Decision.Reason.
func Decide(info core.MediaInfo, caps core.ClientCaps, opts core.DecideOptions, pol Policy) core.Decision {
	v := info.Video()
	a := pickAudio(info, opts.AudioStream)
	sub := pickSubtitle(info, opts.SubtitleStream)

	d := core.Decision{AudioStream: -1, SubtitleStream: -1}
	if a != nil {
		d.AudioStream = a.Index
	}
	if sub != nil {
		d.SubtitleStream = sub.Index
	}

	remoteCap := 0
	if caps.Remote {
		remoteCap = pol.RemoteMaxBitrateKbps
	}
	bitrateCap := minPositive(opts.MaxBitrateKbps, caps.MaxBitrateKbps, remoteCap)
	heightCap := minPositive(caps.MaxHeight, opts.MaxHeight)
	// Text subtitles always remain sidecars, including legacy burn requests.
	burn := sub != nil && IsImageSubtitle(sub.Codec) && (opts.BurnSubtitles || !supportsSubtitle(caps, sub.Codec))
	if a != nil && len(opts.AudioFilters) > 0 {
		d.AudioFilters = append([]string(nil), opts.AudioFilters...)
	}

	// What blocks keeping the video and audio as they are.
	videoIssues, audioIssues, otherIssues := collectIssues(info, v, a, sub, issueContext{
		caps: caps, heightCap: heightCap, bitrateCap: bitrateCap, burn: burn, audioFilters: d.AudioFilters,
	})
	containerOK := len(caps.Containers) == 0 || caps.SupportsContainer(info.Container)
	name := clientName(caps)

	// Plex's native tvOS player can accept profile 8.1 in its decision and
	// fail to open it. Profile 7 additionally carries Blu-ray enhancement
	// data this client cannot use. Explicitly deliver the compatible HDR10
	// base at its original resolution through a copy-only mux.
	hdr10Base := appleTVHDR10Base(v, caps)

	// 1. Direct play.
	if opts.DirectPlay && !hdr10Base && containerOK && len(videoIssues) == 0 && len(audioIssues) == 0 && len(otherIssues) == 0 {
		d.Kind = core.DecisionDirectPlay
		d.VideoCopy, d.AudioCopy = true, true
		d.Container = info.Container
		d.BitrateKbps = info.BitrateKbps
		if v != nil {
			d.VideoCodec = v.Codec
			d.MaxHeight = v.Height
		}
		if a != nil {
			d.AudioCodec = a.Codec
		}
		d.Reason = fmt.Sprintf("direct play: %s supports %s", name, describeSource(info, v, a))
		if v != nil && v.DolbyVision && !SupportsDVProfile(caps, v.DolbyVisionProfile) {
			if base, ok := dvBaseLayer(v); ok {
				d.Reason += "; using compatible " + base + " base layer"
			}
		}
		if sub != nil && !IsImageSubtitle(sub.Codec) {
			d.Reason += "; text subtitles served separately"
		}
		return d
	}

	var why []string
	if hdr10Base {
		why = append(why, fmt.Sprintf("native Plex Apple TV compatibility: Dolby Vision profile %d uses its HDR10 base layer", v.DolbyVisionProfile))
	}
	if !opts.DirectPlay {
		why = append(why, "direct play not allowed by client")
	}
	if !containerOK {
		why = append(why, fmt.Sprintf("container %s not supported", info.Container))
	}
	why = append(why, videoIssues...)
	why = append(why, audioIssues...)
	why = append(why, otherIssues...)

	// Audio plan, shared by remux and transcode.
	audioCopy, audioCodec, audioKbps, audioNote := planAudio(a, caps, audioIssues, bitrateCap)

	// 2. Remux: copy video into HLS.
	videoCopyable := v == nil || (len(videoIssues) == 0 && hlsVideoOK(v.Codec))
	if opts.DirectStream && videoCopyable && !burn && (bitrateCap == 0 || info.BitrateKbps <= bitrateCap ||
		// only the audio pushes it over: transcoding audio may fix that
		(!audioCopy && info.BitrateKbps-copiedAudioKbps(a)+audioKbps <= bitrateCap)) {
		d.Kind = core.DecisionRemux
		d.VideoCopy = true
		d.StripDolbyVision = hdr10Base
		d.AudioCopy = audioCopy
		d.AudioCodec = audioCodec
		if v != nil {
			d.VideoCodec = v.Codec
			d.MaxHeight = v.Height
		}
		d.Container = HLSContainer(caps, d.VideoCodec, audioCodec)
		d.BitrateKbps = info.BitrateKbps
		if !audioCopy && a != nil {
			d.BitrateKbps = info.BitrateKbps - copiedAudioKbps(a) + audioKbps
		}
		parts := []string{fmt.Sprintf("remux to HLS (%s)", d.Container)}
		if v != nil && v.DolbyVision && !SupportsDVProfile(caps, v.DolbyVisionProfile) {
			if base, ok := dvBaseLayer(v); ok {
				parts = append(parts, "using compatible "+base+" base layer")
			}
		}
		if len(why) > 0 {
			parts = append(parts, strings.Join(why, ", "))
		}
		if v != nil {
			parts = append(parts, fmt.Sprintf("video %s copied", v.Codec))
		}
		if audioNote != "" {
			parts = append(parts, audioNote)
		} else if a != nil {
			parts = append(parts, fmt.Sprintf("audio %s copied", a.Codec))
		}
		d.Reason = strings.Join(parts, "; ")
		return d
	}
	if !opts.DirectStream {
		why = append(why, "direct stream not allowed by client")
	} else if v != nil && len(videoIssues) == 0 && !hlsVideoOK(v.Codec) {
		why = append(why, fmt.Sprintf("video codec %s cannot be carried in HLS", v.Codec))
	}

	// 3. Full transcode.
	d.Kind = core.DecisionTranscode
	d.AudioCopy = audioCopy
	d.AudioCodec = audioCodec
	d.BurnSubtitles = burn

	srcH := 1080
	srcRange := rangeSDR
	if v != nil {
		if v.Height > 0 {
			srcH = v.Height
		}
		srcRange = dynamicRange(v)
	}
	keepHDR := false
	if v != nil && isHDR(v) && caps.HDR10 && pol.HEVCEncoder && (caps.SupportsVideo("hevc") || len(caps.VideoCodecs) == 0) {
		switch srcRange {
		case rangeHDR10, rangeHLG:
			keepHDR = true
		case rangeDV:
			if base, ok := dvBaseLayer(v); ok && base != rangeSDR {
				keepHDR = true
			}
		}
	}
	d.ToneMap = v != nil && encodedRange(v) != rangeSDR && !keepHDR
	// A re-encode drops DV RPUs and enhancement layers: kept HDR is the base
	// layer's HDR10/HLG.

	codec := "h264"
	supportsH264 := caps.SupportsVideo("h264") || len(caps.VideoCodecs) == 0
	supportsHEVC := caps.SupportsVideo("hevc") || len(caps.VideoCodecs) == 0
	switch {
	case keepHDR:
		codec = "hevc"
	case !supportsH264 && supportsHEVC && pol.HEVCEncoder:
		codec = "hevc"
	case supportsHEVC && pol.HEVCHardware && minPositive(heightCap, srcH) > 1080:
		codec = "hevc"
	}

	videoBudget := 0
	if bitrateCap > 0 {
		videoBudget = bitrateCap - audioKbps
		if videoBudget < 300 {
			videoBudget = 300
		}
	}
	height := srcH
	if heightCap > 0 && heightCap < height {
		height = heightCap
	}
	if videoBudget > 0 {
		if lh := heightForBitrate(videoBudget, codec); lh < height {
			height = lh
		}
	}
	videoKbps := bitrateCeiling(height, codec)
	if videoBudget > 0 && videoBudget < videoKbps {
		videoKbps = videoBudget
	}
	if src := sourceVideoKbps(info, v, a); src > 0 && height >= srcH {
		// Same resolution: spending more than the source (plus headroom for
		// a less efficient codec) adds nothing.
		limit := src
		if v != nil && v.Codec != codec {
			limit = src * 3 / 2
		}
		if limit < videoKbps {
			videoKbps = limit
		}
	}
	if videoKbps < 200 {
		videoKbps = 200
	}
	d.VideoCodec = codec
	d.MaxHeight = height
	d.BitrateKbps = videoKbps + audioKbps
	d.Container = HLSContainer(caps, codec, audioCodec)

	parts := []string{"transcode"}
	if len(why) > 0 {
		parts = append(parts, strings.Join(why, ", "))
	}
	parts = append(parts, fmt.Sprintf("video -> %s %dp %d kbps", codec, height, videoKbps))
	if d.ToneMap {
		parts = append(parts, fmt.Sprintf("tone-mapping %s to SDR", hdrLabel(v)))
	} else if keepHDR {
		parts = append(parts, "keeping HDR")
	}
	if burn {
		parts = append(parts, fmt.Sprintf("burning in %s subtitles", sub.Codec))
	}
	if audioNote != "" {
		parts = append(parts, audioNote)
	} else if a != nil {
		parts = append(parts, fmt.Sprintf("audio %s copied", a.Codec))
	}
	d.Reason = strings.Join(parts, "; ")
	return d
}

// sourceVideoKbps is the video stream's bitrate, or the total minus audio.
func sourceVideoKbps(info core.MediaInfo, v, a *core.StreamInfo) int {
	if v != nil && v.BitrateKbps > 0 {
		return v.BitrateKbps
	}
	if info.BitrateKbps <= 0 {
		return 0
	}
	n := info.BitrateKbps
	for _, s := range info.Streams {
		if s.Type == "audio" {
			n -= copiedAudioKbps(&s)
		}
	}
	if n <= 0 {
		return info.BitrateKbps
	}
	return n
}

func channelLabel(a *core.StreamInfo) string {
	if a.ChannelLayout != "" {
		return a.ChannelLayout
	}
	if a.Channels > 0 {
		return fmt.Sprintf("%dch", a.Channels)
	}
	return ""
}

func describeSource(info core.MediaInfo, v, a *core.StreamInfo) string {
	parts := []string{info.Container}
	if v != nil {
		s := v.Codec
		if isHDR(v) {
			s += " " + hdrLabel(v)
		}
		parts = append(parts, s)
	}
	if a != nil {
		parts = append(parts, a.Codec)
	}
	return strings.Join(parts, "/")
}

// appleTVHDR10Base is a player quirk, not a hardware certification or a
// blanket HEVC/HDR limit. Profile 5 has no HDR10 base and must not be stripped.
func appleTVHDR10Base(v *core.StreamInfo, caps core.ClientCaps) bool {
	if v == nil || v.Codec != "hevc" || !v.DolbyVision || v.ColorTransfer != "smpte2084" || !caps.HDR10 {
		return false
	}
	// Blu-ray profile 7 has an HDR10 base with compatibility id 6; profile
	// 8.1 uses id 1. Neither requires decoding or reducing that base picture.
	if !(v.DolbyVisionProfile == 7 && v.DolbyVisionCompatibilityID == 6) &&
		!(v.DolbyVisionProfile == 8 && v.DolbyVisionCompatibilityID == 1) {
		return false
	}
	if v.DolbyVisionBLPresent != nil && !*v.DolbyVisionBLPresent {
		return false
	}
	product := strings.ToLower(caps.Product)
	return strings.Contains(product, "plex") && (strings.Contains(product, "apple tv") || strings.EqualFold(caps.Platform, "tvos"))
}
