package transcode

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"time"

	"github.com/braedonsaunders/ether/internal/core"
)

// Native container probing reads Matroska headers straight from the source
// and produces the same MediaInfo ffprobe would, without spawning a process
// or reading media payloads. A Usenet source already holds the first article
// once it is validated, so the probe usually costs no network round trip;
// ffprobe instead reads megabytes of packets and seeks to the file tail.
//
// The result is accepted only when every field playback decisions depend on
// is known exactly. Anything ambiguous returns ErrProbeIncomplete so the
// caller falls back to ffprobe.

// ErrProbeIncomplete means native probing cannot vouch for the stream
// parameters; ffprobe must run.
var ErrProbeIncomplete = errors.New("transcode: native probe incomplete")

const (
	nativeHeadBytes    = 256 << 10
	nativeElementLimit = 4 << 20
	nativeReadBudget   = 16 << 20
)

// Matroska element IDs (https://www.matroska.org/technical/elements.html).
const (
	mkvEBML         = 0x1a45dfa3
	mkvDocType      = 0x4282
	mkvSegment      = 0x18538067
	mkvSeekHead     = 0x114d9b74
	mkvSeek         = 0x4dbb
	mkvSeekID       = 0x53ab
	mkvSeekPosition = 0x53ac
	mkvInfo         = 0x1549a966
	mkvTimecodeScl  = 0x2ad7b1
	mkvDuration     = 0x4489
	mkvTracks       = 0x1654ae6b
	mkvTrackEntry   = 0xae
	mkvTrackNumber  = 0xd7
	mkvTrackType    = 0x83
	mkvFlagDefault  = 0x88
	mkvFlagForced   = 0x55aa
	mkvDefaultDur   = 0x23e383
	mkvName         = 0x536e
	mkvLanguage     = 0x22b59c
	mkvLanguageBCP  = 0x22b59d
	mkvCodecID      = 0x86
	mkvCodecPrivate = 0x63a2
	mkvVideo        = 0xe0
	mkvPixelWidth   = 0xb0
	mkvPixelHeight  = 0xba
	mkvColour       = 0x55b0
	mkvBitsPerChan  = 0x55b2
	mkvTransfer     = 0x55ba
	mkvPrimaries    = 0x55bb
	mkvMatrix       = 0x55b1
	mkvRange        = 0x55b9
	mkvDisplayW     = 0x54b0
	mkvDisplayH     = 0x54ba
	mkvDisplayU     = 0x54b2
	mkvAudio        = 0xe1
	mkvSampling     = 0xb5
	mkvOutSampling  = 0x78b5
	mkvChannels     = 0x9f
	mkvAddMapping   = 0x41e4
	mkvAddIDType    = 0x41e7
	mkvAddIDExtra   = 0x41ed
	mkvEncodings    = 0x6d80
	mkvEncoding     = 0x6240
	mkvEncScope     = 0x5032
	mkvEncType      = 0x5033
	mkvChapters     = 0x1043a770
	mkvEdition      = 0x45b9
	mkvChapterAtom  = 0xb6
	mkvChapterUID   = 0x73c4
	mkvChapterStart = 0x91
	mkvChapterEnd   = 0x92
	mkvChapDisplay  = 0x80
	mkvChapString   = 0x85
	mkvTags         = 0x1254c367
	mkvTag          = 0x7373
	mkvTargets      = 0x63c0
	mkvTagTrackUID  = 0x63c5
	mkvSimpleTag    = 0x67c8
	mkvTagName      = 0x45a3
	mkvTagString    = 0x4487
	mkvTrackUID     = 0x73c5
	mkvCluster      = 0x1f43b675
)

type metadataReader interface {
	ReadMetadataAtContext(context.Context, []byte, int64) (int, error)
}

// nativeSource reads bounded header ranges, caching the file head.
type nativeSource struct {
	ctx  context.Context
	src  io.ReaderAt
	size int64
	head []byte
	left int
}

func (s *nativeSource) read(off int64, n int) ([]byte, error) {
	if off < 0 || n < 0 || off > s.size || int64(n) > s.size-off {
		return nil, incomplete("read outside file")
	}
	if off+int64(n) <= int64(len(s.head)) {
		return s.head[off : off+int64(n)], nil
	}
	if n > s.left {
		return nil, incomplete("read budget exhausted")
	}
	s.left -= n
	b := make([]byte, n)
	var err error
	if mr, ok := s.src.(metadataReader); ok {
		_, err = mr.ReadMetadataAtContext(s.ctx, b, off)
	} else {
		_, err = s.src.ReadAt(b, off)
	}
	if err != nil && !(errors.Is(err, io.EOF) && n > 0) {
		return nil, err
	}
	return b, nil
}

type mkvNode struct {
	id   uint64
	body []byte
}

func mkvVint(b []byte, id bool) (uint64, int, bool) {
	if len(b) == 0 || b[0] == 0 {
		return 0, 0, false
	}
	n := 1
	mask := byte(0x80)
	for b[0]&mask == 0 {
		n++
		mask >>= 1
	}
	if n > 8 || n > len(b) || (id && n > 4) {
		return 0, 0, false
	}
	v := uint64(b[0])
	if !id {
		v &= uint64(mask - 1)
	}
	for i := 1; i < n; i++ {
		v = v<<8 | uint64(b[i])
	}
	return v, n, true
}

// mkvNodes splits a master element body into children.
func mkvNodes(b []byte) ([]mkvNode, bool) {
	var out []mkvNode
	for len(b) > 0 {
		id, ni, ok := mkvVint(b, true)
		if !ok {
			return nil, false
		}
		size, ns, ok := mkvVint(b[ni:], false)
		if !ok || size > uint64(len(b)-ni-ns) {
			return nil, false
		}
		out = append(out, mkvNode{id, b[ni+ns : ni+ns+int(size)]})
		b = b[ni+ns+int(size):]
		if len(out) > 100000 {
			return nil, false
		}
	}
	return out, true
}

func mkvUint(b []byte) uint64 {
	if len(b) > 8 {
		return 0
	}
	var v uint64
	for _, x := range b {
		v = v<<8 | uint64(x)
	}
	return v
}

func mkvFloat(b []byte) float64 {
	switch len(b) {
	case 4:
		return float64(math.Float32frombits(binary.BigEndian.Uint32(b)))
	case 8:
		return math.Float64frombits(binary.BigEndian.Uint64(b))
	}
	return 0
}

func mkvString(b []byte) string {
	if i := strings.IndexByte(string(b), 0); i >= 0 {
		b = b[:i]
	}
	return strings.TrimSpace(string(b))
}

// header reads one element header at off, bounded by end. size < 0 means
// unknown size (runs to end).
func (s *nativeSource) header(off, end int64) (id uint64, start, stop int64, err error) {
	n := int(min(12, end-off))
	if n <= 1 {
		return 0, 0, 0, incomplete("element header past end")
	}
	b, err := s.read(off, n)
	if err != nil {
		return 0, 0, 0, err
	}
	id, ni, ok := mkvVint(b, true)
	if !ok {
		return 0, 0, 0, incomplete("bad element id")
	}
	size, ns, ok := mkvVint(b[ni:], false)
	if !ok {
		return 0, 0, 0, incomplete("bad element size")
	}
	start = off + int64(ni+ns)
	if size == uint64(1)<<uint(7*ns)-1 {
		return id, start, end, nil
	}
	if size > uint64(end-start) {
		return 0, 0, 0, incomplete("element overruns parent")
	}
	return id, start, start + int64(size), nil
}

// ProbeSource probes a media source natively: Matroska/WebM and
// MP4/MOV/M4V. Other containers return ErrProbeIncomplete.
func ProbeSource(ctx context.Context, src io.ReaderAt, size int64) (*core.MediaInfo, error) {
	if size <= 0 {
		return nil, incomplete("empty source")
	}
	s := &nativeSource{ctx: ctx, src: src, size: size, left: nativeReadBudget}
	head, err := s.read(0, int(min(size, nativeHeadBytes)))
	if err != nil {
		return nil, err
	}
	s.head = head
	switch {
	case len(head) >= 4 && binary.BigEndian.Uint32(head) == mkvEBML:
		return s.matroska()
	case isISOBMFF(head):
		return s.mp4()
	}
	return nil, incomplete("container not supported natively")
}

type mkvTrack struct {
	number      uint64
	uid         uint64
	kind        uint64
	codecID     string
	private     []byte
	name        string
	language    string
	bcp47       string
	hasLanguage bool
	flagDefault bool
	forced      bool
	defaultDur  uint64
	width       uint64
	height      uint64
	bitsPerChan uint64
	transfer    int
	primaries   int
	matrix      int
	colourRange int // Matroska Range: 1 broadcast, 2 full
	displayW    uint64
	displayH    uint64
	displayUnit uint64
	sampling    float64
	outSampling float64
	channels    uint64
	dovi        []byte
	privScoped  bool // ContentEncoding compresses or encrypts CodecPrivate
	encrypted   bool
}

func (s *nativeSource) matroska() (*core.MediaInfo, error) {
	id, hs, end, err := s.header(0, s.size)
	if err != nil || id != mkvEBML || end > int64(len(s.head)) {
		return nil, incomplete("bad EBML header")
	}
	children, ok := mkvNodes(s.head[hs:end])
	if !ok {
		return nil, incomplete("bad EBML header")
	}
	for _, c := range children {
		if c.id == mkvDocType {
			if t := mkvString(c.body); t != "matroska" && t != "webm" {
				return nil, incomplete("unsupported doctype")
			}
		}
	}
	var segStart, segEnd int64
	for off, n := end, 0; off < s.size && n < 16; n++ {
		id, start, stop, err := s.header(off, s.size)
		if err != nil {
			return nil, err
		}
		if id == mkvSegment {
			segStart, segEnd = start, stop
			break
		}
		off = stop
	}
	if segStart == 0 {
		return nil, incomplete("no segment")
	}

	var info, tracks, chapters []byte
	targets := map[uint64]int64{}
	load := func(start, stop int64) ([]byte, error) {
		if stop-start > nativeElementLimit {
			return nil, incomplete("element too large")
		}
		return s.read(start, int(stop-start))
	}
	// Top-level elements up to the first Cluster. Attachments and Void are
	// skipped by size; Info/Tracks/Chapters are read; SeekHead locates the
	// ones mkvmerge or other muxers placed elsewhere.
	for off, n := segStart, 0; off < segEnd && n < 64; n++ {
		id, start, stop, err := s.header(off, segEnd)
		if err != nil {
			return nil, err
		}
		if id == mkvCluster {
			break
		}
		switch id {
		case mkvSeekHead, mkvInfo, mkvTracks, mkvChapters:
			b, err := load(start, stop)
			if err != nil {
				return nil, err
			}
			switch id {
			case mkvSeekHead:
				seekTargets(b, segStart, segEnd, targets)
			case mkvInfo:
				info = b
			case mkvTracks:
				tracks = b
			case mkvChapters:
				chapters = b
			}
		}
		if stop <= off {
			return nil, incomplete("element loop")
		}
		off = stop
		if stop > int64(len(s.head)) {
			_, infoAt := targets[mkvInfo]
			_, tracksAt := targets[mkvTracks]
			if (info != nil || infoAt) && (tracks != nil || tracksAt) {
				break // the SeekHead says where the rest is; don't walk
			}
		}
	}
	for _, want := range []struct {
		id  uint64
		dst *[]byte
	}{{mkvInfo, &info}, {mkvTracks, &tracks}} {
		if *want.dst != nil {
			continue
		}
		off, ok := targets[want.id]
		if !ok {
			return nil, incomplete("%s not located", "Info/Tracks")
		}
		id, start, stop, err := s.header(off, segEnd)
		if err != nil || id != want.id {
			return nil, incomplete("%s not located", "Info/Tracks")
		}
		if *want.dst, err = load(start, stop); err != nil {
			return nil, err
		}
	}
	m := &core.MediaInfo{Container: "mkv", Size: s.size}
	scale := uint64(1000000)
	var duration float64
	infoNodes, ok := mkvNodes(info)
	if !ok {
		return nil, incomplete("bad Info")
	}
	for _, c := range infoNodes {
		switch c.id {
		case mkvTimecodeScl:
			scale = mkvUint(c.body)
		case mkvDuration:
			duration = mkvFloat(c.body)
		}
	}
	if scale == 0 || duration <= 0 || math.IsNaN(duration) || math.IsInf(duration, 0) {
		return nil, incomplete("missing duration")
	}
	ns := duration * float64(scale)
	if ns <= 0 || ns >= float64(math.MaxInt64) {
		return nil, incomplete("invalid duration")
	}
	m.Duration = time.Duration(ns)
	m.BitrateKbps = int(float64(s.size*8) / m.Duration.Seconds() / 1000)

	parsed, err := mkvTracksOf(tracks)
	if err != nil {
		return nil, err
	}
	index := 0
	hasVideo := false
	for _, t := range parsed {
		si, accepted, err := t.stream(index)
		if err != nil {
			return nil, err
		}
		if !accepted {
			continue
		}
		index++
		if si.Type == "" {
			continue // a demuxed data stream ffprobe would not map
		}
		if si.Type == "video" {
			hasVideo = true
		}
		m.Streams = append(m.Streams, si)
	}
	if !hasVideo {
		return nil, incomplete("no video stream")
	}
	// Chapters outside the cached head are left to ProbeTrailer, off the
	// Play path.
	if chapters == nil {
		if off, ok := targets[mkvChapters]; ok && off < int64(len(s.head)) {
			if id, start, stop, err := s.header(off, segEnd); err == nil && id == mkvChapters && stop <= int64(len(s.head)) {
				chapters, _ = load(start, stop)
			}
		}
	}
	m.Chapters = mkvChaptersOf(chapters, m.Duration)
	return m, nil
}

// ProbeTrailer reads metadata Matroska keeps after the media (tail
// Chapters, mkvmerge statistics tags). It fills only facts that are not
// already known and never changes stream identity or codec parameters, so
// it can run after playback has started.
func ProbeTrailer(ctx context.Context, src io.ReaderAt, size int64, m *core.MediaInfo) bool {
	if m == nil || m.Container != "mkv" || size <= 0 {
		return false
	}
	s := &nativeSource{ctx: ctx, src: src, size: size, left: nativeReadBudget}
	head, err := s.read(0, int(min(size, 64<<10)))
	if err != nil || len(head) < 4 || binary.BigEndian.Uint32(head) != mkvEBML {
		return false
	}
	s.head = head
	_, _, end, err := s.header(0, size)
	if err != nil {
		return false
	}
	var segStart, segEnd int64
	for off, n := end, 0; off < size && n < 16; n++ {
		id, start, stop, err := s.header(off, size)
		if err != nil {
			return false
		}
		if id == mkvSegment {
			segStart, segEnd = start, stop
			break
		}
		off = stop
	}
	if segStart == 0 {
		return false
	}
	targets := map[uint64]int64{}
	var tracks []byte
	for off, n := segStart, 0; off < segEnd && n < 64; n++ {
		id, start, stop, err := s.header(off, segEnd)
		if err != nil || id == mkvCluster || stop <= off {
			break
		}
		if id == mkvSeekHead || id == mkvTracks {
			if stop-start > nativeElementLimit {
				break
			}
			b, err := s.read(start, int(stop-start))
			if err != nil {
				return false
			}
			if id == mkvSeekHead {
				seekTargets(b, segStart, segEnd, targets)
			} else {
				tracks = b
			}
		}
		off = stop
	}
	changed := false
	element := func(id uint64) []byte {
		off, ok := targets[id]
		if !ok {
			return nil
		}
		got, start, stop, err := s.header(off, segEnd)
		if err != nil || got != id || stop-start > nativeElementLimit {
			return nil
		}
		b, err := s.read(start, int(stop-start))
		if err != nil {
			return nil
		}
		return b
	}
	if len(m.Chapters) == 0 {
		if cs := mkvChaptersOf(element(mkvChapters), m.Duration); len(cs) > 0 {
			m.Chapters = cs
			changed = true
		}
	}
	if tracks == nil {
		return changed
	}
	parsed, err := mkvTracksOf(tracks)
	if err != nil {
		return changed
	}
	bps := mkvTrackBitrates(element(mkvTags))
	if len(bps) == 0 {
		return changed
	}
	index := 0
	for _, t := range parsed {
		si, accepted, err := t.stream(index)
		if err != nil || !accepted {
			continue
		}
		index++
		kbps := bps[t.uid]
		if kbps <= 0 {
			continue
		}
		for i := range m.Streams {
			if m.Streams[i].Index == si.Index && m.Streams[i].BitrateKbps == 0 {
				m.Streams[i].BitrateKbps = kbps
				changed = true
			}
		}
	}
	return changed
}

func seekTargets(b []byte, segStart, segEnd int64, out map[uint64]int64) {
	nodes, ok := mkvNodes(b)
	if !ok {
		return
	}
	for _, c := range nodes {
		if c.id != mkvSeek {
			continue
		}
		fields, ok := mkvNodes(c.body)
		if !ok {
			continue
		}
		var id, pos uint64
		for _, f := range fields {
			switch f.id {
			case mkvSeekID:
				id = mkvUint(f.body)
			case mkvSeekPosition:
				pos = mkvUint(f.body)
			}
		}
		if id != 0 && pos < uint64(segEnd-segStart) {
			if _, dup := out[id]; !dup {
				out[id] = segStart + int64(pos)
			}
		}
	}
}

func mkvTracksOf(b []byte) ([]mkvTrack, error) {
	nodes, ok := mkvNodes(b)
	if !ok {
		return nil, incomplete("bad Tracks")
	}
	var out []mkvTrack
	for _, n := range nodes {
		if n.id != mkvTrackEntry {
			continue
		}
		fields, ok := mkvNodes(n.body)
		if !ok {
			return nil, incomplete("bad TrackEntry")
		}
		t := mkvTrack{flagDefault: true, language: "eng", sampling: 8000, channels: 1}
		for _, f := range fields {
			switch f.id {
			case mkvTrackNumber:
				t.number = mkvUint(f.body)
			case mkvTrackUID:
				t.uid = mkvUint(f.body)
			case mkvTrackType:
				t.kind = mkvUint(f.body)
			case mkvFlagDefault:
				t.flagDefault = mkvUint(f.body) != 0
			case mkvFlagForced:
				t.forced = mkvUint(f.body) != 0
			case mkvDefaultDur:
				t.defaultDur = mkvUint(f.body)
			case mkvName:
				t.name = mkvString(f.body)
			case mkvLanguage:
				t.language, t.hasLanguage = mkvString(f.body), true
			case mkvLanguageBCP:
				t.bcp47 = mkvString(f.body)
			case mkvCodecID:
				t.codecID = mkvString(f.body)
			case mkvCodecPrivate:
				t.private = f.body
			case mkvVideo:
				vs, ok := mkvNodes(f.body)
				if !ok {
					return nil, incomplete("bad Video")
				}
				for _, v := range vs {
					switch v.id {
					case mkvPixelWidth:
						t.width = mkvUint(v.body)
					case mkvPixelHeight:
						t.height = mkvUint(v.body)
					case mkvDisplayW:
						t.displayW = mkvUint(v.body)
					case mkvDisplayH:
						t.displayH = mkvUint(v.body)
					case mkvDisplayU:
						t.displayUnit = mkvUint(v.body)
					case mkvColour:
						cs, _ := mkvNodes(v.body)
						for _, c := range cs {
							switch c.id {
							case mkvTransfer:
								t.transfer = int(mkvUint(c.body))
							case mkvPrimaries:
								t.primaries = int(mkvUint(c.body))
							case mkvMatrix:
								t.matrix = int(mkvUint(c.body))
							case mkvRange:
								t.colourRange = int(mkvUint(c.body))
							case mkvBitsPerChan:
								t.bitsPerChan = mkvUint(c.body)
							}
						}
					}
				}
			case mkvAudio:
				as, ok := mkvNodes(f.body)
				if !ok {
					return nil, incomplete("bad Audio")
				}
				for _, a := range as {
					switch a.id {
					case mkvSampling:
						t.sampling = mkvFloat(a.body)
					case mkvOutSampling:
						t.outSampling = mkvFloat(a.body)
					case mkvChannels:
						t.channels = mkvUint(a.body)
					}
				}
			case mkvAddMapping:
				ms, _ := mkvNodes(f.body)
				var kind uint64
				var extra []byte
				for _, x := range ms {
					switch x.id {
					case mkvAddIDType:
						kind = mkvUint(x.body)
					case mkvAddIDExtra:
						extra = x.body
					}
				}
				switch kind {
				case 0x64766343, 0x64767643, 0x64767743: // dvcC, dvvC, dvwC
					t.dovi = extra
				}
			case mkvEncodings:
				es, _ := mkvNodes(f.body)
				for _, e := range es {
					if e.id != mkvEncoding {
						continue
					}
					scope, kind := uint64(1), uint64(0)
					fs, _ := mkvNodes(e.body)
					for _, x := range fs {
						switch x.id {
						case mkvEncScope:
							scope = mkvUint(x.body)
						case mkvEncType:
							kind = mkvUint(x.body)
						}
					}
					if scope&2 != 0 {
						t.privScoped = true
					}
					if kind == 1 {
						t.encrypted = true
					}
				}
			}
		}
		out = append(out, t)
	}
	return out, nil
}

// stream maps a track to the StreamInfo ffprobe would report. accepted
// mirrors ffmpeg's matroska demuxer: tracks it skips take no stream index.
// A zero Type with accepted true is a data stream that keeps its index.
func (t mkvTrack) stream(index int) (core.StreamInfo, bool, error) {
	const (
		video, audio, subtitle, metadata = 1, 2, 0x11, 0x21
	)
	if t.kind != video && t.kind != audio && t.kind != subtitle && t.kind != metadata {
		return core.StreamInfo{}, false, nil
	}
	if t.codecID == "" {
		return core.StreamInfo{}, false, nil
	}
	lead := t.codecID[0]
	if (t.kind == video && lead != 'V') || (t.kind == audio && lead != 'A') || ((t.kind == subtitle || t.kind == metadata) && lead != 'D' && lead != 'S') {
		return core.StreamInfo{}, false, nil
	}
	if t.encrypted || t.privScoped {
		return core.StreamInfo{}, false, incomplete("encrypted or compressed codec private")
	}
	if t.kind == metadata {
		return core.StreamInfo{}, true, nil
	}
	si := core.StreamInfo{Index: index, Title: t.name, Default: t.flagDefault, Forced: t.forced}
	// libavformat reports the legacy ISO 639-2 Language element, which
	// mkvmerge writes next to LanguageBCP47. A BCP 47 tag alone is mapped
	// to the same three-letter form.
	si.Language = normalizeLanguage(t.language)
	if !t.hasLanguage && t.bcp47 != "" {
		if norm := normalizeBCP47Language(t.bcp47); norm != "" {
			si.Language = norm
		}
	}
	var err error
	switch t.kind {
	case video:
		si.Type = "video"
		err = t.video(&si)
	case audio:
		si.Type = "audio"
		err = t.audio(&si)
	default:
		si.Type = "subtitle"
		si.Codec = mkvSubtitleCodec(t.codecID)
		if si.Codec == "" {
			err = incomplete("subtitle codec %s", t.codecID)
		}
	}
	if err != nil {
		return core.StreamInfo{}, false, err
	}
	return si, true, nil
}

func (t mkvTrack) video(si *core.StreamInfo) error {
	if t.width == 0 || t.height == 0 || t.defaultDur == 0 {
		return incomplete("video geometry or frame rate missing")
	}
	si.Width, si.Height = int(t.width), int(t.height)
	si.FrameRate = math.Round(1e9/float64(t.defaultDur)*1000) / 1000
	codec := ""
	switch {
	case strings.HasPrefix(t.codecID, "V_MPEG4/ISO/AVC"):
		codec = "h264"
	case strings.HasPrefix(t.codecID, "V_MPEGH/ISO/HEVC"):
		codec = "hevc"
	case t.codecID == "V_AV1":
		codec = "av1"
	default:
		return incomplete("video codec %s", t.codecID)
	}
	if err := videoConfig(si, codec, t.private, t.transfer, t.dovi); err != nil {
		return err
	}
	fullRange := -1
	switch t.colourRange {
	case 1:
		fullRange = 0
	case 2:
		fullRange = 1
	}
	var dw, dh int
	if t.displayUnit <= 3 { // pixels, cm, inches or aspect ratio: all proportional
		dw, dh = int(t.displayW), int(t.displayH)
	}
	containerPicture(si, t.primaries, t.matrix, fullRange, dw, dh)
	return nil
}

// spsPicture records the sample aspect ratio and colour the SPS VUI
// signals. A container's own values (mkvColour, colr, pasp) replace them.
func spsPicture(si *core.StreamInfo, f spsFacts) {
	if f.sarNum > 0 && f.sarDen > 0 {
		n, d := reduce(f.sarNum, f.sarDen)
		si.SampleAspectRatio = fmt.Sprintf("%d:%d", n, d)
	}
	switch f.fullRange {
	case 0:
		si.ColorRange = "tv"
	case 1:
		si.ColorRange = "pc"
	}
	si.ColorPrimaries = primariesName(f.primaries)
	si.ColorSpace = matrixName(f.matrix)
}

// containerPicture applies a container's explicit picture facts: H.273
// primaries and matrix codes (0 or 2 unspecified), full range (-1 unknown)
// and display size in pixels (0 unknown).
func containerPicture(si *core.StreamInfo, primaries, matrix, fullRange, displayW, displayH int) {
	if n := primariesName(primaries); n != "" {
		si.ColorPrimaries = n
	}
	if n := matrixName(matrix); n != "" {
		si.ColorSpace = n
	}
	switch fullRange {
	case 0:
		si.ColorRange = "tv"
	case 1:
		si.ColorRange = "pc"
	}
	if displayW > 0 && displayH > 0 && si.Width > 0 && si.Height > 0 {
		n, d := reduce(displayW*si.Height, displayH*si.Width)
		si.SampleAspectRatio = fmt.Sprintf("%d:%d", n, d)
	}
}

// videoConfig fills codec facts from a decoder configuration record (avcC,
// hvcC or av1C), the container's transfer characteristics (0 or 2 when
// unknown) and an optional Dolby Vision configuration record.
func videoConfig(si *core.StreamInfo, codec string, p []byte, transfer int, dovi []byte) error {
	si.Codec = codec
	switch codec {
	case "h264":
		if len(p) < 7 || p[0] != 1 {
			return incomplete("bad avcC")
		}
		name, ok := avcProfileName(p[1], p[2])
		if !ok {
			return incomplete("h264 profile %d", p[1])
		}
		si.Profile, si.Level = name, int(p[3])
		si.CodecString = fmt.Sprintf("avc1.%02x%02x%02x", p[1], p[2], p[3])
		sps := avcConfigSPS(p)
		if sps == nil {
			return incomplete("no h264 SPS")
		}
		f, err := avcSPS(sps)
		if err != nil {
			return incomplete("h264 SPS: %v", err)
		}
		si.BitDepth = f.bitDepth
		spsPicture(si, f)
		if transfer == 0 || transfer == 2 {
			transfer = f.transfer
		}
	case "hevc":
		if len(p) < 23 || p[0] != 1 {
			return incomplete("bad hvcC")
		}
		name, ok := hevcProfileName(p)
		if !ok {
			return incomplete("hevc profile %d", p[1]&0x1f)
		}
		si.Profile, si.Level = name, int(p[12])
		si.CodecString = "hvc1." + hevcCodecs(p)
		sps := hevcConfigSPS(p)
		if sps == nil {
			return incomplete("no hevc SPS")
		}
		f, err := hevcSPS(sps)
		if err != nil {
			return incomplete("hevc SPS: %v", err)
		}
		si.BitDepth = f.bitDepth
		spsPicture(si, f)
		if transfer == 0 || transfer == 2 {
			transfer = f.transfer
		}
	case "av1":
		if len(p) < 4 || p[0] != 0x81 || transfer == 0 || transfer == 2 {
			return incomplete("av1 colour unknown") // colour lives in the sequence header OBU
		}
		profile := p[1] >> 5
		si.Profile = []string{"Main", "High", "Professional", "", "", "", "", ""}[profile]
		if si.Profile == "" {
			return incomplete("av1 profile")
		}
		si.Level = int(p[1] & 0x1f)
		si.BitDepth = 8
		if p[2]&0x40 != 0 {
			si.BitDepth = 10
			if p[2]&0x20 != 0 {
				si.BitDepth = 12
			}
		}
	default:
		return incomplete("video codec %s", codec)
	}
	si.ColorTransfer = transferName(transfer)
	if len(dovi) >= 4 {
		d := dovi
		si.DolbyVision = true
		si.DolbyVisionVersion = fmt.Sprintf("%d.%d", d[0], d[1])
		bits := binary.BigEndian.Uint16(d[2:4])
		si.DolbyVisionProfile = int(bits >> 9)
		si.DolbyVisionLevel = int(bits>>3) & 0x3f
		rpu, el, bl := bits&4 != 0, bits&2 != 0, bits&1 != 0
		si.DolbyVisionRPUPresent, si.DolbyVisionELPresent, si.DolbyVisionBLPresent = &rpu, &el, &bl
		if len(d) >= 5 {
			si.DolbyVisionCompatibilityID = int(d[4] >> 4)
		}
	}
	return nil
}

// avcProfileName mirrors libavcodec's H.264 profile naming, including the
// constrained/intra variants it derives from constraint_set flags.
func avcProfileName(profile, constraints byte) (string, bool) {
	set1, set3 := constraints&0x40 != 0, constraints&0x10 != 0
	switch profile {
	case 66:
		if set1 {
			return "Constrained Baseline", true
		}
		return "Baseline", true
	case 77:
		return "Main", true
	case 88:
		return "Extended", true
	case 100:
		return "High", true
	case 110:
		if set3 {
			return "High 10 Intra", true
		}
		return "High 10", true
	case 122:
		if set3 {
			return "High 4:2:2 Intra", true
		}
		return "High 4:2:2", true
	case 244:
		if set3 {
			return "High 4:4:4 Intra", true
		}
		return "High 4:4:4 Predictive", true
	case 44:
		return "CAVLC 4:4:4", true
	case 118:
		return "Multiview High", true
	case 128:
		return "Stereo High", true
	}
	return "", false
}

func hevcProfileName(c []byte) (string, bool) {
	profile := int(c[1] & 0x1f)
	if profile == 0 {
		compat := binary.BigEndian.Uint32(c[2:6])
		for j := 1; j < 32; j++ {
			if compat&(1<<uint(31-j)) != 0 {
				profile = j
				break
			}
		}
	}
	switch profile {
	case 1:
		return "Main", true
	case 2:
		return "Main 10", true
	case 3:
		return "Main Still Picture", true
	case 4:
		return "Rext", true
	case 9:
		return "SCC", true
	}
	return "", false
}

// avcConfigSPS returns the first SPS NAL of an AVCDecoderConfigurationRecord.
func avcConfigSPS(c []byte) []byte {
	if len(c) < 8 || c[5]&0x1f == 0 {
		return nil
	}
	n := int(binary.BigEndian.Uint16(c[6:8]))
	if n == 0 || 8+n > len(c) {
		return nil
	}
	return c[8 : 8+n]
}

// hevcConfigSPS returns the first SPS NAL of an HEVCDecoderConfigurationRecord.
func hevcConfigSPS(c []byte) []byte {
	arrays := int(c[22])
	b := c[23:]
	for i := 0; i < arrays; i++ {
		if len(b) < 3 {
			return nil
		}
		kind := b[0] & 0x3f
		count := int(binary.BigEndian.Uint16(b[1:3]))
		b = b[3:]
		for j := 0; j < count; j++ {
			if len(b) < 2 {
				return nil
			}
			n := int(binary.BigEndian.Uint16(b))
			if 2+n > len(b) {
				return nil
			}
			if kind == 33 && n > 0 {
				return b[2 : 2+n]
			}
			b = b[2+n:]
		}
	}
	return nil
}

func (t mkvTrack) audio(si *core.StreamInfo) error {
	id := t.codecID
	rate := t.sampling
	if t.outSampling > 0 {
		rate = t.outSampling
	}
	switch {
	case strings.HasPrefix(id, "A_AAC"):
		si.Codec = "aac"
		if len(t.private) >= 2 {
			profile, sbrRate, channels, ok := aacConfig(t.private)
			if !ok {
				return incomplete("bad AudioSpecificConfig")
			}
			si.Profile = profile
			if sbrRate > 0 {
				rate = float64(sbrRate)
			}
			t.channels = uint64(channels) // the decoder's output, as ffprobe reports
		} else if strings.Contains(id, "/SBR") {
			si.Profile = "HE-AAC"
		} else if strings.HasSuffix(id, "/LC") {
			si.Profile = "LC"
		} else {
			return incomplete("audio codec %s", id)
		}
	case strings.HasPrefix(id, "A_EAC3"):
		si.Codec = "eac3"
	case strings.HasPrefix(id, "A_AC3"):
		si.Codec = "ac3"
	case strings.HasPrefix(id, "A_DTS"):
		si.Codec = "dts"
	case strings.HasPrefix(id, "A_TRUEHD"), strings.HasPrefix(id, "A_MLP"):
		si.Codec = "truehd"
	case strings.HasPrefix(id, "A_FLAC"):
		si.Codec = "flac"
	case strings.HasPrefix(id, "A_OPUS"):
		si.Codec = "opus"
		rate = 48000
	case strings.HasPrefix(id, "A_VORBIS"):
		si.Codec = "vorbis"
	case strings.HasPrefix(id, "A_MPEG/L3"):
		si.Codec = "mp3"
	case strings.HasPrefix(id, "A_MPEG/L2"):
		si.Codec = "mp2"
	case strings.HasPrefix(id, "A_PCM/"):
		si.Codec = "pcm"
	default:
		return incomplete("audio channels or rate missing")
	}
	if t.channels == 0 || rate <= 0 || rate > 1e6 {
		return incomplete("audio channels or rate missing")
	}
	si.Channels = int(t.channels)
	si.SampleRate = int(rate)
	si.ChannelLayout = defaultChannelLayout(si.Codec, si.Channels)
	return nil
}

// aacConfig reads an AudioSpecificConfig: libavcodec's profile name, the
// SBR output rate when explicitly signalled, and the channel configuration.
func aacConfig(b []byte) (profile string, sbrRate, channels int, ok bool) {
	r := &bitReader{b: b}
	rates := []int{96000, 88200, 64000, 48000, 44100, 32000, 24000, 22050, 16000, 12000, 11025, 8000, 7350}
	objectType := func() int {
		t := int(r.u(5))
		if t == 31 {
			t = 32 + int(r.u(6))
		}
		return t
	}
	rate := func() int {
		i := int(r.u(4))
		if i == 15 {
			return int(r.u(24))
		}
		if i < len(rates) {
			return rates[i]
		}
		return 0
	}
	aot := objectType()
	rate()
	channels = int(r.u(4))
	sbr, ps := false, false
	if aot == 5 || aot == 29 {
		sbr, ps = true, aot == 29
		sbrRate = rate()
		aot = objectType()
	} else if aot >= 1 && aot <= 4 && channels > 0 {
		// GASpecificConfig, then backward-compatible SBR/PS signalling.
		r.skip(1) // frameLengthFlag
		if r.flag() {
			r.skip(14) // coreCoderDelay
		}
		r.skip(1) // extensionFlag
		if r.err == nil && len(r.b)*8-r.pos >= 16 && r.u(11) == 0x2b7 {
			if objectType() == 5 && r.flag() {
				sbr = true
				sbrRate = rate()
				if r.err == nil && len(r.b)*8-r.pos >= 12 && r.u(11) == 0x548 {
					ps = r.flag()
				}
			}
		}
		r.err = nil // extension parsing is optional
	}
	if r.err != nil || channels == 0 {
		return "", 0, 0, false
	}
	// libavcodec decodes mono SBR as stereo in case parametric stereo is
	// present.
	if sbr && channels == 1 {
		channels = 2
	}
	switch {
	case ps:
		return "HE-AACv2", sbrRate, channels, true
	case sbr:
		return "HE-AAC", sbrRate, channels, true
	}
	names := map[int]string{1: "Main", 2: "LC", 3: "SSR", 4: "LTP", 23: "LD", 39: "ELD", 42: "xHE-AAC"}
	if name, found := names[aot]; found {
		return name, 0, channels, true
	}
	return "", 0, 0, false
}

// defaultChannelLayout is the layout name ffmpeg's decoders report for a
// channel count. Only display depends on it; unknown counts stay empty.
func defaultChannelLayout(codec string, n int) string {
	switch n {
	case 1:
		return "mono"
	case 2:
		return "stereo"
	}
	switch codec {
	case "ac3", "eac3", "dts", "truehd":
		switch n {
		case 6:
			return "5.1(side)"
		case 8:
			return "7.1"
		}
	case "aac", "flac", "opus", "vorbis", "pcm":
		switch n {
		case 6:
			return "5.1"
		case 8:
			return "7.1"
		}
	}
	return ""
}

func mkvSubtitleCodec(id string) string {
	switch {
	case strings.HasPrefix(id, "S_TEXT/UTF8"), strings.HasPrefix(id, "S_TEXT/ASCII"):
		return "subrip"
	case strings.HasPrefix(id, "S_TEXT/ASS"), strings.HasPrefix(id, "S_TEXT/SSA"), strings.HasPrefix(id, "S_ASS"), strings.HasPrefix(id, "S_SSA"):
		return "ass"
	case strings.HasPrefix(id, "S_TEXT/WEBVTT"), strings.HasPrefix(id, "D_WEBVTT"):
		return "webvtt"
	case strings.HasPrefix(id, "S_VOBSUB"):
		return "vobsub"
	case strings.HasPrefix(id, "S_DVBSUB"):
		return "dvbsub"
	case strings.HasPrefix(id, "S_HDMV/PGS"):
		return "pgs"
	case strings.HasPrefix(id, "S_HDMV/TEXTST"):
		return "hdmv_text_subtitle"
	}
	return ""
}

// mkvChaptersOf mirrors libavformat: top-level atoms of every edition with
// a UID and a strictly increasing start; missing ends close at the next
// chapter or the duration.
func mkvChaptersOf(b []byte, duration time.Duration) []core.Chapter {
	if b == nil {
		return nil
	}
	editions, ok := mkvNodes(b)
	if !ok {
		return nil
	}
	type atom struct {
		uid        uint64
		start, end int64
		title      string
	}
	var atoms []atom
	for _, e := range editions {
		if e.id != mkvEdition {
			continue
		}
		cs, ok := mkvNodes(e.body)
		if !ok {
			continue
		}
		for _, c := range cs {
			if c.id != mkvChapterAtom {
				continue
			}
			fs, ok := mkvNodes(c.body)
			if !ok {
				continue
			}
			a := atom{start: -1, end: -1}
			for _, f := range fs {
				switch f.id {
				case mkvChapterUID:
					a.uid = mkvUint(f.body)
				case mkvChapterStart:
					a.start = int64(mkvUint(f.body))
				case mkvChapterEnd:
					a.end = int64(mkvUint(f.body))
				case mkvChapDisplay:
					ds, _ := mkvNodes(f.body)
					for _, d := range ds {
						if d.id == mkvChapString && a.title == "" {
							a.title = mkvString(d.body)
						}
					}
				}
			}
			atoms = append(atoms, a)
		}
	}
	var kept []atom
	var maxStart int64
	for _, a := range atoms {
		if a.start < 0 || a.uid == 0 || (maxStart != 0 && a.start <= maxStart) {
			continue
		}
		kept = append(kept, a)
		maxStart = a.start
	}
	var out []core.Chapter
	for i, a := range kept {
		end := a.end
		if end < 0 {
			end = int64(duration)
			if i+1 < len(kept) {
				end = kept[i+1].start
			}
		}
		if a.start < 0 || end <= a.start {
			continue
		}
		out = append(out, core.Chapter{ID: int64(a.uid), Title: a.title, Start: time.Duration(a.start), End: time.Duration(end)})
	}
	return out
}

// mkvTrackBitrates reads mkvmerge's per-track BPS statistics tags.
func mkvTrackBitrates(b []byte) map[uint64]int {
	if b == nil {
		return nil
	}
	tags, ok := mkvNodes(b)
	if !ok {
		return nil
	}
	out := map[uint64]int{}
	for _, t := range tags {
		if t.id != mkvTag {
			continue
		}
		fs, ok := mkvNodes(t.body)
		if !ok {
			continue
		}
		var uid uint64
		bps := 0
		for _, f := range fs {
			switch f.id {
			case mkvTargets:
				ts, _ := mkvNodes(f.body)
				for _, x := range ts {
					if x.id == mkvTagTrackUID {
						uid = mkvUint(x.body)
					}
				}
			case mkvSimpleTag:
				ss, _ := mkvNodes(f.body)
				var name, value string
				for _, x := range ss {
					switch x.id {
					case mkvTagName:
						name = mkvString(x.body)
					case mkvTagString:
						value = mkvString(x.body)
					}
				}
				if strings.EqualFold(name, "BPS") || strings.EqualFold(name, "BPS-eng") {
					bps = int(atoi64(value) / 1000)
				}
			}
		}
		if uid != 0 && bps > 0 {
			out[uid] = bps
		}
	}
	return out
}

func incomplete(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrProbeIncomplete}, args...)...)
}

// iso6391to2 maps ISO 639-1 codes to the ISO 639-2/B codes ffmpeg reports.
var iso6391to2 = map[string]string{
	"ar": "ara", "bg": "bul", "ca": "cat", "cs": "cze", "da": "dan", "de": "ger", "el": "gre", "en": "eng",
	"es": "spa", "et": "est", "fa": "per", "fi": "fin", "fr": "fre", "he": "heb", "hi": "hin", "hr": "hrv",
	"hu": "hun", "id": "ind", "is": "ice", "it": "ita", "ja": "jpn", "ko": "kor", "lt": "lit", "lv": "lav",
	"ms": "may", "nb": "nob", "nl": "dut", "nn": "nno", "no": "nor", "pl": "pol", "pt": "por", "ro": "rum",
	"ru": "rus", "sk": "slo", "sl": "slv", "sr": "srp", "sv": "swe", "ta": "tam", "te": "tel", "th": "tha",
	"tr": "tur", "uk": "ukr", "vi": "vie", "zh": "chi",
}
