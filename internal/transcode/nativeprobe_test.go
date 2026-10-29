package transcode

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/braedonsaunders/ether/internal/core"
)

// The fixtures were muxed by ffmpeg and the .ffprobe.json files are the
// bundled ffprobe's output for them, so native probing is held to what the
// ffprobe path reported.
func TestProbeSourceMatchesRecordedFFprobe(t *testing.T) {
	var files []string
	for _, ext := range []string{"mkv", "mp4", "mov"} {
		more, _ := filepath.Glob("testdata/nativeprobe/*." + ext)
		files = append(files, more...)
	}
	if len(files) == 0 {
		t.Fatal("no fixtures")
	}
	for _, f := range files {
		t.Run(filepath.Base(f), func(t *testing.T) {
			raw, err := os.ReadFile(strings.TrimSuffix(f, filepath.Ext(f)) + ".ffprobe.json")
			if err != nil {
				t.Fatal(err)
			}
			want, err := ParseProbe(raw)
			if err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(f)
			if err != nil {
				t.Fatal(err)
			}
			want.Size = int64(len(data))
			got, err := ProbeSource(context.Background(), bytes.NewReader(data), int64(len(data)))
			if err != nil {
				t.Fatalf("ProbeSource: %v", err)
			}
			normalizeForNative(want, got)
			// libavformat derives MP4 durations and bitrates from edit
			// lists and the longest stream; tolerate rounding there.
			if d := want.Duration - got.Duration; d > -50*time.Millisecond && d < 50*time.Millisecond {
				want.Duration = got.Duration
				want.BitrateKbps = got.BitrateKbps
			}
			for i := range want.Streams {
				tolerance := 0.02
				if want.Streams[i].Type == "audio" {
					tolerance = 0.06 // encoder priming in the audio edit list
				}
				if i < len(got.Streams) && closeTo(want.Streams[i].BitrateKbps, got.Streams[i].BitrateKbps, tolerance) {
					want.Streams[i].BitrateKbps = got.Streams[i].BitrateKbps
				}
			}
			w, _ := json.MarshalIndent(want, "", " ")
			g, _ := json.MarshalIndent(got, "", " ")
			if string(w) != string(g) {
				t.Errorf("native probe differs from ffprobe\nffprobe: %s\nnative:  %s", w, g)
			}
		})
	}
}

// normalizeForNative removes the facts native probing deliberately leaves
// to later enrichment: frame-header audio bitrates and the DTS/TrueHD/E-AC-3
// profiles that need audio packets, plus ffprobe's "unknown" transfer.
func normalizeForNative(want, got *core.MediaInfo) {
	for i := range want.Streams {
		w := &want.Streams[i]
		if w.ColorTransfer == "unknown" {
			w.ColorTransfer = ""
		}
		if i >= len(got.Streams) || w.Type != "audio" {
			continue
		}
		if got.Streams[i].BitrateKbps == 0 {
			w.BitrateKbps = 0
		}
		switch w.Codec {
		case "dts", "truehd", "eac3":
			if got.Streams[i].Profile == "" {
				w.Profile = ""
			}
		}
	}
}

func TestProbeSourceReadsColourFromSPSWithoutTrackColour(t *testing.T) {
	for file, want := range map[string]string{
		"hevc_hdr10_eac3_ac3_ass.mkv":   "smpte2084",
		"hevc_hlg_dts.mkv":              "arib-std-b67",
		"h264hi10_truehd_flac_opus.mkv": "", // VUI says unspecified; only the track Colour says bt709
	} {
		data, err := os.ReadFile(filepath.Join("testdata/nativeprobe", file))
		if err != nil {
			t.Fatal(err)
		}
		tracks := fixtureTracks(t, data)
		for _, tr := range tracks {
			if tr.kind != 1 {
				continue
			}
			tr.transfer = 0 // as if the muxer wrote no Colour element
			si, ok, err := tr.stream(0)
			if err != nil || !ok {
				t.Fatalf("%s: stream: %v %v", file, ok, err)
			}
			if si.ColorTransfer != want {
				t.Errorf("%s: SPS transfer = %q, want %q", file, si.ColorTransfer, want)
			}
		}
	}
}

func fixtureTracks(t *testing.T, data []byte) []mkvTrack {
	t.Helper()
	s := &nativeSource{ctx: context.Background(), src: bytes.NewReader(data), size: int64(len(data)), head: data, left: nativeReadBudget}
	_, _, end, err := s.header(0, s.size)
	if err != nil {
		t.Fatal(err)
	}
	_, segStart, segEnd, err := s.header(end, s.size)
	if err != nil {
		t.Fatal(err)
	}
	for off := segStart; off < segEnd; {
		id, start, stop, err := s.header(off, segEnd)
		if err != nil {
			t.Fatal(err)
		}
		if id == mkvTracks {
			tracks, err := mkvTracksOf(data[start:stop])
			if err != nil {
				t.Fatal(err)
			}
			return tracks
		}
		off = stop
	}
	t.Fatal("no Tracks")
	return nil
}

// --- synthetic Matroska -----------------------------------------------------

func ebmlID(id uint64) []byte {
	var b []byte
	for id > 0 {
		b = append([]byte{byte(id)}, b...)
		id >>= 8
	}
	return b
}

func el(id uint64, body ...[]byte) []byte {
	payload := bytes.Join(body, nil)
	size := make([]byte, 8)
	binary.BigEndian.PutUint64(size, uint64(len(payload)))
	size[0] = 0x01
	return append(append(ebmlID(id), size...), payload...)
}

func elUnknown(id uint64, body ...[]byte) []byte {
	return append(append(ebmlID(id), 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff), bytes.Join(body, nil)...)
}

func elU(id, v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return el(id, b)
}

func elF(id uint64, v float64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, math.Float64bits(v))
	return el(id, b)
}

func elS(id uint64, s string) []byte { return el(id, []byte(s)) }

func fixturePrivate(t *testing.T, file, codec string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata/nativeprobe", file))
	if err != nil {
		t.Fatal(err)
	}
	for _, tr := range fixtureTracks(t, data) {
		if tr.codecID == codec {
			return tr.private
		}
	}
	t.Fatalf("%s has no %s", file, codec)
	return nil
}

type metaCountingReader struct {
	r     *bytes.Reader
	reads atomic.Int32
	far   atomic.Int32 // reads beyond the head window
}

func (c *metaCountingReader) ReadMetadataAtContext(_ context.Context, p []byte, off int64) (int, error) {
	c.reads.Add(1)
	if off+int64(len(p)) > nativeHeadBytes {
		c.far.Add(1)
	}
	return c.r.ReadAt(p, off)
}

func (c *metaCountingReader) ReadAt(p []byte, off int64) (int, error) {
	return 0, errors.New("native probe must use metadata reads")
}

func TestProbeSourceFollowsSeekHeadPastAttachments(t *testing.T) {
	hvcc := fixturePrivate(t, "hevc_hdr10_eac3_ac3_ass.mkv", "V_MPEGH/ISO/HEVC")
	dovi := []byte{1, 0, 8<<1 | 0, 6<<3 | 0x5, 0x10} // profile 8, level 6, rpu+bl, compat 1
	tracks := el(mkvTracks,
		el(mkvTrackEntry, elU(mkvTrackNumber, 1), elU(mkvTrackType, 1), elS(mkvCodecID, "V_MPEGH/ISO/HEVC"),
			el(mkvCodecPrivate, hvcc), elU(mkvDefaultDur, 41708333),
			el(mkvVideo, elU(mkvPixelWidth, 3840), elU(mkvPixelHeight, 2160)),
			el(mkvAddMapping, elU(mkvAddIDType, 0x64766343), el(mkvAddIDExtra, dovi))),
		// A complex track takes no stream index in ffmpeg.
		el(mkvTrackEntry, elU(mkvTrackNumber, 2), elU(mkvTrackType, 3), elS(mkvCodecID, "V_COMPLEX")),
		// A metadata track takes an index but is not reported.
		el(mkvTrackEntry, elU(mkvTrackNumber, 3), elU(mkvTrackType, 0x21), elS(mkvCodecID, "D_WEBVTT/METADATA")),
		el(mkvTrackEntry, elU(mkvTrackNumber, 4), elU(mkvTrackType, 2), elS(mkvCodecID, "A_EAC3"),
			elU(mkvFlagDefault, 0), elS(mkvLanguageBCP, "fr-CA"),
			el(mkvAudio, elF(mkvSampling, 48000), elU(mkvChannels, 8))),
	)
	info := el(mkvInfo, elU(mkvTimecodeScl, 1000000), elF(mkvDuration, 5_400_000))
	attachments := el(0x1941a469, make([]byte, 600<<10)) // fonts before the media
	// Segment layout: SeekHead, Info, Attachments, Tracks, Cluster.
	sh := seekHeadFor(0, 0)
	tracksPos := uint64(len(sh) + len(info) + len(attachments))
	sh = seekHeadFor(tracksPos, uint64(len(sh)))
	body := bytes.Join([][]byte{sh, info, attachments, tracks, el(mkvCluster, make([]byte, 1024))}, nil)
	file := append(el(mkvEBML, elS(mkvDocType, "matroska")), elUnknown(mkvSegment, body)...)

	src := &metaCountingReader{r: bytes.NewReader(file)}
	got, err := ProbeSource(context.Background(), src, int64(len(file)))
	if err != nil {
		t.Fatalf("ProbeSource: %v", err)
	}
	if got.Duration != 90*time.Minute {
		t.Errorf("duration = %v", got.Duration)
	}
	if len(got.Streams) != 2 {
		t.Fatalf("streams = %+v", got.Streams)
	}
	v, a := got.Streams[0], got.Streams[1]
	if v.Index != 0 || v.Codec != "hevc" || v.Profile != "Main 10" || v.BitDepth != 10 || v.ColorTransfer != "smpte2084" || v.FrameRate != 23.976 {
		t.Errorf("video = %+v", v)
	}
	if !v.DolbyVision || v.DolbyVisionProfile != 8 || v.DolbyVisionLevel != 6 || v.DolbyVisionCompatibilityID != 1 ||
		v.DolbyVisionBLPresent == nil || !*v.DolbyVisionBLPresent || v.DolbyVisionELPresent == nil || *v.DolbyVisionELPresent || v.DolbyVisionVersion != "1.0" {
		t.Errorf("dolby vision = %+v", v)
	}
	if a.Index != 2 || a.Codec != "eac3" || a.Channels != 8 || a.ChannelLayout != "7.1" || a.SampleRate != 48000 || a.Default || a.Language != "fre" {
		t.Errorf("audio = %+v", a)
	}
	if far := src.far.Load(); far != 2 {
		t.Errorf("reads past the head = %d, want 2 (Tracks header and body)", far)
	}
}

func seekHeadFor(tracksPos, infoPos uint64) []byte {
	return el(mkvSeekHead,
		el(mkvSeek, el(mkvSeekID, ebmlID(mkvTracks)), elU(mkvSeekPosition, tracksPos)),
		el(mkvSeek, el(mkvSeekID, ebmlID(mkvInfo)), elU(mkvSeekPosition, infoPos)))
}

func TestProbeSourceDefersToFFprobe(t *testing.T) {
	avcc := fixturePrivate(t, "h264_aac_srt_chapters.mkv", "V_MPEG4/ISO/AVC")
	video := func(extra ...[]byte) []byte {
		fields := [][]byte{elU(mkvTrackNumber, 1), elU(mkvTrackType, 1), elS(mkvCodecID, "V_MPEG4/ISO/AVC"),
			el(mkvCodecPrivate, avcc), elU(mkvDefaultDur, 40000000),
			el(mkvVideo, elU(mkvPixelWidth, 128), elU(mkvPixelHeight, 72))}
		return el(mkvTrackEntry, append(fields, extra...)...)
	}
	file := func(info []byte, entries ...[]byte) []byte {
		return append(el(mkvEBML, elS(mkvDocType, "matroska")), el(mkvSegment, info, el(mkvTracks, entries...))...)
	}
	info := el(mkvInfo, elF(mkvDuration, 1000))
	for name, data := range map[string][]byte{
		"not matroska":     []byte("RIFF....AVI LIST"),
		"no duration":      file(el(mkvInfo, elU(mkvTimecodeScl, 1000000)), video()),
		"no frame rate":    file(info, el(mkvTrackEntry, elU(mkvTrackNumber, 1), elU(mkvTrackType, 1), elS(mkvCodecID, "V_MPEG4/ISO/AVC"), el(mkvCodecPrivate, avcc), el(mkvVideo, elU(mkvPixelWidth, 128), elU(mkvPixelHeight, 72)))),
		"vfw video":        file(info, el(mkvTrackEntry, elU(mkvTrackNumber, 1), elU(mkvTrackType, 1), elS(mkvCodecID, "V_MS/VFW/FOURCC"), elU(mkvDefaultDur, 40000000), el(mkvVideo, elU(mkvPixelWidth, 128), elU(mkvPixelHeight, 72)))),
		"encrypted":        file(info, video(el(mkvEncodings, el(mkvEncoding, elU(mkvEncType, 1))))),
		"acm audio":        file(info, video(), el(mkvTrackEntry, elU(mkvTrackNumber, 2), elU(mkvTrackType, 2), elS(mkvCodecID, "A_MS/ACM"))),
		"unknown subtitle": file(info, video(), el(mkvTrackEntry, elU(mkvTrackNumber, 2), elU(mkvTrackType, 0x11), elS(mkvCodecID, "S_KATE"))),
		"audio only":       file(info, el(mkvTrackEntry, elU(mkvTrackNumber, 1), elU(mkvTrackType, 2), elS(mkvCodecID, "A_AC3"), el(mkvAudio, elF(mkvSampling, 48000), elU(mkvChannels, 2)))),
	} {
		_, err := ProbeSource(context.Background(), bytes.NewReader(data), int64(len(data)))
		if !errors.Is(err, ErrProbeIncomplete) {
			t.Errorf("%s: err = %v, want ErrProbeIncomplete", name, err)
		}
	}
	// Fragmented MP4 keeps its sample tables in moof boxes.
	frag, err := os.ReadFile("testdata/nativeprobe/h264_aac_fragmented.defer")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ProbeSource(context.Background(), bytes.NewReader(frag), int64(len(frag))); !errors.Is(err, ErrProbeIncomplete) {
		t.Errorf("fragmented mp4: err = %v, want ErrProbeIncomplete", err)
	}
	// The same file with a frame rate is accepted.
	ok := file(info, video())
	if _, err := ProbeSource(context.Background(), bytes.NewReader(ok), int64(len(ok))); err != nil {
		t.Fatalf("control: %v", err)
	}
}

func TestProbeTrailerFillsTailChaptersAndTrackBitrates(t *testing.T) {
	avcc := fixturePrivate(t, "h264_aac_srt_chapters.mkv", "V_MPEG4/ISO/AVC")
	tracks := el(mkvTracks,
		el(mkvTrackEntry, elU(mkvTrackNumber, 1), elU(mkvTrackUID, 77), elU(mkvTrackType, 1), elS(mkvCodecID, "V_MPEG4/ISO/AVC"),
			el(mkvCodecPrivate, avcc), elU(mkvDefaultDur, 40000000), el(mkvVideo, elU(mkvPixelWidth, 128), elU(mkvPixelHeight, 72))),
		el(mkvTrackEntry, elU(mkvTrackNumber, 2), elU(mkvTrackUID, 78), elU(mkvTrackType, 2), elS(mkvCodecID, "A_AC3"),
			el(mkvAudio, elF(mkvSampling, 48000), elU(mkvChannels, 6))))
	info := el(mkvInfo, elF(mkvDuration, 60000))
	cluster := el(mkvCluster, make([]byte, 300<<10))
	chapters := el(mkvChapters, el(mkvEdition,
		el(mkvChapterAtom, elU(mkvChapterUID, 5), elU(mkvChapterStart, 0), el(mkvChapDisplay, elS(mkvChapString, "Intro"))),
		el(mkvChapterAtom, elU(mkvChapterUID, 6), elU(mkvChapterStart, uint64(20*time.Second)), el(mkvChapDisplay, elS(mkvChapString, "Credits")))))
	tag := func(uid uint64, bps string) []byte {
		return el(mkvTag, el(mkvTargets, elU(mkvTagTrackUID, uid)), el(mkvSimpleTag, elS(mkvTagName, "BPS"), elS(mkvTagString, bps)))
	}
	tags := el(mkvTags, tag(77, "4800000"), tag(78, "640000"))
	pre := func(sh []byte) int { return len(sh) + len(info) + len(tracks) + len(cluster) }
	seek := func(chap, tg uint64) []byte {
		return el(mkvSeekHead,
			el(mkvSeek, el(mkvSeekID, ebmlID(mkvChapters)), elU(mkvSeekPosition, chap)),
			el(mkvSeek, el(mkvSeekID, ebmlID(mkvTags)), elU(mkvSeekPosition, tg)))
	}
	sh := seek(0, 0)
	sh = seek(uint64(pre(sh)), uint64(pre(sh)+len(chapters)))
	data := append(el(mkvEBML, elS(mkvDocType, "matroska")), el(mkvSegment, sh, info, tracks, cluster, chapters, tags)...)

	m, err := ProbeSource(context.Background(), bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Chapters) != 0 {
		t.Fatalf("tail chapters read on the Play path: %+v", m.Chapters)
	}
	if !ProbeTrailer(context.Background(), bytes.NewReader(data), int64(len(data)), m) {
		t.Fatal("ProbeTrailer reported no change")
	}
	want := []core.Chapter{{ID: 5, Title: "Intro", End: 20 * time.Second}, {ID: 6, Title: "Credits", Start: 20 * time.Second, End: time.Minute}}
	if len(m.Chapters) != 2 || m.Chapters[0] != want[0] || m.Chapters[1] != want[1] {
		t.Errorf("chapters = %+v", m.Chapters)
	}
	if m.Streams[0].BitrateKbps != 4800 || m.Streams[1].BitrateKbps != 640 {
		t.Errorf("bitrates = %d/%d", m.Streams[0].BitrateKbps, m.Streams[1].BitrateKbps)
	}
}

func TestBitReaderRejectsTruncatedParameterSets(t *testing.T) {
	sps := []byte{0x42, 0x01, 0x01, 0x02, 0x20}
	if _, err := hevcSPS(sps); err == nil {
		t.Error("truncated HEVC SPS accepted")
	}
	if _, err := avcSPS([]byte{0x67, 0x64, 0x00}); err == nil {
		t.Error("truncated AVC SPS accepted")
	}
	if got := rbsp([]byte{0, 0, 3, 1, 0, 0, 3}); !bytes.Equal(got, []byte{0, 0, 1, 0, 0}) {
		t.Errorf("rbsp = % x", got)
	}
}

func closeTo(a, b int, frac float64) bool {
	d := math.Abs(float64(a - b))
	return d <= 1 || d <= frac*math.Max(float64(a), float64(b))
}

func TestMatroskaLanguageMatchesLibavformat(t *testing.T) {
	for _, c := range []struct {
		track mkvTrack
		want  string
	}{
		{mkvTrack{language: "eng", hasLanguage: true, bcp47: "en-US"}, "eng"},
		// BCP47-only pt-BR keeps its region via langtag ("pt-br", not the
		// generic legacy "por"): regional preferences depend on it.
		{mkvTrack{language: "eng", bcp47: "pt-BR"}, "pt-br"},
		// Script is parsed, not read as region: zh-Hant-TW => zh-tw.
		{mkvTrack{bcp47: "zh-Hant-TW"}, "zh-tw"},
		// Primary "x" is not a language: garbage BCP47 keeps the legacy.
		{mkvTrack{language: "eng", bcp47: "x-klingon"}, "eng"},
		{mkvTrack{language: "und", hasLanguage: true}, ""},
	} {
		c.track.kind, c.track.codecID, c.track.channels, c.track.sampling = 2, "A_AC3", 2, 48000
		si, ok, err := c.track.stream(1)
		if err != nil || !ok || si.Language != c.want {
			t.Errorf("%+v: language %q (ok %v, err %v), want %q", c.track, si.Language, ok, err, c.want)
		}
	}
}
