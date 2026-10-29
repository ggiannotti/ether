package plex

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/braedonsaunders/ether/internal/config"
	"github.com/braedonsaunders/ether/internal/core"
	"github.com/braedonsaunders/ether/internal/langtag"
	"github.com/braedonsaunders/ether/internal/store"
	"github.com/braedonsaunders/ether/internal/trackprefs"
)

// fakeSubtitles is Deps.Subtitles: cached sidecars per title, viewer picks,
// canned search offers, and a record of prefetch/timeline calls.
type fakeSubtitles struct {
	mu         sync.Mutex
	cached     map[core.RatingKey][]core.StreamInfo // automatic, one per language
	picks      map[core.RatingKey][]core.StreamInfo // viewer picks via search
	files      map[string]string                    // ExternalURL -> SRT
	offers     []SubtitleOffer
	searchErr  error
	searchLang string
	searchRel  string
	prefetched []core.RatingKey
	atOffset   time.Duration
	atFormat   string
}

func newFakeSubtitles() *fakeSubtitles {
	return &fakeSubtitles{cached: map[core.RatingKey][]core.StreamInfo{}, picks: map[core.RatingKey][]core.StreamInfo{}, files: map[string]string{}}
}

const sampleSRT = "1\n00:00:05,000 --> 00:00:07,000\nThey're coming to get you.\n\n"

func srtToVTT(s string) string {
	return "WEBVTT\n\n" + regexp.MustCompile(`(\d\d),(\d\d\d)`).ReplaceAllString(s, "$1.$2")
}

func sidecar(lang, url string) core.StreamInfo {
	return core.StreamInfo{Index: 9, Type: "subtitle", Codec: "srt", Language: lang, External: true, ExternalURL: url}
}

func (f *fakeSubtitles) cache(key core.RatingKey, st core.StreamInfo) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cached[key] = append(f.cached[key], st)
	f.files[st.ExternalURL] = sampleSRT
}

func (f *fakeSubtitles) OpenFormat(id, format string) (io.ReadCloser, string, error) {
	f.mu.Lock()
	text, ok := f.files[id]
	f.mu.Unlock()
	if !ok {
		return nil, "", core.ErrNotFound
	}
	if format == "vtt" {
		return io.NopCloser(strings.NewReader(srtToVTT(text))), "vtt", nil
	}
	return io.NopCloser(strings.NewReader(text)), "srt", nil
}

func (f *fakeSubtitles) OpenFormatAt(id, format string, offset time.Duration) (io.ReadCloser, string, error) {
	f.mu.Lock()
	f.atOffset, f.atFormat = offset, format
	f.mu.Unlock()
	rc, actual, err := f.OpenFormat(id, format)
	if err != nil {
		return nil, "", err
	}
	b, _ := io.ReadAll(rc)
	return io.NopCloser(strings.NewReader("shifted " + offset.String() + "\n" + string(b))), actual, nil
}

// CachedSidecars mirrors subtitles.Service: picks always (unless listed),
// automatic tracks only for languages embedded does not cover.
func (f *fakeSubtitles) CachedSidecars(_ context.Context, t *core.Title, _ *core.User, embedded []core.StreamInfo) []core.StreamInfo {
	f.mu.Lock()
	defer f.mu.Unlock()
	present, langs := map[string]bool{}, map[string]bool{}
	for _, st := range embedded {
		present[st.ExternalURL] = st.External
		if st.Type == "subtitle" {
			langs[langtag.Normalize(st.Language)] = true
		}
	}
	var out []core.StreamInfo
	for _, st := range f.picks[t.Key] {
		if !present[st.ExternalURL] {
			out = append(out, st)
			langs[langtag.Normalize(st.Language)] = true
		}
	}
	for _, st := range f.cached[t.Key] {
		if !present[st.ExternalURL] && !langs[langtag.Normalize(st.Language)] {
			out = append(out, st)
		}
	}
	return out
}

func (f *fakeSubtitles) PrefetchSidecars(_ context.Context, t *core.Title, _ *core.User) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.prefetched = append(f.prefetched, t.Key)
	return true
}

func (f *fakeSubtitles) SearchSubtitles(_ context.Context, t *core.Title, _ *core.User, lang, release string) ([]SubtitleOffer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.searchLang, f.searchRel = lang, release
	return f.offers, f.searchErr
}

func (f *fakeSubtitles) DownloadSubtitle(_ context.Context, t *core.Title, _ *core.User, id int64) (core.StreamInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, o := range f.offers {
		if o.ID == id {
			st := sidecar(o.Language, fmt.Sprintf("ether-subtitle:pick-%d", id))
			f.picks[t.Key] = append(f.picks[t.Key], st)
			f.files[st.ExternalURL] = sampleSRT
			return st, nil
		}
	}
	return core.StreamInfo{}, core.ErrNotFound
}

var (
	// estimateInfo is what playback.Estimate gives before a session: a
	// video and an audio stream, no subtitle tracks.
	estimateInfo = core.MediaInfo{
		Container: "mkv", Duration: 139 * time.Minute, BitrateKbps: 7600, Estimated: true,
		Streams: []core.StreamInfo{
			{Index: 0, Type: "video", Codec: "h264", Width: 1920, Height: 1080, Default: true},
			{Index: 1, Type: "audio", Codec: "eac3", Language: "en", Channels: 6, Default: true},
		},
	}
	// probedInfo is the session's media: more embedded streams, and the
	// sidecar Enrich chose for this release (a different file than the one
	// cached before play).
	probedInfo = core.MediaInfo{
		Container: "mkv", Duration: 139 * time.Minute, Size: 8_000_000_000, BitrateKbps: 7600,
		Streams: []core.StreamInfo{
			{Index: 0, Type: "video", Codec: "h264", Width: 1920, Height: 1080},
			{Index: 1, Type: "audio", Codec: "eac3", Language: "en", Channels: 6, Default: true},
			{Index: 2, Type: "audio", Codec: "aac", Language: "fr", Channels: 2},
			{Index: 3, Type: "subtitle", Codec: "subrip", Language: "fr"},
			{Index: 4, Type: "subtitle", Codec: "srt", Language: "en", External: true, ExternalURL: "ether-subtitle:release-en", Title: "EN (OpenSubtitles: Fight.Club.1999.1080p)"},
		},
	}
)

func newSubtitleFixture(t *testing.T) (*fixture, *fakeSubtitles) {
	t.Helper()
	subs := newFakeSubtitles()
	f := newFixture(func(d *Deps) { d.Subtitles = subs })
	t.Cleanup(func() { f.srv.Close() })
	f.playback.info = estimateInfo
	subs.cache(fightClub.Key, sidecar("en", "ether-subtitle:cached-en"))
	subs.files["ether-subtitle:release-en"] = sampleSRT
	return f, subs
}

var streamTagRe = regexp.MustCompile(`<Stream [^>]*>`)

// subtitleTags returns the streamType="3" Stream elements of an XML body.
func subtitleTags(body string) []string {
	var out []string
	for _, tag := range streamTagRe.FindAllString(body, -1) {
		if strings.Contains(tag, `streamType="3"`) {
			out = append(out, tag)
		}
	}
	return out
}

func externalID(key core.RatingKey, url string) int64 {
	return slotHash(key, "external/"+url)
}

func TestCachedSidecarListedBeforePlayback(t *testing.T) {
	f, _ := newSubtitleFixture(t)
	id := externalID(fightClub.Key, "ether-subtitle:cached-en")

	rec := f.get(t, "/library/metadata/"+fightClub.Key.String())
	subs := subtitleTags(rec.Body.String())
	want := fmt.Sprintf(`<Stream id="%d" streamType="3" canAutoSync="0" codec="srt" key="/library/streams/%d" language="English" languageTag="en" languageCode="eng" format="srt" displayTitle="English (SRT External)" extendedDisplayTitle="English (SRT External)" external="1" />`, id, id)
	if len(subs) != 1 || subs[0] != want {
		t.Fatalf("detail subtitle streams:\n got %q\nwant %q", subs, want)
	}

	// The play queue item (what the player builds its menus from) lists
	// the same stream under the same id.
	uri := url.QueryEscape("server://m/com.plexapp.plugins.library/library/metadata/" + fightClub.Key.String())
	rec = f.request(t, http.MethodPost, "/playQueues?type=video&uri="+uri, nil)
	if got := subtitleTags(rec.Body.String()); len(got) != 1 || !strings.Contains(got[0], fmt.Sprintf(`id="%d"`, id)) || !strings.Contains(got[0], `external="1"`) {
		t.Fatalf("play queue subtitle streams: %q\n%s", got, rec.Body.String())
	}
}

func TestSidecarStreamServesRequestedFormat(t *testing.T) {
	f, _ := newSubtitleFixture(t)
	f.get(t, "/library/metadata/"+fightClub.Key.String()) // registers stream ids
	id := externalID(fightClub.Key, "ether-subtitle:cached-en")
	for _, tc := range []struct {
		path, ctype, prefix string
	}{
		{fmt.Sprintf("/library/streams/%d", id), "application/x-subrip", "1\n00:00:05,000"},
		{fmt.Sprintf("/library/streams/%d?encoding=utf-8&format=srt", id), "application/x-subrip", "1\n00:00:05,000"},
		{fmt.Sprintf("/library/streams/%d?format=webvtt", id), "text/vtt", "WEBVTT\n\n1\n00:00:05.000"},
		{fmt.Sprintf("/library/streams/%d.vtt", id), "text/vtt", "WEBVTT"},
	} {
		rec := f.get(t, tc.path)
		if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != tc.ctype || !strings.HasPrefix(rec.Body.String(), tc.prefix) {
			t.Fatalf("%s: %d %q %q", tc.path, rec.Code, rec.Header().Get("Content-Type"), rec.Body.String())
		}
	}
	if rec := f.get(t, "/library/streams/12345"); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown stream: %d", rec.Code)
	}
}

func TestSidecarSelectionSurvivesProbeAndRemux(t *testing.T) {
	f, _ := newSubtitleFixture(t)
	key := fightClub.Key.String()
	cachedID := externalID(fightClub.Key, "ether-subtitle:cached-en")
	f.get(t, "/library/metadata/"+key)
	f.request(t, http.MethodPut, fmt.Sprintf("/library/parts/%s?subtitleStreamID=%d&allParts=1", key, cachedID), nil)
	if subs := subtitleTags(f.get(t, "/library/metadata/"+key).Body.String()); len(subs) != 1 || !strings.Contains(subs[0], `selected="1"`) {
		t.Fatalf("selection not shown: %q", subs)
	}

	// Play: the probed release has more streams and its own English
	// sidecar. The viewer's English choice follows it; text sidecars are
	// served to the client, never passed into the remux.
	f.playback.info = probedInfo
	rec := f.get(t, "/video/:/transcode/universal/decision?path=/library/metadata/"+key+"&directPlay=0&directStream=1&session=remux1&protocol=hls")
	releaseID := externalID(fightClub.Key, "ether-subtitle:release-en")
	subs := subtitleTags(rec.Body.String())
	if len(subs) != 1 || !strings.Contains(subs[0], fmt.Sprintf(`id="%d"`, releaseID)) || !strings.Contains(subs[0], `selected="1"`) ||
		!strings.Contains(subs[0], fmt.Sprintf(`key="/library/streams/%d"`, releaseID)) || !strings.Contains(subs[0], `decision="copy" location="sidecar"`) {
		t.Fatalf("decision subtitle: %q\n%s", subs, rec.Body.String())
	}
	if f.trans.lastOpts.SubtitleStream != -1 {
		t.Fatalf("sidecar reached the transcoder: %+v", f.trans.lastOpts)
	}

	// Off.
	f.request(t, http.MethodPut, "/library/parts/"+key+"?subtitleStreamID=0", nil)
	for _, tag := range subtitleTags(f.get(t, "/library/metadata/"+key).Body.String()) {
		if strings.Contains(tag, `selected="1"`) {
			t.Fatalf("subtitle still selected after off: %s", tag)
		}
	}
}

func TestSubtitleStreamIDOnDecisionPicksSidecar(t *testing.T) {
	f, _ := newSubtitleFixture(t)
	f.playback.info = probedInfo
	key := fightClub.Key.String()
	f.get(t, "/library/metadata/"+key)
	releaseID := externalID(fightClub.Key, "ether-subtitle:release-en")
	// Direct play lists only the selected streams, the sidecar among them.
	rec := f.get(t, fmt.Sprintf("/video/:/transcode/universal/decision?path=/library/metadata/%s&subtitleStreamID=%d&session=dp1", key, releaseID))
	subs := subtitleTags(rec.Body.String())
	if len(subs) != 1 || !strings.Contains(subs[0], fmt.Sprintf(`key="/library/streams/%d"`, releaseID)) || !strings.Contains(subs[0], `selected="1"`) {
		t.Fatalf("direct play subtitle: %q\n%s", subs, rec.Body.String())
	}
}

func TestTrackPreferenceAlwaysSelectsCachedSidecar(t *testing.T) {
	f, _ := newSubtitleFixture(t)
	db, err := store.OpenMemory(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	prefs := trackprefs.New(db)
	f.srv.d.TrackPreferences = prefs
	r := trackprefs.Default()
	r.SubtitleLanguages = []string{"en"}
	r.SubtitleMode = "always"
	if err := prefs.PutRules(context.Background(), 0, r); err != nil {
		t.Fatal(err)
	}
	subs := subtitleTags(f.get(t, "/library/metadata/"+fightClub.Key.String()).Body.String())
	if len(subs) != 1 || !strings.Contains(subs[0], `selected="1"`) {
		t.Fatalf("always mode did not select the sidecar: %q", subs)
	}
	// And at Play, without any explicit choice.
	f.playback.info = probedInfo
	rec := f.get(t, "/video/:/transcode/universal/decision?path=/library/metadata/"+fightClub.Key.String()+"&session=always1")
	if got := subtitleTags(rec.Body.String()); len(got) != 1 || !strings.Contains(got[0], `external="1"`) || !strings.Contains(got[0], `selected="1"`) {
		t.Fatalf("always mode at play: %q", got)
	}
}

func TestSubtitleSearchAndPick(t *testing.T) {
	f, subs := newSubtitleFixture(t)
	subs.offers = []SubtitleOffer{
		{ID: 102, Language: "en", Title: "Fight.Club.1999.SDH", Provider: "OpenSubtitles", Score: 90, HearingImpaired: true},
		{ID: 101, Language: "en", Title: "Fight.Club.1999.1080p.BluRay", Provider: "OpenSubtitles", Score: 80, PerfectMatch: true},
	}
	key := fightClub.Key.String()
	mc := decodeJSON(t, f.get(t, "/library/metadata/"+key+"/subtitles?language=eng&hearingImpaired=0&forced=0", jsonAccept...))
	results, _ := mc["Stream"].([]any)
	if mc["size"].(float64) != 2 || len(results) != 2 || subs.searchLang != "eng" {
		t.Fatalf("search: %v (lang %q)", mc, subs.searchLang)
	}
	first := results[0].(map[string]any)
	wantKey := "/library/metadata/" + key + "/subtitles/101"
	if first["key"] != wantKey || first["streamType"].(float64) != 3 || first["codec"] != "srt" || first["languageCode"] != "eng" ||
		first["providerTitle"] != "OpenSubtitles" || first["perfectMatch"] != true || first["hearingImpaired"] != false ||
		first["title"] != "Fight.Club.1999.1080p.BluRay" || first["displayTitle"] != "English (SRT)" {
		t.Fatalf("first result (non-SDH preferred): %v", first)
	}
	mc = decodeJSON(t, f.get(t, "/library/metadata/"+key+"/subtitles?language=eng&hearingImpaired=2", jsonAccept...))
	if mc["size"].(float64) != 1 {
		t.Fatalf("SDH-only filter: %v", mc)
	}

	// Picks are validated against the title's search results.
	if rec := f.request(t, http.MethodPut, "/library/metadata/"+key+"/subtitles?key="+url.QueryEscape("/library/metadata/"+alien.Key.String()+"/subtitles/101"), nil); rec.Code != http.StatusBadRequest {
		t.Fatalf("foreign key: %d", rec.Code)
	}
	if rec := f.request(t, http.MethodPut, "/library/metadata/"+key+"/subtitles?key="+url.QueryEscape("/library/metadata/"+key+"/subtitles/999"), nil); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown offer: %d", rec.Code)
	}
	if rec := f.request(t, http.MethodPut, "/library/metadata/"+key+"/subtitles?key="+url.QueryEscape(wantKey), nil); rec.Code != http.StatusOK {
		t.Fatalf("pick: %d %s", rec.Code, rec.Body.String())
	}
	pickID := externalID(fightClub.Key, "ether-subtitle:pick-101")
	found := false
	for _, tag := range subtitleTags(f.get(t, "/library/metadata/"+key).Body.String()) {
		if strings.Contains(tag, fmt.Sprintf(`id="%d"`, pickID)) {
			found = true
			if !strings.Contains(tag, `selected="1"`) || !strings.Contains(tag, `external="1"`) {
				t.Fatalf("picked track not selected: %s", tag)
			}
		} else if strings.Contains(tag, `selected="1"`) {
			t.Fatalf("other track selected: %s", tag)
		}
	}
	if !found {
		t.Fatal("picked track not listed")
	}
	if rec := f.get(t, "/library/streams/"+strconv.FormatInt(pickID, 10)); rec.Code != http.StatusOK || rec.Body.String() != sampleSRT {
		t.Fatalf("picked track body: %d %q", rec.Code, rec.Body.String())
	}

	// A failing provider answers an empty container, never an error the
	// client would keep retrying.
	subs.searchErr = fmt.Errorf("provider down")
	rec := f.get(t, "/library/metadata/"+key+"/subtitles?language=eng")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `<MediaContainer size="0"`) {
		t.Fatalf("provider error: %d %s", rec.Code, rec.Body.String())
	}
	if rec := f.get(t, "/library/metadata/1000000000999999/subtitles?language=eng"); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown title: %d", rec.Code)
	}
}

func TestSubtitleSearchWithoutServiceIsEmpty(t *testing.T) {
	f := newFixture()
	defer f.srv.Close()
	rec := f.get(t, "/library/metadata/"+fightClub.Key.String()+"/subtitles?language=eng")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `<MediaContainer size="0"`) {
		t.Fatalf("search without service: %d %s", rec.Code, rec.Body.String())
	}
}

func TestTranscodeSidecarSubtitlesFollowSessionOffset(t *testing.T) {
	f, subs := newSubtitleFixture(t)
	f.playback.info = probedInfo
	f.trans.decision = core.Decision{Kind: core.DecisionRemux, VideoCopy: true, AudioCopy: true, Container: "mpegts"}
	key := fightClub.Key.String()
	f.get(t, "/library/metadata/"+key)
	releaseID := externalID(fightClub.Key, "ether-subtitle:release-en")
	path := url.QueryEscape("/library/metadata/" + key)
	rec := f.get(t, fmt.Sprintf("/video/:/transcode/universal/start.m3u8?path=%s&session=hls1&protocol=hls&offset=30&directPlay=0&directStream=1&subtitleStreamID=%d", path, releaseID))
	if rec.Code != http.StatusOK {
		t.Fatalf("start: %d %s", rec.Code, rec.Body.String())
	}
	rec = f.get(t, fmt.Sprintf("/video/:/transcode/universal/subtitles?path=%s&session=hls1&offset=30&directPlay=0&directStream=1&subtitleStreamID=%d", path, releaseID))
	if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != "application/x-subrip" || !strings.HasPrefix(rec.Body.String(), "shifted 30s\n1\n00:00:05,000") {
		t.Fatalf("sidecar: %d %q %q", rec.Code, rec.Header().Get("Content-Type"), rec.Body.String())
	}
	// The session's own selection applies without an explicit id; WebVTT on request.
	rec = f.get(t, "/video/:/transcode/universal/subtitles?session=hls1&format=webvtt")
	if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != "text/vtt" || subs.atOffset != 30*time.Second || subs.atFormat != "vtt" || !strings.Contains(rec.Body.String(), "WEBVTT") {
		t.Fatalf("session sidecar: %d %q offset=%v format=%q", rec.Code, rec.Body.String(), subs.atOffset, subs.atFormat)
	}
	// Another device cannot read the session.
	rec = f.get(t, "/video/:/transcode/universal/subtitles?session=hls1", "X-Plex-Client-Identifier", "client-2")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `<MediaContainer size="0"`) {
		t.Fatalf("foreign client: %d %s", rec.Code, rec.Body.String())
	}
	// Subtitles off: nothing to serve.
	f.get(t, fmt.Sprintf("/video/:/transcode/universal/start.m3u8?path=%s&session=hls2&protocol=hls&directPlay=0&directStream=1&subtitleStreamID=0", path))
	rec = f.get(t, "/video/:/transcode/universal/subtitles?session=hls2")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `<MediaContainer size="0"`) {
		t.Fatalf("no subtitle: %d %s", rec.Code, rec.Body.String())
	}
}

func TestDetailPrefetchesSidecarsWhenNotWarmed(t *testing.T) {
	f, subs := newSubtitleFixture(t)
	f.get(t, "/library/metadata/"+fightClub.Key.String())
	if len(subs.prefetched) != 1 || subs.prefetched[0] != fightClub.Key {
		t.Fatalf("prefetch: %v", subs.prefetched)
	}
	// Lists and non-playable pages do not prefetch.
	f.get(t, "/library/metadata/"+breakingBad.Key.String())
	f.get(t, "/library/sections/1/all?type=1")
	if len(subs.prefetched) != 1 {
		t.Fatalf("unexpected prefetch: %v", subs.prefetched)
	}
	base := f.srv.d.Config
	f.srv.d.Config = func() config.Config {
		c := base()
		c.Cache.DisableSpeculation = true
		return c
	}
	f.get(t, "/library/metadata/"+alien.Key.String())
	if len(subs.prefetched) != 1 {
		t.Fatalf("prefetch with speculation off: %v", subs.prefetched)
	}
}
