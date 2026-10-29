package trackprefs

import (
	"context"
	"testing"

	"github.com/braedonsaunders/ether/internal/core"
	"github.com/braedonsaunders/ether/internal/langtag"
	"github.com/braedonsaunders/ether/internal/store"
)

// TestSelectZhScriptRegion locks the langtag consumer behaviour for
// script+region tags: a zh-TW want must select a zh-Hant-TW track (the
// script "Hant" is not a region) and must not select zh-Hans.
func TestSelectZhScriptRegion(t *testing.T) {
	if got := langtag.MatchTier("zh-TW", "zh-Hant-TW"); got == 0 {
		t.Fatalf("MatchTier(zh-TW, zh-Hant-TW) = 0, want a match")
	}
	ctx := context.Background()
	db, err := store.OpenMemory(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := New(db)
	r := Default()
	r.SubtitleLanguages = []string{"zh-TW"}
	r.SubtitleMode = "always"
	if err := s.PutRules(ctx, 7, r); err != nil {
		t.Fatal(err)
	}
	info := core.MediaInfo{Streams: []core.StreamInfo{
		{Type: "audio", Language: "en", Index: 0},
		{Type: "subtitle", Language: "zh-Hans", Index: 1},
		{Type: "subtitle", Language: "zh-Hant-TW", Index: 2},
	}}
	sel, err := s.Select(ctx, 7, core.MovieKey(1), info, "")
	if err != nil {
		t.Fatal(err)
	}
	if sel.Subtitle != 2 {
		t.Fatalf("Select(zh-TW want, [zh-Hans zh-Hant-TW]) subtitle = %d, want 2", sel.Subtitle)
	}
	if got := rankLanguage("zh-Hans", []string{"zh-TW"}); got != 0 {
		t.Fatalf("rankLanguage(zh-Hans, [zh-TW]) = %d, want 0 (distinct regions)", got)
	}
}
