package plex

import "testing"

// TestSameLanguageZhScriptRegion: the script subtag is not a region, so
// zh-TW matches zh-Hant-TW but not zh-Hans (folded to zh-cn).
func TestSameLanguageZhScriptRegion(t *testing.T) {
	if !sameLanguage("zh-TW", "zh-Hant-TW") {
		t.Error("sameLanguage(zh-TW, zh-Hant-TW) = false, want true")
	}
	if sameLanguage("zh-TW", "zh-Hans") {
		t.Error("sameLanguage(zh-TW, zh-Hans) = true, want false")
	}
	if got := languageName("pt-br"); got != "Portuguese" {
		t.Errorf("languageName(pt-br) = %q, want Portuguese", got)
	}
	if got := languageCode3("pt-br"); got != "por" {
		t.Errorf("languageCode3(pt-br) = %q, want por", got)
	}
}
