package langtag

import "testing"

// tierCorpus is the single regional matrix shared (by semantics, not by
// import) with the consumer tests in subtitles, trackprefs and plex.
var tierCorpus = []struct {
	want, track string
	tier        int
}{
	{"pt", "pt", 3}, {"pt", "pt-br", 2}, {"pt", "pt-pt", 2},
	{"pt-br", "pt", 1}, {"pt-br", "pt-br", 3}, {"pt-br", "pt-pt", 0},
	{"pt-pt", "pt", 1}, {"pt-pt", "pt-br", 0}, {"pt-pt", "pt-pt", 3},
	{"por", "pt", 3}, {"por", "pt-br", 2}, {"por", "pt-pt", 2},
	{"zh", "zh", 3}, {"zh", "zh-cn", 2}, {"zh", "zh-tw", 2},
	{"zh-cn", "zh", 1}, {"zh-cn", "zh-cn", 3}, {"zh-cn", "zh-tw", 0},
	{"zh-tw", "zh", 1}, {"zh-tw", "zh-cn", 0}, {"zh-tw", "zh-tw", 3},
	// Script must not be read as region: both sides fold to zh-tw.
	{"zh-TW", "zh-Hant-TW", 3}, {"zh-Hant-TW", "zh-TW", 3},
	{"zh", "zh-Hant", 2}, {"zh", "zh-Hans", 2},
	{"zh-tw", "zh-Hans", 0}, {"zh-cn", "zh-Hant", 0},
	{"zh-Hans", "zh", 1}, {"zh-Hant", "zh-tw", 3},
	{"pt-BR", "pt-Latn-BR", 3}, {"pt", "pt-Latn-BR", 2},
	// Non-pt/zh regions fold away before matching.
	{"es-419", "es", 3}, {"es", "es-419", 3}, {"en-GB", "en", 3}, {"en", "en-GB", 3},
	{"en", "pt-br", 0}, {"", "pt", 0}, {"pt", "", 0}, {"und", "en", 0},
}

func TestMatchTierCorpus(t *testing.T) {
	for _, c := range tierCorpus {
		if got := MatchTier(c.want, c.track); got != c.tier {
			t.Errorf("MatchTier(%q, %q) = %d, want %d", c.want, c.track, got, c.tier)
		}
	}
	if got := MatchTier("pt-br", "pob"); got != 3 {
		t.Errorf("MatchTier(pt-br, pob) = %d, want 3", got)
	}
	if got := MatchTier("POR", "PT-BR"); got != 2 {
		t.Errorf("MatchTier(POR, PT-BR) = %d, want 2", got)
	}
}

func TestNormalizeCorpus(t *testing.T) {
	for in, want := range map[string]string{
		"eng": "en", "EN-us": "en", "en-GB": "en", "pt_BR": "pt-br",
		"pob": "pt-br", "por": "pt", "por-PT": "pt-pt", "pt-PT": "pt-pt",
		"zh-TW": "zh-tw", "chi": "zh", "zho-tw": "zh-tw", "fre": "fr",
		"zh-Hant-TW": "zh-tw", "zh-Hans": "zh-cn", "zh-Hant": "zh-tw",
		"zh-Hans-CN": "zh-cn", "zh-Hant-CN": "zh-cn", "zh-Hans-HK": "zh-hk",
		"pt-Latn-BR": "pt-br", "es-419": "es", "spa": "es", "und": "",
		"unknown": "", "": "", "x-klingon": "", "english": "",
	} {
		if got := Normalize(in); got != want {
			t.Errorf("Normalize(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestParseBCP47(t *testing.T) {
	for in, want := range map[string]Tag{
		"zh-Hant-TW": {Primary: "zh", Script: "hant", Region: "tw"},
		"zh-Hans":    {Primary: "zh", Script: "hans"},
		"zh-TW":      {Primary: "zh", Region: "tw"},
		"pt-Latn-BR": {Primary: "pt", Script: "latn", Region: "br"},
		"es-419":     {Primary: "es", Region: "419"},
		"en-GB":      {Primary: "en", Region: "gb"},
		"eng-US":     {Primary: "en", Region: "us"},
		"pob":        {Primary: "pt", Region: "br"},
		"de-CH-1901": {Primary: "de", Region: "ch"},
		"en":         {Primary: "en"},
	} {
		got, ok := ParseBCP47(in)
		if !ok || got != want {
			t.Errorf("ParseBCP47(%q) = %+v, %v; want %+v, true", in, got, ok, want)
		}
	}
	for _, bad := range []string{"", " ", "e", "english", "en-", "-en", "en--US", "123", "en-1", "zh-han", "en US", "x-klingon", "en-US-abc"} {
		if got, ok := ParseBCP47(bad); ok {
			t.Errorf("ParseBCP47(%q) = %+v, true; want rejected", bad, got)
		}
	}
}
