// Package langtag is the single shared language-tag helper for Ether.
//
// It exists so subtitles, trackprefs, plex and the transcode probes do
// not drift apart with private copies of the same normalisation: a tag
// is parsed as BCP 47 (primary, optional script, optional region),
// folded to a canonical comparable form, and matched by tier.
package langtag

import "strings"

// Tag is a parsed BCP 47 tag. Primary is canonical: known ISO 639-2
// codes fold to their ISO 639-1 form ("eng" -> "en", "por" -> "pt"),
// and the legacy OpenSubtitles code "pob" parses as primary "pt" with
// region "br". Script and Region are lower-case and empty when absent.
type Tag struct {
	Primary string
	Script  string
	Region  string
}

// iso6392to1 maps ISO 639-2 (B and T variants) primary subtags to
// ISO 639-1. Unknown 3-letter primaries pass through unchanged.
var iso6392to1 = map[string]string{
	"eng": "en", "spa": "es", "fre": "fr", "fra": "fr", "ger": "de", "deu": "de",
	"ita": "it", "por": "pt", "dut": "nl", "nld": "nl", "jpn": "ja", "kor": "ko",
	"chi": "zh", "zho": "zh", "rus": "ru", "ara": "ar", "hin": "hi", "swe": "sv",
	"nor": "no", "nob": "nb", "nno": "nn", "dan": "da", "fin": "fi", "pol": "pl",
	"tur": "tr", "cze": "cs", "ces": "cs", "hun": "hu", "ukr": "uk", "gre": "el",
	"ell": "el", "heb": "he", "tha": "th", "vie": "vi", "ind": "id", "rum": "ro",
	"ron": "ro", "bul": "bg", "hrv": "hr", "srp": "sr", "slo": "sk", "slk": "sk",
	"slv": "sl", "est": "et", "lav": "lv", "lit": "lt", "per": "fa", "fas": "fa",
	"may": "ms", "msa": "ms", "tam": "ta", "tel": "te", "ice": "is", "isl": "is",
	"cat": "ca", "baq": "eu", "eus": "eu", "glg": "gl", "mkd": "mk", "sqi": "sq",
	"hye": "hy", "kat": "ka", "bos": "bs",
}

func isAlpha(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < 'a' || s[i] > 'z' {
			return false
		}
	}
	return len(s) > 0
}

func isDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return len(s) > 0
}

func isAlnum(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') {
			return false
		}
	}
	return len(s) > 0
}

// ParseBCP47 parses a BCP 47 tag into primary, script and region.
//
// The region is the 2-letter or 3-digit subtag that follows the
// optional 4-letter script subtag — never the script itself, so
// "zh-Hant-TW" yields primary "zh", script "hant", region "tw"
// (taking the second subtag as the region would read "hant" and lose
// "tw"). Variant and extension subtags after the region are validated
// but not reported. Garbage — empty subtags, a primary that is not
// 2-3 ASCII letters, stray short subtags — is rejected (ok is false).
func ParseBCP47(tag string) (Tag, bool) {
	s := strings.ReplaceAll(strings.ToLower(strings.TrimSpace(tag)), "_", "-")
	if s == "" {
		return Tag{}, false
	}
	parts := strings.Split(s, "-")
	for _, p := range parts {
		if len(p) > 8 || !isAlnum(p) {
			return Tag{}, false
		}
	}
	primary := parts[0]
	if len(primary) < 2 || len(primary) > 3 || !isAlpha(primary) {
		return Tag{}, false
	}
	if primary == "pob" {
		return Tag{Primary: "pt", Region: "br"}, true
	}
	if one, ok := iso6392to1[primary]; ok {
		primary = one
	}
	out := Tag{Primary: primary}
	i := 1
	if i < len(parts) && len(parts[i]) == 4 && isAlpha(parts[i]) {
		out.Script = parts[i]
		i++
	}
	if i < len(parts) && ((len(parts[i]) == 2 && isAlpha(parts[i])) || (len(parts[i]) == 3 && isDigits(parts[i]))) {
		out.Region = parts[i]
		i++
	}
	// Remaining subtags: variants (5-8 alnum, or 4 starting with a
	// digit) and extension sequences (a 1-char singleton followed by
	// 2-8 alnum subtags). Anything else is garbage.
	extension := false // in an extension; a singleton still owes a subtag
	for ; i < len(parts); i++ {
		p := parts[i]
		switch {
		case len(p) == 1:
			if extension && len(parts[i-1]) == 1 {
				return Tag{}, false // singleton with no subtag before it
			}
			extension = true
		case extension:
			// Extension subtag; stay in the extension.
		case len(p) >= 5 || (len(p) == 4 && p[0] >= '0' && p[0] <= '9'):
			// Variant.
		default:
			return Tag{}, false
		}
	}
	if extension && len(parts[len(parts)-1]) == 1 {
		return Tag{}, false // trailing singleton without subtags
	}
	return out, true
}

// Normalize folds a language tag to its canonical comparable form:
// the primary subtag, plus the region only for Portuguese and Chinese
// (pt-br vs pt-pt and zh-cn vs zh-tw are distinct languages to every
// consumer; en-GB and en-US are not). The script is discarded, except
// that for Chinese a script without an explicit region implies one by
// convention: zh-Hans -> zh-cn, zh-Hant -> zh-tw (an explicit region
// always wins: zh-Hant-CN -> zh-cn). "pob" folds to "pt-br", ISO 639-2
// primaries to ISO 639-1, and empty, invalid or undetermined tags to "".
func Normalize(tag string) string {
	t, ok := ParseBCP47(tag)
	if !ok || t.Primary == "und" {
		return ""
	}
	if t.Primary == "pt" || t.Primary == "zh" {
		region := t.Region
		if region == "" && t.Primary == "zh" {
			switch t.Script {
			case "hans":
				region = "cn"
			case "hant":
				region = "tw"
			}
		}
		if region != "" {
			return t.Primary + "-" + region
		}
	}
	return t.Primary
}

// MatchTier scores a wanted language against a track language, both
// normalised with Normalize:
//
//   - 3: exact (pt-br × pt-br, pt × pt, por × pt after normalising);
//   - 2: generic want × regional track (pt × pt-br, zh × zh-tw) — a
//     generic preference accepts any region of the language;
//   - 1: regional want × generic track (pt-br × pt, zh-tw × zh) —
//     accepted, but weak: it never beats tier 3 or 2 for the same want;
//   - 0: different languages, regions that differ (pt-br × pt-pt,
//     zh-cn × zh-tw), or an empty/invalid side.
//
// Because only pt/zh keep a region after normalising, script is never
// mistaken for region here: zh-TW × zh-Hant-TW both fold to zh-tw (3).
func MatchTier(want, track string) int {
	w, t := Normalize(want), Normalize(track)
	if w == "" || t == "" {
		return 0
	}
	if w == t {
		return 3
	}
	wb, wr, _ := strings.Cut(w, "-")
	tb, tr, _ := strings.Cut(t, "-")
	if wb != tb {
		return 0
	}
	switch {
	case wr == "" && tr != "":
		return 2
	case wr != "" && tr == "":
		return 1
	default:
		return 0
	}
}
