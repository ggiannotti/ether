package subtitles

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf16"
	"unicode/utf8"

	"golang.org/x/text/encoding/charmap"

	"github.com/braedonsaunders/ether/internal/langtag"
)

// Cue is one subtitle cue.
type Cue struct {
	Start, End time.Duration
	Text       string
}

// legacyCharmap is the Windows code page legacy subtitle files in a
// language are almost always encoded in.
func legacyCharmap(lang string) *charmap.Charmap {
	switch langtag.Normalize(lang) {
	case "ru", "uk", "bg", "sr", "mk", "be":
		return charmap.Windows1251
	case "pl", "cs", "sk", "hu", "ro", "hr", "sl", "bs", "sq":
		return charmap.Windows1250
	case "el":
		return charmap.Windows1253
	case "tr", "az":
		return charmap.Windows1254
	case "he":
		return charmap.Windows1255
	case "ar", "fa", "ur":
		return charmap.Windows1256
	case "et", "lv", "lt":
		return charmap.Windows1257
	case "vi":
		return charmap.Windows1258
	case "th":
		return charmap.Windows874
	}
	return charmap.Windows1252
}

// toUTF8 converts subtitle bytes to UTF-8. Handles UTF-8 (with/without BOM),
// UTF-16LE/BE (BOM or NUL-pattern heuristic) and falls back to the
// language's legacy Windows code page (Windows-1252 when unknown), which is
// what most legacy subtitle files actually use.
func toUTF8(b []byte) string { return toUTF8Lang(b, "") }

func toUTF8Lang(b []byte, lang string) string {
	switch {
	case len(b) >= 3 && b[0] == 0xEF && b[1] == 0xBB && b[2] == 0xBF:
		return string(b[3:])
	case len(b) >= 2 && b[0] == 0xFF && b[1] == 0xFE:
		return decodeUTF16(b[2:], true)
	case len(b) >= 2 && b[0] == 0xFE && b[1] == 0xFF:
		return decodeUTF16(b[2:], false)
	}
	// BOM-less UTF-16 heuristic: ASCII text has a NUL in every other byte.
	if len(b) >= 4 {
		le, be := 0, 0
		n := min(len(b), 512)
		for i := 0; i+1 < n; i += 2 {
			if b[i] != 0 && b[i+1] == 0 {
				le++
			}
			if b[i] == 0 && b[i+1] != 0 {
				be++
			}
		}
		if le > 10 && le > be*4 {
			return decodeUTF16(b, true)
		}
		if be > 10 && be > le*4 {
			return decodeUTF16(b, false)
		}
	}
	if utf8.Valid(b) {
		return string(b)
	}
	out, err := legacyCharmap(lang).NewDecoder().Bytes(b)
	if err != nil {
		return strings.ToValidUTF8(string(b), string(utf8.RuneError))
	}
	return string(out)
}

func decodeUTF16(b []byte, littleEndian bool) string {
	u := make([]uint16, 0, len(b)/2)
	for i := 0; i+1 < len(b); i += 2 {
		if littleEndian {
			u = append(u, uint16(b[i])|uint16(b[i+1])<<8)
		} else {
			u = append(u, uint16(b[i])<<8|uint16(b[i+1]))
		}
	}
	return string(utf16.Decode(u))
}

var (
	srtTimeRe = regexp.MustCompile(`(\d{1,2}):(\d{2}):(\d{2})[,.](\d{1,3})\s*-->\s*(\d{1,2}):(\d{2}):(\d{2})[,.](\d{1,3})`)
	vttTimeRe = regexp.MustCompile(`(?:(\d{1,2}):)?(\d{2}):(\d{2})[,.](\d{1,3})\s*-->\s*(?:(\d{1,2}):)?(\d{2}):(\d{2})[,.](\d{1,3})`)
	tagRe     = regexp.MustCompile(`\{\\[^}]*\}`)
	// blankLineRe matches the horizontal whitespace of a whitespace-only line.
	blankLineRe = regexp.MustCompile(`(?m)^[ \t\x{00a0}\x{feff}]+$`)
)

func ts(h, m, s, ms string) time.Duration {
	hi, _ := strconv.Atoi(h)
	mi, _ := strconv.Atoi(m)
	si, _ := strconv.Atoi(s)
	for len(ms) < 3 {
		ms += "0"
	}
	msi, _ := strconv.Atoi(ms[:3])
	return time.Duration(hi)*time.Hour + time.Duration(mi)*time.Minute + time.Duration(si)*time.Second + time.Duration(msi)*time.Millisecond
}

// ParseCues parses SRT or WebVTT text (already UTF-8) into cues.
func ParseCues(text string) ([]Cue, error) {
	text = strings.ReplaceAll(strings.ReplaceAll(text, "\r\n", "\n"), "\r", "\n")
	// A separator line holding only spaces/tabs still ends a cue; without
	// this the next cue's number and timing would be read as body text.
	text = blankLineRe.ReplaceAllString(text, "")
	isVTT := strings.HasPrefix(strings.TrimSpace(text), "WEBVTT")
	blocks := strings.Split(text, "\n\n")
	var cues []Cue
	for _, blk := range blocks {
		lines := strings.Split(strings.Trim(blk, "\n"), "\n")
		timeIdx := -1
		var start, end time.Duration
		for i, ln := range lines {
			if isVTT {
				if m := vttTimeRe.FindStringSubmatch(ln); m != nil {
					start, end, timeIdx = ts(m[1], m[2], m[3], m[4]), ts(m[5], m[6], m[7], m[8]), i
					break
				}
			} else if m := srtTimeRe.FindStringSubmatch(ln); m != nil {
				start, end, timeIdx = ts(m[1], m[2], m[3], m[4]), ts(m[5], m[6], m[7], m[8]), i
				break
			}
		}
		if timeIdx < 0 || timeIdx+1 >= len(lines) {
			continue
		}
		body := strings.TrimSpace(tagRe.ReplaceAllString(strings.Join(lines[timeIdx+1:], "\n"), ""))
		if body == "" || end <= start {
			continue
		}
		cues = append(cues, Cue{Start: start, End: end, Text: body})
	}
	if len(cues) == 0 {
		return nil, fmt.Errorf("no subtitle cues found")
	}
	return cues, nil
}

func formatTS(d time.Duration, sep byte) string {
	if d < 0 {
		d = 0
	}
	h := d / time.Hour
	m := (d % time.Hour) / time.Minute
	s := (d % time.Minute) / time.Second
	ms := (d % time.Second) / time.Millisecond
	return fmt.Sprintf("%02d:%02d:%02d%c%03d", h, m, s, sep, ms)
}

// RenderSRT renders cues as SubRip.
func RenderSRT(cues []Cue) string {
	var sb strings.Builder
	for i, c := range cues {
		fmt.Fprintf(&sb, "%d\n%s --> %s\n%s\n\n", i+1, formatTS(c.Start, ','), formatTS(c.End, ','), c.Text)
	}
	return sb.String()
}

var (
	htmlTagRe = regexp.MustCompile(`</?([a-zA-Z]+)[^>]*>`)
	entityRe  = regexp.MustCompile(`^&(#\d+|#x[0-9a-fA-F]+|[a-zA-Z]+);`)
)

// vttText makes SRT cue text valid WebVTT: only <i>, <b> and <u> survive
// (SRT's <font color> is not VTT), bare "&" and "<" are escaped, and "-->"
// (forbidden in VTT cue text) is replaced.
func vttText(s string) string {
	s = htmlTagRe.ReplaceAllStringFunc(s, func(tag string) string {
		m := htmlTagRe.FindStringSubmatch(tag)
		switch strings.ToLower(m[1]) {
		case "i", "b", "u":
			if strings.HasPrefix(tag, "</") {
				return "\x01/" + strings.ToLower(m[1]) + "\x02"
			}
			return "\x01" + strings.ToLower(m[1]) + "\x02"
		}
		return ""
	})
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '&':
			if entityRe.MatchString(s[i:]) {
				b.WriteByte('&')
			} else {
				b.WriteString("&amp;")
			}
		case '<':
			b.WriteString("&lt;")
		case '>':
			b.WriteString("&gt;")
		default:
			b.WriteByte(c)
		}
	}
	out := strings.NewReplacer("\x01", "<", "\x02", ">", "--&gt;", "→").Replace(b.String())
	return out
}

// RenderVTT renders cues as WebVTT.
func RenderVTT(cues []Cue) string {
	var sb strings.Builder
	sb.WriteString("WEBVTT\n\n")
	for _, c := range cues {
		fmt.Fprintf(&sb, "%s --> %s\n%s\n\n", formatTS(c.Start, '.'), formatTS(c.End, '.'), vttText(c.Text))
	}
	return sb.String()
}

// Normalize converts raw downloaded subtitle bytes to UTF-8 SRT and WebVTT,
// guessing a legacy encoding as Windows-1252.
func Normalize(raw []byte) (srt, vtt string, err error) { return NormalizeLang(raw, "") }

// NormalizeLang is Normalize with the subtitle's language, which picks the
// legacy code page for non-UTF files (Windows-1251 for Russian, …).
func NormalizeLang(raw []byte, lang string) (srt, vtt string, err error) {
	text := toUTF8Lang(raw, lang)
	cues, err := ParseCues(text)
	if err != nil && strings.Contains(strings.ToLower(text), "[events]") {
		cues, err = ParseASS(text)
	}
	if err != nil {
		return "", "", err
	}
	return RenderSRT(cues), RenderVTT(cues), nil
}
