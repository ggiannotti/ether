package cardigann

import (
	"errors"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// parseSize reads sizes such as "1.4 GiB", "700 MB", "1,234.5 KB",
// "1.234,5 MB" or raw byte counts. Like Cardigann, decimal and binary
// units are both treated as powers of 1024. Unparseable input is 0.
func parseSize(s string) int64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	var num strings.Builder
	var unit strings.Builder
	seenDigit := false
	for _, r := range s {
		switch {
		case unicode.IsDigit(r) || r == '.' || r == ',':
			if unit.Len() > 0 {
				// Digits after the unit ("1.5 GB (1610612736 bytes)") end the value.
				goto done
			}
			num.WriteRune(r)
			if unicode.IsDigit(r) {
				seenDigit = true
			}
		case unicode.IsLetter(r):
			if seenDigit {
				unit.WriteRune(unicode.ToLower(r))
			}
		case unicode.IsSpace(r) || r == ' ':
			if unit.Len() > 0 {
				goto done
			}
		}
	}
done:
	val := strings.ReplaceAll(num.String(), ",", ".")
	if strings.Count(val, ".") > 1 {
		last := strings.LastIndexByte(val, '.')
		val = strings.ReplaceAll(val[:last], ".", "") + val[last:]
	}
	// "1.234" with a byte unit or none and exactly three decimals after a
	// single separator is ambiguous; Cardigann reads it as a decimal.
	f, err := strconv.ParseFloat(val, 64)
	if err != nil || f < 0 {
		return 0
	}
	mult := 1.0
	switch u := unit.String(); {
	case strings.HasPrefix(u, "t") || strings.HasPrefix(u, "т"):
		mult = 1 << 40
	case strings.HasPrefix(u, "g") || strings.HasPrefix(u, "г"):
		mult = 1 << 30
	case strings.HasPrefix(u, "m") || strings.HasPrefix(u, "м"):
		mult = 1 << 20
	case strings.HasPrefix(u, "k") || strings.HasPrefix(u, "к"):
		mult = 1 << 10
	}
	v := f * mult
	if v > math.MaxInt64/2 {
		return 0
	}
	return int64(math.Round(v))
}

// coerceInt reads the leading integer of s ("1,234 seeders" → 1234).
func coerceInt(s string) int {
	var b strings.Builder
	started := false
	for _, r := range strings.TrimSpace(s) {
		switch {
		case r >= '0' && r <= '9':
			b.WriteRune(r)
			started = true
		case (r == ',' || r == '.' || r == ' ') && started:
			if r == '.' {
				goto done
			}
		case r == '-' && !started:
			b.WriteRune(r)
		default:
			if started {
				goto done
			}
		}
	}
done:
	n, _ := strconv.Atoi(b.String())
	return n
}

// --- dates ------------------------------------------------------------------

var errDate = errors.New("unrecognised date")

// goLayoutHint reports whether a layout is written in Go reference-time
// notation (which always contains digits) rather than .NET notation.
func goLayoutHint(layout string) bool {
	return strings.ContainsAny(layout, "0123456789")
}

// dotnetLayout translates a .NET custom date format ("yyyy-MM-dd HH:mm")
// into a Go layout.
func dotnetLayout(f string) string {
	var b strings.Builder
	for i := 0; i < len(f); {
		c := f[i]
		if c == '\'' || c == '"' {
			j := strings.IndexByte(f[i+1:], c)
			if j < 0 {
				b.WriteString(f[i+1:])
				break
			}
			b.WriteString(f[i+1 : i+1+j])
			i += j + 2
			continue
		}
		if c == '\\' && i+1 < len(f) {
			b.WriteByte(f[i+1])
			i += 2
			continue
		}
		n := 1
		for i+n < len(f) && f[i+n] == c {
			n++
		}
		tok := f[i : i+n]
		switch c {
		case 'y':
			if n <= 2 {
				b.WriteString("06")
			} else {
				b.WriteString("2006")
			}
		case 'M':
			switch {
			case n >= 4:
				b.WriteString("January")
			case n == 3:
				b.WriteString("Jan")
			case n == 2:
				b.WriteString("01")
			default:
				b.WriteString("1")
			}
		case 'd':
			switch {
			case n >= 4:
				b.WriteString("Monday")
			case n == 3:
				b.WriteString("Mon")
			case n == 2:
				b.WriteString("02")
			default:
				b.WriteString("2")
			}
		case 'H':
			b.WriteString("15")
		case 'h':
			if n == 2 {
				b.WriteString("03")
			} else {
				b.WriteString("3")
			}
		case 'm':
			if n == 2 {
				b.WriteString("04")
			} else {
				b.WriteString("4")
			}
		case 's':
			if n == 2 {
				b.WriteString("05")
			} else {
				b.WriteString("5")
			}
		case 'f', 'F':
			b.WriteString(strings.Repeat("0", n))
		case 't':
			b.WriteString("PM")
		case 'z':
			switch {
			case n >= 3:
				b.WriteString("-07:00")
			default:
				b.WriteString("-07")
			}
		case 'K':
			b.WriteString("Z07:00")
		default:
			b.WriteString(tok)
		}
		i += n
	}
	return b.String()
}

// parseLayout parses value with a Go or .NET layout. Times without a zone
// are taken as UTC.
func parseLayout(value, layout string) (time.Time, error) {
	if !goLayoutHint(layout) {
		layout = dotnetLayout(layout)
	}
	value = strings.TrimSpace(value)
	t, err := time.Parse(layout, value)
	if err != nil {
		// Month and day names are matched case-sensitively by Go; retry
		// with title case ("JAN" → "Jan").
		if t2, err2 := time.Parse(layout, titleWords(value)); err2 == nil {
			return t2, nil
		}
		return time.Time{}, err
	}
	return t, nil
}

func titleWords(s string) string {
	b := []rune(strings.ToLower(s))
	for i := range b {
		if unicode.IsLetter(b[i]) && (i == 0 || !unicode.IsLetter(b[i-1])) {
			b[i] = unicode.ToUpper(b[i])
		}
	}
	return string(b)
}

var relUnitRE = regexp.MustCompile(`(?i)(\d+(?:[.,]\d+)?|an?|one)\s*(seconds?|secs?|s|minutes?|mins?|m|hours?|hrs?|h|days?|d|weeks?|wks?|w|months?|mos?|years?|yrs?|y)\b`)

// timeAgo parses relative times such as "2 hours ago", "1 day, 3 hours"
// or "5 mins". ok is false when nothing relative was found.
func timeAgo(s string, now time.Time) (time.Time, bool) {
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "now" || s == "just now" || strings.HasPrefix(s, "less than a minute") {
		return now, true
	}
	matches := relUnitRE.FindAllStringSubmatch(s, -1)
	if len(matches) == 0 {
		return time.Time{}, false
	}
	t := now
	for _, m := range matches {
		var n float64
		switch m[1] {
		case "a", "an", "one":
			n = 1
		default:
			n, _ = strconv.ParseFloat(strings.ReplaceAll(m[1], ",", "."), 64)
		}
		u := m[2]
		switch {
		case strings.HasPrefix(u, "mo"):
			t = t.AddDate(0, -int(n), 0)
		case strings.HasPrefix(u, "s"):
			t = t.Add(-time.Duration(n * float64(time.Second)))
		case strings.HasPrefix(u, "m"):
			t = t.Add(-time.Duration(n * float64(time.Minute)))
		case strings.HasPrefix(u, "h"):
			t = t.Add(-time.Duration(n * float64(time.Hour)))
		case strings.HasPrefix(u, "d"):
			t = t.Add(-time.Duration(n * 24 * float64(time.Hour)))
		case strings.HasPrefix(u, "w"):
			t = t.Add(-time.Duration(n * 7 * 24 * float64(time.Hour)))
		case strings.HasPrefix(u, "y"):
			t = t.AddDate(-int(n), 0, 0)
		}
	}
	return t, true
}

var commonLayouts = []string{
	time.RFC1123Z, time.RFC1123, time.RFC3339Nano, time.RFC3339, time.RFC822Z, time.RFC822,
	"Mon, 2 Jan 2006 15:04:05 -0700", "Mon, 2 Jan 2006 15:04:05 MST",
	"2006-01-02T15:04:05", "2006-01-02T15:04:05Z0700", "2006-01-02 15:04:05 -0700", "2006-01-02 15:04:05Z07:00",
	"2006-01-02 15:04:05", "2006-01-02 15:04", "2006-01-02",
	"2006/01/02 15:04:05", "2006/01/02 15:04", "2006/01/02", "2006.01.02 15:04", "2006.01.02",
	"01/02/2006 15:04:05", "01/02/2006 15:04", "01/02/2006 3:04:05 PM", "01/02/2006 3:04 PM", "01/02/2006", "1/2/2006",
	"02.01.2006 15:04:05", "02.01.2006 15:04", "02.01.2006", "02-01-2006 15:04", "02-01-2006",
	"Jan 2, 2006 15:04", "Jan 2, 2006 3:04 PM", "Jan 2, 2006", "Jan 02 2006", "January 2, 2006", "January 2 2006",
	"2 Jan 2006 15:04:05", "2 Jan 2006 15:04", "2 Jan 2006", "02 Jan 2006", "2 January 2006", "02-Jan-2006", "02-Jan-06",
	"Mon Jan 2 15:04:05 2006", "Monday, January 2, 2006", "Jan 2 2006 15:04",
	"15:04 02/01/2006", "15:04:05 2006-01-02",
}

var ordinalRE = regexp.MustCompile(`(?i)\b(\d{1,2})(st|nd|rd|th)\b`)
var clockRE = regexp.MustCompile(`(?i)(\d{1,2}):(\d{2})(?::(\d{2}))?\s*(am|pm)?`)

// fuzzyTime parses a date in an unknown format: unix timestamps, relative
// times, "today"/"yesterday" with an optional clock time, and common
// absolute layouts. Times without a zone are UTC.
func fuzzyTime(s string, now time.Time) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, errDate
	}
	lower := strings.ToLower(s)
	if isDigits(s) {
		n, _ := strconv.ParseInt(s, 10, 64)
		switch {
		case len(s) >= 12 && len(s) <= 13:
			return time.UnixMilli(n).UTC(), nil
		case len(s) >= 9 && len(s) <= 10:
			return time.Unix(n, 0).UTC(), nil
		}
	}
	for _, day := range []struct {
		word   string
		offset int
	}{{"today", 0}, {"yesterday", -1}, {"tomorrow", 1}} {
		if strings.Contains(lower, day.word) {
			base := now.UTC().AddDate(0, 0, day.offset)
			t := time.Date(base.Year(), base.Month(), base.Day(), 0, 0, 0, 0, time.UTC)
			if m := clockRE.FindStringSubmatch(lower); m != nil {
				h, _ := strconv.Atoi(m[1])
				mi, _ := strconv.Atoi(m[2])
				sec, _ := strconv.Atoi(m[3])
				if m[4] == "pm" && h < 12 {
					h += 12
				}
				if m[4] == "am" && h == 12 {
					h = 0
				}
				t = t.Add(time.Duration(h)*time.Hour + time.Duration(mi)*time.Minute + time.Duration(sec)*time.Second)
			}
			return t, nil
		}
	}
	if strings.Contains(lower, "ago") || lower == "now" || lower == "just now" {
		if t, ok := timeAgo(lower, now); ok {
			return t, nil
		}
	}
	clean := ordinalRE.ReplaceAllString(s, "$1")
	clean = strings.ReplaceAll(clean, " at ", " ")
	clean = strings.Join(strings.Fields(clean), " ")
	for _, cand := range []string{clean, titleWords(clean)} {
		for _, layout := range commonLayouts {
			if t, err := time.Parse(layout, cand); err == nil {
				return t, nil
			}
		}
	}
	if t, ok := timeAgo(lower, now); ok {
		return t, nil
	}
	return time.Time{}, errDate
}

func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// formatDate is how filters hand dates to later steps and fields.
func formatDate(t time.Time) string { return t.UTC().Format(time.RFC3339) }
