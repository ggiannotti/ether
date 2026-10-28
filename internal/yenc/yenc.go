// Package yenc implements the yEnc binary encoding used for Usenet binary
// posts (http://www.yenc.org/yenc-draft.1.3.txt): a fast streaming decoder
// with =ybegin/=ypart/=yend parsing and CRC32 verification, and an encoder
// used by tests and the fake NNTP server.
//
// yEnc maps every byte b to (b+42) mod 256. The "critical" output characters
// NUL, LF, CR and '=' are escaped as '=' followed by (b+42+64) mod 256;
// encoders also escape TAB and SPACE at line edges and '.' at the start of a
// line (to survive NNTP dot-stuffing done badly by some servers).
package yenc

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
)

// Errors returned by the decoder. Decode errors are wrapped with context; use
// errors.Is to test for them.
var (
	// ErrNoHeader means no =ybegin line was found: the article is not yEnc.
	ErrNoHeader = errors.New("yenc: no =ybegin header")
	// ErrTruncated means the input ended before the =yend trailer.
	ErrTruncated = errors.New("yenc: data truncated (no =yend)")
	// ErrCRCMismatch means a pcrc32 or crc32 check failed.
	ErrCRCMismatch = errors.New("yenc: crc32 mismatch")
	// ErrSizeMismatch means the decoded length disagrees with the headers.
	ErrSizeMismatch = errors.New("yenc: size mismatch")
	// ErrMalformed covers unparseable keyword lines.
	ErrMalformed = errors.New("yenc: malformed header")
	// ErrTooLarge means the headers announce a part larger than the limit.
	ErrTooLarge = errors.New("yenc: part exceeds size limit")
)

// Header holds the =ybegin and (for multipart posts) =ypart fields.
type Header struct {
	Name  string // file name
	Line  int    // nominal encoded line length
	Size  int64  // size of the whole file
	Part  int    // 1-based part number, 0 for single-part posts
	Total int    // number of parts, 0 if not given
	// Begin and End are the 1-based inclusive byte offsets of this part in
	// the file. For single-part posts Begin is 1 and End is Size.
	Begin int64
	End   int64
	// Multipart is true when the post carried a =ypart line.
	Multipart bool
}

// PartSize is the decoded size of this part announced by the header.
func (h *Header) PartSize() int64 {
	if h.End >= h.Begin && h.Begin > 0 {
		return h.End - h.Begin + 1
	}
	return h.Size
}

// Trailer holds the =yend fields.
type Trailer struct {
	Size       int64 // decoded size of this part
	Part       int
	PartCRC32  uint32 // pcrc32: CRC of this part
	CRC32      uint32 // crc32: CRC of the whole file
	HasPartCRC bool
	HasCRC     bool
}

// Part is one decoded article.
type Part struct {
	Header  Header
	Trailer Trailer
	Data    []byte
}

const (
	keyBegin = "=ybegin "
	keyPart  = "=ypart "
	keyEnd   = "=yend"
)

var (
	keyBeginBytes = []byte(keyBegin)
	keyPartBytes  = []byte(keyPart)
	keyEndBytes   = []byte(keyEnd)
)

// parseFields splits "k1=v1 k2=v2 name=rest of line" into a map. If nameLast
// is true, "name=" swallows the rest of the line (file names may contain
// spaces and '=').
func parseFields(line []byte, nameLast bool) (map[string]string, string, bool) {
	fields := make(map[string]string, 6)
	name, hasName := "", false
	if nameLast {
		if i := bytes.Index(line, []byte(" name=")); i >= 0 {
			name = string(bytes.TrimRight(line[i+len(" name="):], " \t\r"))
			hasName = true
			line = line[:i]
		} else if bytes.HasPrefix(line, []byte("name=")) {
			name = string(bytes.TrimRight(line[len("name="):], " \t\r"))
			hasName = true
			line = nil
		}
	}
	for _, tok := range bytes.Fields(line) {
		k, v, ok := bytes.Cut(tok, []byte("="))
		if !ok {
			continue
		}
		fields[string(bytes.ToLower(k))] = string(v)
	}
	return fields, name, hasName
}

func fieldInt(f map[string]string, k string) (int64, bool, error) {
	v, ok := f[k]
	if !ok {
		return 0, false, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		return 0, true, fmt.Errorf("%w: %s=%q", ErrMalformed, k, v)
	}
	return n, true, nil
}

func fieldCRC(f map[string]string, k string) (uint32, bool, error) {
	v, ok := f[k]
	if !ok {
		return 0, false, nil
	}
	// Some posters zero-pad, some don't, some prefix 0x.
	if len(v) > 2 && (v[:2] == "0x" || v[:2] == "0X") {
		v = v[2:]
	}
	n, err := strconv.ParseUint(v, 16, 32)
	if err != nil {
		return 0, true, fmt.Errorf("%w: %s=%q", ErrMalformed, k, v)
	}
	return uint32(n), true, nil
}

// ParseBegin parses a "=ybegin ..." line.
func ParseBegin(line []byte) (Header, error) {
	var h Header
	if !bytes.HasPrefix(line, keyBeginBytes) {
		return h, fmt.Errorf("%w: not a =ybegin line", ErrMalformed)
	}
	f, name, hasName := parseFields(line[len(keyBegin):], true)
	if !hasName {
		return h, fmt.Errorf("%w: =ybegin without name", ErrMalformed)
	}
	h.Name = name
	size, ok, err := fieldInt(f, "size")
	if err != nil {
		return h, err
	}
	if !ok {
		return h, fmt.Errorf("%w: =ybegin without size", ErrMalformed)
	}
	h.Size = size
	if v, _, err := fieldInt(f, "line"); err != nil {
		return h, err
	} else {
		h.Line = int(v)
	}
	if v, _, err := fieldInt(f, "part"); err != nil {
		return h, err
	} else {
		h.Part = int(v)
	}
	if v, _, err := fieldInt(f, "total"); err != nil {
		return h, err
	} else {
		h.Total = int(v)
	}
	h.Begin, h.End = 1, h.Size
	return h, nil
}

// ParsePart parses a "=ypart ..." line into h.
func ParsePart(line []byte, h *Header) error {
	if !bytes.HasPrefix(line, keyPartBytes) {
		return fmt.Errorf("%w: not a =ypart line", ErrMalformed)
	}
	f, _, _ := parseFields(line[len(keyPart):], false)
	begin, okB, err := fieldInt(f, "begin")
	if err != nil {
		return err
	}
	end, okE, err := fieldInt(f, "end")
	if err != nil {
		return err
	}
	if !okB || !okE || begin < 1 || end < begin {
		return fmt.Errorf("%w: =ypart begin=%d end=%d", ErrMalformed, begin, end)
	}
	if h.Size > 0 && end > h.Size {
		return fmt.Errorf("%w: =ypart end %d beyond size %d", ErrMalformed, end, h.Size)
	}
	h.Begin, h.End, h.Multipart = begin, end, true
	return nil
}

// ParseEnd parses a "=yend ..." line.
func ParseEnd(line []byte) (Trailer, error) {
	var t Trailer
	if !bytes.HasPrefix(line, keyEndBytes) {
		return t, fmt.Errorf("%w: not a =yend line", ErrMalformed)
	}
	f, _, _ := parseFields(line[len(keyEnd):], false)
	size, ok, err := fieldInt(f, "size")
	if err != nil {
		return t, err
	}
	if !ok {
		return t, fmt.Errorf("%w: =yend without size", ErrMalformed)
	}
	t.Size = size
	if v, _, err := fieldInt(f, "part"); err != nil {
		return t, err
	} else {
		t.Part = int(v)
	}
	if t.PartCRC32, t.HasPartCRC, err = fieldCRC(f, "pcrc32"); err != nil {
		return t, err
	}
	if t.CRC32, t.HasCRC, err = fieldCRC(f, "crc32"); err != nil {
		return t, err
	}
	return t, nil
}
