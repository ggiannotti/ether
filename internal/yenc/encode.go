package yenc

import (
	"fmt"
	"hash/crc32"
	"io"
	"strconv"
)

// DefaultLineLength is the conventional encoded line length.
const DefaultLineLength = 128

// EncodeOptions describes the part being encoded.
type EncodeOptions struct {
	Name       string
	LineLength int // encoded characters per line; 0 = DefaultLineLength
	// Part > 0 produces a multipart post with a =ypart line. Total is the
	// number of parts, Begin the 1-based offset of data in the file and
	// FileSize the size of the whole file.
	Part     int
	Total    int
	Begin    int64
	FileSize int64
	// FileCRC, when HasFileCRC is set, is written as crc32= in the =yend of
	// a multipart post (the CRC of the whole file).
	FileCRC    uint32
	HasFileCRC bool
}

// escape classes for encoded output characters.
const (
	escAlways = 1 // NUL, LF, CR, '='
	escEdge   = 2 // TAB, SPACE: escape at line start or end
	escStart  = 3 // '.': escape at line start
)

var escTable = func() (t [256]uint8) {
	t[0], t['\n'], t['\r'], t['='] = escAlways, escAlways, escAlways, escAlways
	t['\t'], t[' '] = escEdge, escEdge
	t['.'] = escStart
	return t
}()

// AppendEncode appends the yEnc encoding of data (header, body lines and
// trailer, CRLF-terminated) to dst.
func AppendEncode(dst, data []byte, o EncodeOptions) ([]byte, error) {
	if o.LineLength <= 0 {
		o.LineLength = DefaultLineLength
	}
	if o.Name == "" {
		return dst, fmt.Errorf("yenc: encode: empty name")
	}
	multipart := o.Part > 0
	if multipart {
		if o.Begin <= 0 {
			o.Begin = 1
		}
		if o.FileSize <= 0 {
			o.FileSize = o.Begin - 1 + int64(len(data))
		}
		if o.Begin-1+int64(len(data)) > o.FileSize {
			return dst, fmt.Errorf("yenc: encode: part %d..%d exceeds file size %d", o.Begin, o.Begin-1+int64(len(data)), o.FileSize)
		}
	} else {
		o.FileSize = int64(len(data))
	}

	// Header.
	dst = append(dst, "=ybegin "...)
	if multipart {
		dst = append(dst, "part="...)
		dst = strconv.AppendInt(dst, int64(o.Part), 10)
		if o.Total > 0 {
			dst = append(dst, " total="...)
			dst = strconv.AppendInt(dst, int64(o.Total), 10)
		}
		dst = append(dst, ' ')
	}
	dst = append(dst, "line="...)
	dst = strconv.AppendInt(dst, int64(o.LineLength), 10)
	dst = append(dst, " size="...)
	dst = strconv.AppendInt(dst, o.FileSize, 10)
	dst = append(dst, " name="...)
	dst = append(dst, o.Name...)
	dst = append(dst, "\r\n"...)
	if multipart {
		dst = append(dst, "=ypart begin="...)
		dst = strconv.AppendInt(dst, o.Begin, 10)
		dst = append(dst, " end="...)
		dst = strconv.AppendInt(dst, o.Begin-1+int64(len(data)), 10)
		dst = append(dst, "\r\n"...)
	}

	// Body. Worst case doubles the size, plus CRLFs.
	if need := len(dst) + 2*len(data) + 2*(len(data)/o.LineLength+1) + 96; cap(dst) < need {
		grown := make([]byte, len(dst), need)
		copy(grown, dst)
		dst = grown
	}
	col := 0
	last := len(data) - 1
	ll := o.LineLength
	for i, b := range data {
		c := b + 42
		esc := false
		switch escTable[c] {
		case escAlways:
			esc = true
		case escEdge:
			esc = col == 0 || col >= ll-1 || i == last
		case escStart:
			esc = col == 0
		}
		if esc {
			dst = append(dst, '=', c+64)
			col += 2
		} else {
			dst = append(dst, c)
			col++
		}
		if col >= ll {
			dst = append(dst, '\r', '\n')
			col = 0
		}
	}
	if col > 0 {
		dst = append(dst, '\r', '\n')
	}

	// Trailer.
	crc := crc32.ChecksumIEEE(data)
	dst = append(dst, "=yend size="...)
	dst = strconv.AppendInt(dst, int64(len(data)), 10)
	if multipart {
		dst = append(dst, " part="...)
		dst = strconv.AppendInt(dst, int64(o.Part), 10)
		dst = append(dst, " pcrc32="...)
		dst = appendHex32(dst, crc)
		if o.HasFileCRC {
			dst = append(dst, " crc32="...)
			dst = appendHex32(dst, o.FileCRC)
		}
	} else {
		dst = append(dst, " crc32="...)
		dst = appendHex32(dst, crc)
	}
	dst = append(dst, "\r\n"...)
	return dst, nil
}

func appendHex32(dst []byte, v uint32) []byte {
	const hex = "0123456789abcdef"
	for s := 28; s >= 0; s -= 4 {
		dst = append(dst, hex[(v>>uint(s))&0xf])
	}
	return dst
}

// EncodeBytes returns the yEnc encoding of data.
func EncodeBytes(data []byte, o EncodeOptions) ([]byte, error) {
	return AppendEncode(nil, data, o)
}

// Encode writes the yEnc encoding of data to w.
func Encode(w io.Writer, data []byte, o EncodeOptions) error {
	b, err := AppendEncode(nil, data, o)
	if err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}
