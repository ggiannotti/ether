package yenc

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math/bits"
)

// DefaultMaxSize bounds the buffer a header can make the decoder allocate.
// Usenet parts are typically 384 KiB-1 MiB; 64 MiB leaves ample headroom
// while stopping a hostile header from requesting gigabytes.
const DefaultMaxSize = 64 << 20

// MaxLineSize limits each encoded line, including headers. Normal posts use
// 128 or 256 bytes; this allows unusually long lines without letting malformed
// input allocate an unbounded assembly or scratch buffer.
const MaxLineSize = 1 << 20

// MaxPreambleSize bounds text skipped before the yEnc header.
const MaxPreambleSize = 64 << 10

// LineReader yields one line at a time without its line terminator (LF or
// CRLF). The returned slice is only valid until the next call. At the end of
// input it returns io.EOF. The NNTP client implements it directly over its
// connection buffer so article bodies are decoded without an extra copy.
type LineReader interface {
	ReadLine() ([]byte, error)
}

// DecodeOptions tunes decoding. The zero value is valid.
type DecodeOptions struct {
	// Buffer is reused for the decoded data when it has enough capacity.
	Buffer []byte
	// MaxSize caps the decoded part size; 0 means DefaultMaxSize.
	MaxSize int64
	// SkipCRC disables CRC32 verification (sizes are still checked).
	SkipCRC bool
}

// DecodeLines decodes one yEnc part from lr. Lines before =ybegin are
// skipped. Decoding stops after the =yend line; any remaining lines are left
// unread so the caller can drain its transport.
//
// When the data decodes but fails verification (ErrCRCMismatch or
// ErrSizeMismatch) the returned Part is non-nil and holds the data, so
// callers may choose to use it anyway.
func DecodeLines(lr LineReader, opt *DecodeOptions) (*Part, error) {
	var o DecodeOptions
	if opt != nil {
		o = *opt
	}
	if o.MaxSize <= 0 {
		o.MaxSize = DefaultMaxSize
	}

	// A slice cannot exceed the platform's addressable size, even when a
	// caller supplies a larger int64 limit on a 32-bit target.
	o.MaxSize = min(o.MaxSize, int64(int(^uint(0)>>1)))
	p := &Part{}
	preamble := 0
	// 1. Find =ybegin.
	for {
		line, err := lr.ReadLine()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, ErrNoHeader
			}
			return nil, err
		}
		if len(line) > MaxLineSize {
			return nil, fmt.Errorf("%w: encoded line exceeds %d bytes", ErrTooLarge, MaxLineSize)
		}
		if bytes.HasPrefix(line, keyBeginBytes) {
			h, err := ParseBegin(line)
			if err != nil {
				return nil, err
			}
			p.Header = h
			break
		}
		preamble += len(line) + 2
		if preamble > MaxPreambleSize {
			return nil, fmt.Errorf("%w: preamble exceeds %d bytes", ErrTooLarge, MaxPreambleSize)
		}
	}

	// 2. Optional =ypart, then data lines up to =yend.
	first := true
	escaped := false
	var dst []byte
	var smallScratch [512]byte
	scratch := smallScratch[:]
	// Count wire bytes as well as decoded bytes: empty lines and redundant
	// escape line breaks must not permit unlimited work for a tiny part.
	// Six wire bytes permit every escaped byte to be split across CRLF lines.
	wireBudget := o.MaxSize
	if wireBudget > (int64(^uint64(0)>>1)-MaxPreambleSize)/6 {
		wireBudget = int64(^uint64(0) >> 1)
	} else {
		wireBudget = wireBudget*6 + MaxPreambleSize
	}
	alloc := func(hint int64) error {
		if hint > o.MaxSize {
			return fmt.Errorf("%w: %d bytes", ErrTooLarge, hint)
		}
		if hint <= 0 {
			hint = min(64<<10, o.MaxSize)
		}
		if int64(cap(o.Buffer)) >= hint {
			dst = o.Buffer[:0:min(int64(cap(o.Buffer)), o.MaxSize)]
		} else {
			dst = make([]byte, 0, hint)
		}
		return nil
	}
	for {
		line, err := lr.ReadLine()
		if err != nil {
			if errors.Is(err, io.EOF) {
				if dst != nil {
					p.Data = dst
				}
				return p, ErrTruncated
			}
			return nil, err
		}
		if len(line) > MaxLineSize {
			return nil, fmt.Errorf("%w: encoded line exceeds %d bytes", ErrTooLarge, MaxLineSize)
		}
		wireBudget -= int64(len(line)) + 2
		if wireBudget < 0 {
			return nil, fmt.Errorf("%w: encoded body exceeds budget", ErrTooLarge)
		}
		if first {
			first = false
			if bytes.HasPrefix(line, keyPartBytes) {
				if err := ParsePart(line, &p.Header); err != nil {
					return nil, err
				}
				if err := alloc(p.Header.PartSize()); err != nil {
					return nil, err
				}
				continue
			}
			if err := alloc(p.Header.PartSize()); err != nil {
				return nil, err
			}
		}
		if len(line) >= len(keyEnd) && line[0] == '=' && line[1] == 'y' && bytes.HasPrefix(line, keyEndBytes) {
			if escaped {
				p.Data = dst
				return p, fmt.Errorf("%w: dangling escape before =yend", ErrMalformed)
			}
			t, err := ParseEnd(line)
			if err != nil {
				p.Data = dst
				return p, err
			}
			p.Trailer = t
			p.Data = dst
			return p, verify(p, o.SkipCRC)
		}
		if len(line) == 0 {
			continue
		}
		if cap(dst)-len(dst) >= len(line) {
			// Fast path: decode straight into the output (decoded length
			// never exceeds encoded length).
			n := len(dst)
			var w int
			w, escaped = DecodeLine(dst[n:n+len(line)], line, escaped)
			dst = dst[:n+w]
		} else {
			// Near the end of the announced size, or the size was wrong:
			// decode via scratch so an exact-size buffer is not doubled.
			if cap(scratch) < len(line) {
				scratch = make([]byte, len(line))
			}
			var w int
			w, escaped = DecodeLine(scratch[:len(line)], line, escaped)
			if int64(w) > o.MaxSize-int64(len(dst)) {
				return nil, fmt.Errorf("%w: more than %d bytes", ErrTooLarge, o.MaxSize)
			}
			if w > cap(dst)-len(dst) {
				current := int64(cap(dst))
				grownCapacity := current + min(current, o.MaxSize-current)
				capacity := max(int64(len(dst)+w), min(o.MaxSize, max(grownCapacity, 512)))
				grown := make([]byte, len(dst), int(capacity))
				copy(grown, dst)
				dst = grown
			}
			dst = append(dst, scratch[:w]...)
		}
	}
}

func verify(p *Part, skipCRC bool) error {
	n := int64(len(p.Data))
	if p.Trailer.Size != n {
		return fmt.Errorf("%w: decoded %d bytes, =yend size=%d", ErrSizeMismatch, n, p.Trailer.Size)
	}
	if want := p.Header.PartSize(); want != n {
		return fmt.Errorf("%w: decoded %d bytes, header announces %d", ErrSizeMismatch, n, want)
	}
	if skipCRC {
		return nil
	}
	whole := !p.Header.Multipart || (p.Header.Begin == 1 && p.Header.End == p.Header.Size)
	if p.Trailer.HasPartCRC || (p.Trailer.HasCRC && whole) {
		got := crc32.ChecksumIEEE(p.Data)
		if p.Trailer.HasPartCRC && got != p.Trailer.PartCRC32 {
			return fmt.Errorf("%w: pcrc32 got %08x want %08x", ErrCRCMismatch, got, p.Trailer.PartCRC32)
		}
		if p.Trailer.HasCRC && whole && got != p.Trailer.CRC32 {
			return fmt.Errorf("%w: crc32 got %08x want %08x", ErrCRCMismatch, got, p.Trailer.CRC32)
		}
	}
	return nil
}

// DecodeLine decodes one encoded line (without line terminator) into dst and
// returns the number of bytes written. dst must have room for len(src)
// bytes. escaped carries a dangling '=' from the end of the previous line,
// and the returned flag reports a dangling '=' at the end of this one.
//
// This is the hot loop. Each native word both detects '=' and subtracts 42
// from eight independent byte lanes. Fusing detection with decoding avoids
// scanning each escaped run twice and calling a search routine per escape.
// The first set lane in the zero-byte mask is the first real escape even
// when subtraction borrows into a following lane. The short tail is scalar.
// No allocation, lookup table, unsafe access or architecture-specific ISA
// requirement is involved. Exact in-place decoding is supported.
func DecodeLine(dst, src []byte, escaped bool) (int, bool) {
	const (
		high   = uint64(0x8080808080808080)
		ones   = uint64(0x0101010101010101)
		offset = uint64(0x2a2a2a2a2a2a2a2a)
	)
	n := 0
	if escaped && len(src) > 0 {
		dst[0] = src[0] - 106
		n = 1
		src = src[1:]
		escaped = false
	}
	for len(src) >= 8 {
		word := binary.LittleEndian.Uint64(src)
		eq := word ^ uint64(0x3d3d3d3d3d3d3d3d)
		mask := (eq - ones) &^ eq & high
		if mask == 0 {
			// Setting each high bit prevents a borrow into the next byte;
			// restore the original high-bit effect after subtraction.
			binary.LittleEndian.PutUint64(dst[n:], ((word|high)-offset)^(^word&high))
			src = src[8:]
			n += 8
			continue
		}
		prefix := bits.TrailingZeros64(mask) / 8
		for i := 0; i < prefix; i++ {
			dst[n+i] = src[i] - 42
		}
		n += prefix
		src = src[prefix+1:]
		if len(src) == 0 {
			return n, true
		}
		dst[n] = src[0] - 106
		n++
		src = src[1:]
	}
	for _, c := range src {
		if escaped {
			dst[n] = c - 106
			n++
			escaped = false
		} else if c == '=' {
			escaped = true
		} else {
			dst[n] = c - 42
			n++
		}
	}
	return n, escaped
}

// Decode decodes a whole encoded article body held in memory.
func Decode(encoded []byte) (*Part, error) {
	return DecodeLines(&bytesLines{b: encoded}, nil)
}

// DecodeReader decodes one part from r.
func DecodeReader(r io.Reader, opt *DecodeOptions) (*Part, error) {
	return DecodeLines(NewLineReader(r), opt)
}

// bytesLines is a LineReader over an in-memory buffer.
type bytesLines struct{ b []byte }

func (l *bytesLines) ReadLine() ([]byte, error) {
	if len(l.b) == 0 {
		return nil, io.EOF
	}
	i := bytes.IndexByte(l.b, '\n')
	var line []byte
	if i < 0 {
		line, l.b = l.b, nil
	} else {
		line, l.b = l.b[:i], l.b[i+1:]
	}
	return trimCR(line), nil
}

func trimCR(b []byte) []byte {
	if n := len(b); n > 0 && b[n-1] == '\r' {
		return b[:n-1]
	}
	return b
}

// NewLineReader adapts an io.Reader to a LineReader. Lines longer than the
// internal buffer are assembled in a side buffer up to MaxLineSize bytes.
// An oversized line returns ErrTooLarge; the reader must then be discarded.
func NewLineReader(r io.Reader) LineReader {
	if br, ok := r.(*bufio.Reader); ok {
		return &readerLines{r: br}
	}
	return &readerLines{r: bufio.NewReaderSize(r, 32<<10)}
}

type readerLines struct {
	r    *bufio.Reader
	long []byte
}

func (l *readerLines) ReadLine() ([]byte, error) {
	line, err := l.r.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		if len(line) > MaxLineSize {
			return nil, fmt.Errorf("%w: encoded line exceeds %d bytes", ErrTooLarge, MaxLineSize)
		}
		l.long = append(l.long[:0], line...)
		for err == bufio.ErrBufferFull {
			line, err = l.r.ReadSlice('\n')
			if len(line) > MaxLineSize+2-len(l.long) {
				return nil, fmt.Errorf("%w: encoded line exceeds %d bytes", ErrTooLarge, MaxLineSize)
			}
			l.long = append(l.long, line...)
		}
		line = l.long
	}
	if len(trimCR(bytes.TrimSuffix(line, []byte{'\n'}))) > MaxLineSize {
		return nil, fmt.Errorf("%w: encoded line exceeds %d bytes", ErrTooLarge, MaxLineSize)
	}
	if err != nil {
		if err == io.EOF && len(line) > 0 {
			return trimCR(line), nil
		}
		return nil, err
	}
	return trimCR(line[:len(line)-1]), nil
}
