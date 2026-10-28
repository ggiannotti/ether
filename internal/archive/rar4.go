package archive

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

// RAR 4.x block types.
const (
	rar4Main   = 0x73
	rar4File   = 0x74
	rar4NewSub = 0x7a
	rar4End    = 0x7b
)

// RAR 4.x flags.
const (
	rar4LongBlock = 0x8000

	rar4MainVolume       = 0x0001
	rar4MainSolid        = 0x0008
	rar4MainNewNumbering = 0x0010
	rar4MainPassword     = 0x0080
	rar4MainFirstVolume  = 0x0100

	rar4FileSplitBefore = 0x0001
	rar4FileSplitAfter  = 0x0002
	rar4FilePassword    = 0x0004
	rar4FileSolid       = 0x0010
	rar4FileDirMask     = 0x00e0
	rar4FileLarge       = 0x0100
	rar4FileUnicode     = 0x0200

	rar4EndNextVolume = 0x0001
	rar4EndDataCRC    = 0x0002
	rar4EndVolNumber  = 0x0008

	rar4BaseLen     = 7
	rar4FileFixed   = rar4BaseLen + 25
	rar4MethodStore = 0x30
)

func readRAR4(r io.ReaderAt, size int64, o RAROptions) (*RARVolume, error) {
	v := &RARVolume{Format: FormatRAR4, Number: -1}
	pos := int64(len(sigRAR4))
	var base [rar4BaseLen]byte
	mainSeen := false
	headersEncrypted := false
	crypto := newRARCrypto(o)
	numberKnown := false
	for pos < size {
		if err := crypto.ctx.Err(); err != nil {
			return nil, err
		}
		var h []byte
		var headerBytes int64
		if headersEncrypted {
			salt := make([]byte, 8)
			if err := readAt(r, salt, pos, size); err != nil {
				return nil, err
			}
			keys, err := crypto.rar4(salt)
			if err != nil {
				return nil, err
			}
			var padded int64
			h, padded, err = decryptRARHeader(r, size, pos+8, keys.key, keys.iv, false)
			if err != nil {
				return nil, err
			}
			headerBytes = 8 + padded
		} else {
			if err := readAt(r, base[:], pos, size); err != nil {
				return nil, err
			}
			length := int64(binary.LittleEndian.Uint16(base[5:]))
			if length < rar4BaseLen || length > size-pos {
				return nil, fmt.Errorf("archive: invalid RAR4 header size: %w", ErrCorrupt)
			}
			h = make([]byte, length)
			copy(h, base[:])
			if err := readAt(r, h[rar4BaseLen:], pos+rar4BaseLen, size); err != nil {
				return nil, err
			}
			headerBytes = length
		}
		typ := h[2]
		flags := binary.LittleEndian.Uint16(h[3:])
		hsize := int64(len(h))
		if !mainSeen && typ != rar4Main {
			return nil, fmt.Errorf("archive: rar4 block at %d: type %#x before main header: %w", pos, typ, ErrCorrupt)
		}
		switch typ {
		case rar4Main, rar4File, rar4NewSub, rar4End:
			if got, want := uint16(crc32.ChecksumIEEE(h[2:])), binary.LittleEndian.Uint16(h); got != want {
				if headersEncrypted {
					return nil, fmt.Errorf("archive: encrypted RAR4 header CRC mismatch: %w: %w", ErrBadPassword, ErrCorrupt)
				}
				return nil, fmt.Errorf("archive: rar4 block at %d: header crc %#04x, want %#04x: %w", pos, got, want, ErrCorrupt)
			}
		}
		var addSize int64
		if flags&rar4LongBlock != 0 {
			if hsize < rar4BaseLen+4 {
				return nil, fmt.Errorf("archive: rar4 block at %d: long block header too short: %w", pos, ErrCorrupt)
			}
			addSize = int64(binary.LittleEndian.Uint32(h[7:]))
		}
		next := pos + headerBytes
		switch typ {
		case rar4Main:
			if mainSeen {
				break
			}
			mainSeen = true
			if flags&rar4MainPassword != 0 {
				if o.Password == "" {
					return nil, ErrPasswordRequired
				}
				headersEncrypted = true
			}
			v.MultiVolume = flags&rar4MainVolume != 0
			v.First = flags&rar4MainFirstVolume != 0 || !v.MultiVolume
			v.Solid = flags&rar4MainSolid != 0
			v.NewNumbering = flags&rar4MainNewNumbering != 0
			v.DataStart = next
		case rar4File, rar4NewSub:
			f, dataSize, err := parseRAR4File(h, flags)
			if err != nil {
				return nil, fmt.Errorf("archive: rar4 file header at %d: %w", pos, err)
			}
			if typ == rar4File && f.Encrypted && o.Password != "" {
				if flags&0x400 == 0 || h[24] < 29 {
					return nil, fmt.Errorf("archive: legacy RAR encryption unsupported: %w", ErrEncrypted)
				}
				saltOff := rar4FileFixed + int(binary.LittleEndian.Uint16(h[26:]))
				if flags&rar4FileLarge != 0 {
					saltOff += 8
				}
				if saltOff > len(h)-8 {
					return nil, fmt.Errorf("archive: RAR4 salt truncated: %w", ErrCorrupt)
				}
				keys, err := crypto.rar4(h[saltOff : saltOff+8])
				if err != nil {
					return nil, err
				}
				f.Encryption = &Encryption{Key: keys.key, IV: keys.iv, Size: f.UnpackedSize}
			}
			f.HeaderOffset = pos
			f.DataOffset = next
			if dataSize > size-next {
				return nil, fmt.Errorf("archive: rar4 file %q data (%d bytes at %d) runs past end of volume: %w",
					f.Name, dataSize, next, ErrCorrupt)
			}
			next += dataSize
			if typ == rar4NewSub {
				break
			}
			v.Files = append(v.Files, f)
			if f.SplitAfter || (o.MaxFiles > 0 && len(v.Files) >= o.MaxFiles) {
				return finishRAR4(v, numberKnown), nil
			}
		case rar4End:
			v.EndFound = true
			v.MoreVolumes = flags&rar4EndNextVolume != 0
			off := rar4BaseLen
			if flags&rar4EndDataCRC != 0 {
				off += 4
			}
			if flags&rar4EndVolNumber != 0 && int(hsize) >= off+2 {
				v.Number = int(binary.LittleEndian.Uint16(h[off:]))
				numberKnown = true
			}
			return finishRAR4(v, numberKnown), nil
		default:
			// Marker, comment, AV, old sub-block, protect, sign or unknown
			// blocks: skip them along with any data that follows.
			if addSize > size-next {
				return nil, fmt.Errorf("archive: rar4 block at %d (type %#x) runs past end of volume: %w", pos, typ, ErrCorrupt)
			}
			next += addSize
		}
		pos = next
	}
	if !mainSeen {
		return nil, fmt.Errorf("archive: rar4 main header missing: %w", ErrCorrupt)
	}
	return finishRAR4(v, numberKnown), nil
}

func finishRAR4(v *RARVolume, numberKnown bool) *RARVolume {
	if !numberKnown {
		if v.First {
			v.Number = 0
		} else {
			v.Number = -1
		}
	}
	return v
}

// parseRAR4File decodes a file (0x74) or new sub-block (0x7a) header and
// returns it with the size of the data that follows it.
func parseRAR4File(h []byte, flags uint16) (RARFile, int64, error) {
	var f RARFile
	if len(h) < rar4FileFixed {
		return f, 0, fmt.Errorf("header too short (%d bytes): %w", len(h), ErrCorrupt)
	}
	packLow := binary.LittleEndian.Uint32(h[7:])
	unpLow := binary.LittleEndian.Uint32(h[11:])
	f.CRC32 = binary.LittleEndian.Uint32(h[16:])
	f.HasCRC = true
	f.Method = int(h[25])
	nameSize := int(binary.LittleEndian.Uint16(h[26:]))
	p := rar4FileFixed
	var packHigh, unpHigh uint32
	if flags&rar4FileLarge != 0 {
		if len(h) < p+8 {
			return f, 0, fmt.Errorf("large header too short: %w", ErrCorrupt)
		}
		packHigh = binary.LittleEndian.Uint32(h[p:])
		unpHigh = binary.LittleEndian.Uint32(h[p+4:])
		p += 8
		if unpHigh == 0xffffffff && unpLow == 0xffffffff {
			f.UnpackedSize = -1
		}
	} else if unpLow == 0xffffffff {
		f.UnpackedSize = -1
	}
	if f.UnpackedSize == 0 {
		f.UnpackedSize = int64(unpHigh)<<32 | int64(unpLow)
	}
	pack := int64(packHigh)<<32 | int64(packLow)
	if pack < 0 || f.UnpackedSize < -1 {
		return f, 0, fmt.Errorf("size overflows int64: %w", ErrCorrupt)
	}
	f.PackedSize = pack
	if nameSize > len(h)-p {
		return f, 0, fmt.Errorf("name (%d bytes) overflows header: %w", nameSize, ErrCorrupt)
	}
	f.Name = decodeRAR4Name(h[p:p+nameSize], flags&rar4FileUnicode != 0)
	f.Stored = f.Method == rar4MethodStore
	f.Encrypted = flags&rar4FilePassword != 0
	f.Solid = flags&rar4FileSolid != 0
	f.Dir = flags&rar4FileDirMask == rar4FileDirMask
	f.SplitBefore = flags&rar4FileSplitBefore != 0
	f.SplitAfter = flags&rar4FileSplitAfter != 0
	return f, pack, nil
}

// decodeRAR4Name decodes a RAR 4.x file name field. With the unicode flag the
// field is either plain UTF-8 (no NUL) or an OEM name, a NUL and RAR's
// compact UTF-16 encoding relative to that name.
func decodeRAR4Name(field []byte, unicode bool) string {
	var name string
	nul := -1
	for i, c := range field {
		if c == 0 {
			nul = i
			break
		}
	}
	switch {
	case unicode && nul < 0:
		name = string(field)
	case unicode:
		name = decodeRAR4Unicode(field, field[nul+1:])
		if name == "" {
			name = bytesToString(field[:nul])
		}
	default:
		if nul >= 0 {
			field = field[:nul]
		}
		name = bytesToString(field)
	}
	return strings.ReplaceAll(name, `\`, "/")
}

// bytesToString keeps valid UTF-8 and otherwise maps each byte to the rune
// of the same value (Latin-1), which is the best that can be done without a
// code page table.
func bytesToString(b []byte) string {
	if utf8.Valid(b) {
		return string(b)
	}
	rs := make([]rune, len(b))
	for i, c := range b {
		rs[i] = rune(c)
	}
	return string(rs)
}

// decodeRAR4Unicode implements RAR's EncodeFileName::Decode: name is the
// whole name field (the OEM name the encoding refers back to) and enc the
// bytes following its NUL terminator.
func decodeRAR4Unicode(name, enc []byte) string {
	const maxLen = 1 << 12
	at := func(i int) uint16 {
		if i < len(name) {
			return uint16(name[i])
		}
		return 0
	}
	out := make([]uint16, 0, len(name))
	pos := 0
	if len(enc) == 0 {
		return ""
	}
	high := uint16(enc[pos])
	pos++
	var flags byte
	flagBits := 0
	for pos < len(enc) && len(out) < maxLen {
		if flagBits == 0 {
			flags = enc[pos]
			pos++
			flagBits = 8
		}
		switch flags >> 6 {
		case 0:
			if pos >= len(enc) {
				break
			}
			out = append(out, uint16(enc[pos]))
			pos++
		case 1:
			if pos >= len(enc) {
				break
			}
			out = append(out, uint16(enc[pos])+high<<8)
			pos++
		case 2:
			if pos+1 >= len(enc) {
				break
			}
			out = append(out, uint16(enc[pos])|uint16(enc[pos+1])<<8)
			pos += 2
		case 3:
			if pos >= len(enc) {
				break
			}
			length := int(enc[pos])
			pos++
			if length&0x80 != 0 {
				if pos >= len(enc) {
					break
				}
				corr := uint16(enc[pos])
				pos++
				for n := (length & 0x7f) + 2; n > 0 && len(out) < maxLen; n-- {
					out = append(out, (at(len(out))+corr)&0xff+high<<8)
				}
			} else {
				for n := length + 2; n > 0 && len(out) < maxLen; n-- {
					out = append(out, at(len(out)))
				}
			}
		}
		flags <<= 2
		flagBits -= 2
	}
	// The decoded name is NUL-terminated if shorter than the buffer.
	for i, c := range out {
		if c == 0 {
			out = out[:i]
			break
		}
	}
	return string(utf16.Decode(out))
}
