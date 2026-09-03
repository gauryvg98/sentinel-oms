// Package journal reads the log that sentineld writes.
//
// Read only, on purpose. There is exactly one writer and it is in Rust; a Go
// append path would be a second implementation of the durability rules, and two
// implementations of a rule is one rule and one bug waiting to be found.
//
// The format is specified in docs/rewrite/JOURNAL-FORMAT.md and pinned by
// testdata/journal-golden, which the Rust side writes and both sides read. If
// this file and that document disagree, the golden fixture is the tiebreak.
package journal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	// SegmentHeaderLen is the size of the header at the start of every segment.
	SegmentHeaderLen = 32
	// FrameHeaderLen is the size of the header on every record.
	FrameHeaderLen = 36
	// MaxPayload bounds a single record. A length above it is a torn frame
	// rather than a big one, and refusing early stops a reader allocating a
	// gigabyte because four bytes were garbage.
	MaxPayload = 1 << 20
	// Version is the format version this reader speaks.
	Version = 2
	// segmentExt is the extension every segment carries.
	segmentExt = ".jrnl"
	// cursorDir holds one file per consumer.
	cursorDir = "cursors"
)

// Magic identifies a segment file.
var Magic = [8]byte{'S', 'E', 'N', 'T', 'J', 'R', 'N', 0x01}

// castagnoli is the same polynomial the Rust writer uses. Sharing a published
// CRC rather than inventing one is what lets these two implementations agree
// without either porting a table.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Kind is what a record holds.
type Kind uint16

// The record kinds. Zero is deliberately not one of them, so a kind field that
// was never written is refused rather than decoded as whichever came first.
const (
	KindEpochClaimed    Kind = 1
	KindIntentPersisted Kind = 2
	KindOrderEvent      Kind = 3
	KindDecision        Kind = 4
	KindMark            Kind = 5
	KindBalance         Kind = 6
	KindTicked          Kind = 7
)

var kindNames = map[Kind]string{
	KindEpochClaimed:    "EPOCH_CLAIMED",
	KindIntentPersisted: "INTENT_PERSISTED",
	KindOrderEvent:      "ORDER_EVENT",
	KindDecision:        "DECISION",
	KindMark:            "MARK",
	KindBalance:         "BALANCE",
	KindTicked:          "TICKED",
}

func (k Kind) String() string {
	if name, ok := kindNames[k]; ok {
		return name
	}
	return fmt.Sprintf("Kind(%d)", uint16(k))
}

// Valid reports whether this is a kind we know.
func (k Kind) Valid() bool {
	_, ok := kindNames[k]
	return ok
}

// Header is everything about a record except its payload.
type Header struct {
	Seq   uint64
	Epoch uint64
	Nanos uint64
	Kind  Kind
	Len   uint32
}

// FrameLen is the total bytes the record occupies on disk.
func (h Header) FrameLen() int64 { return FrameHeaderLen + int64(h.Len) }

// Record is one entry from the log.
type Record struct {
	Header  Header
	Payload []byte
}

// Errors the reader distinguishes. A caller has to be able to tell "nothing
// more yet" from "this log is damaged", because one is normal and the other
// must stop the consumer.
var (
	// ErrNotAJournal means the file's magic is not ours.
	ErrNotAJournal = errors.New("not a sentinel journal segment")
	// ErrCorrupt means a complete-looking frame failed its checks.
	ErrCorrupt = errors.New("journal frame is corrupt")
	// ErrSequenceGap means the log skips. A hole, not a gap to read past.
	ErrSequenceGap = errors.New("journal skips a sequence")
)

// UnsupportedVersionError is a segment written by a format we do not speak.
type UnsupportedVersionError struct{ Found uint32 }

func (e UnsupportedVersionError) Error() string {
	return fmt.Sprintf("journal format version %d, expected %d", e.Found, Version)
}

// SegmentHeader is what a segment file starts with.
type SegmentHeader struct {
	BaseSeq uint64
	Created uint64
}

// DecodeSegmentHeader reads a segment header.
func DecodeSegmentHeader(b []byte) (SegmentHeader, error) {
	if len(b) < SegmentHeaderLen {
		return SegmentHeader{}, ErrCorrupt
	}
	for i, want := range Magic {
		if b[i] != want {
			return SegmentHeader{}, ErrNotAJournal
		}
	}
	if v := binary.LittleEndian.Uint32(b[8:12]); v != Version {
		return SegmentHeader{}, UnsupportedVersionError{Found: v}
	}
	return SegmentHeader{
		BaseSeq: binary.LittleEndian.Uint64(b[16:24]),
		Created: binary.LittleEndian.Uint64(b[24:32]),
	}, nil
}

// DecodeFrameHeader reads a frame header and verifies it against its payload.
//
// The checksum covers the header from byte 8 onward and then the payload, so a
// corrupted sequence or timestamp is caught as surely as a corrupted payload.
func DecodeFrameHeader(b []byte, payload []byte) (Header, error) {
	if len(b) < FrameHeaderLen {
		return Header{}, ErrCorrupt
	}
	length := binary.LittleEndian.Uint32(b[0:4])
	if length > MaxPayload {
		return Header{}, fmt.Errorf("%w: length %d over cap", ErrCorrupt, length)
	}
	if uint32(len(payload)) != length {
		return Header{}, fmt.Errorf("%w: claims %d payload bytes, have %d", ErrCorrupt, length, len(payload))
	}

	stored := binary.LittleEndian.Uint32(b[4:8])
	var withoutCRC [FrameHeaderLen]byte
	copy(withoutCRC[:], b[:FrameHeaderLen])
	binary.LittleEndian.PutUint32(withoutCRC[4:8], 0)
	computed := crc32.Update(0, castagnoli, withoutCRC[8:])
	computed = crc32.Update(computed, castagnoli, payload)
	if stored != computed {
		return Header{}, fmt.Errorf("%w: checksum %#08x does not match %#08x", ErrCorrupt, stored, computed)
	}

	kind := Kind(binary.LittleEndian.Uint16(b[32:34]))
	if !kind.Valid() {
		return Header{}, fmt.Errorf("%w: record kind %d is not known", ErrCorrupt, uint16(kind))
	}

	return Header{
		Seq:   binary.LittleEndian.Uint64(b[8:16]),
		Epoch: binary.LittleEndian.Uint64(b[16:24]),
		Nanos: binary.LittleEndian.Uint64(b[24:32]),
		Kind:  kind,
		Len:   length,
	}, nil
}

// SegmentName is a segment's file name: its base sequence in 16 hex digits.
// Fixed width, so lexical order is numeric order.
func SegmentName(baseSeq uint64) string {
	return fmt.Sprintf("%016x%s", baseSeq, segmentExt)
}

func parseSegmentName(name string) (uint64, bool) {
	stem, ok := strings.CutSuffix(name, segmentExt)
	if !ok || len(stem) != 16 {
		return 0, false
	}
	seq, err := strconv.ParseUint(stem, 16, 64)
	if err != nil {
		return 0, false
	}
	return seq, true
}

type segmentRef struct {
	baseSeq uint64
	path    string
}

// ListSegments returns every segment in dir, in sequence order.
func ListSegments(dir string) ([]segmentRef, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read journal dir: %w", err)
	}
	var found []segmentRef
	for _, entry := range entries {
		if seq, ok := parseSegmentName(entry.Name()); ok {
			found = append(found, segmentRef{baseSeq: seq, path: filepath.Join(dir, entry.Name())})
		}
	}
	sort.Slice(found, func(i, j int) bool { return found[i].baseSeq < found[j].baseSeq })
	return found, nil
}
