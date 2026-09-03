package journal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Tailer follows the journal as the writer extends it.
//
// Next returns (nil, nil) when it has caught up. That is not an error and not
// the end — it is everything durable so far. Callers block on a notification
// and ask again; nothing here polls, because a ticker would add its own period
// to every event's latency and burn a core when the market is quiet.
type Tailer struct {
	dir      string
	segments []segmentRef
	index    int
	file     *os.File
	offset   int64
	nextSeq  uint64

	frameHdr [FrameHeaderLen]byte
	payload  []byte
}

// OpenTailer positions a tailer to deliver everything after seq.
//
// Zero means from the beginning, which is always a correct place to start:
// every consumer of this log is building a projection, and a projection is a
// fold that can be re-run.
func OpenTailer(dir string, after uint64) (*Tailer, error) {
	segments, err := ListSegments(dir)
	if err != nil {
		return nil, err
	}
	t := &Tailer{
		dir:      dir,
		segments: segments,
		nextSeq:  after + 1,
		payload:  make([]byte, 0, 1024),
	}
	if err := t.seekToStart(); err != nil {
		return nil, err
	}
	return t, nil
}

// NextSeq is the sequence this tailer will deliver next.
func (t *Tailer) NextSeq() uint64 { return t.nextSeq }

// Close releases the open segment.
func (t *Tailer) Close() error {
	if t.file == nil {
		return nil
	}
	err := t.file.Close()
	t.file = nil
	return err
}

// Next returns the next record, or (nil, nil) when caught up.
func (t *Tailer) Next() (*Record, error) {
	for {
		if t.file == nil {
			opened, err := t.openCurrent()
			if err != nil {
				return nil, err
			}
			if !opened {
				grew, err := t.rescan()
				if err != nil {
					return nil, err
				}
				if !grew {
					return nil, nil
				}
				continue
			}
		}

		size, err := t.size()
		if err != nil {
			return nil, err
		}

		header, ok, err := t.readFrameAt(t.offset, size)
		if err != nil {
			return nil, err
		}
		if ok {
			if header.Seq != t.nextSeq {
				return nil, fmt.Errorf("%w: expected %d, found %d", ErrSequenceGap, t.nextSeq, header.Seq)
			}
			t.offset += header.FrameLen()
			t.nextSeq = header.Seq + 1
			payload := make([]byte, len(t.payload))
			copy(payload, t.payload)
			return &Record{Header: header, Payload: payload}, nil
		}

		// Nothing more in this segment. If a later one exists we have reached
		// its end; otherwise we have caught up with the writer.
		if t.index+1 < len(t.segments) {
			if err := t.advance(); err != nil {
				return nil, err
			}
			continue
		}
		grew, err := t.rescan()
		if err != nil {
			return nil, err
		}
		if !grew {
			return nil, nil
		}
	}
}

// Drain reads everything available now.
func (t *Tailer) Drain() ([]*Record, error) {
	var out []*Record
	for {
		record, err := t.Next()
		if err != nil {
			return out, err
		}
		if record == nil {
			return out, nil
		}
		out = append(out, record)
	}
}

func (t *Tailer) size() (int64, error) {
	info, err := t.file.Stat()
	if err != nil {
		return 0, fmt.Errorf("stat segment: %w", err)
	}
	return info.Size(), nil
}

// readFrameAt reads one frame. The false return means "not a whole frame
// there yet", which during live tailing is the writer not having flushed this
// far — never a reason to go looking for the next plausible frame.
func (t *Tailer) readFrameAt(offset, size int64) (Header, bool, error) {
	if size-offset < FrameHeaderLen {
		return Header{}, false, nil
	}
	if _, err := t.file.ReadAt(t.frameHdr[:], offset); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return Header{}, false, nil
		}
		return Header{}, false, fmt.Errorf("read frame header: %w", err)
	}
	length := int64(uint32(t.frameHdr[0]) | uint32(t.frameHdr[1])<<8 |
		uint32(t.frameHdr[2])<<16 | uint32(t.frameHdr[3])<<24)
	if length > MaxPayload {
		return Header{}, false, fmt.Errorf("%w: length %d over cap", ErrCorrupt, length)
	}
	if offset+FrameHeaderLen+length > size {
		return Header{}, false, nil
	}
	if cap(t.payload) < int(length) {
		t.payload = make([]byte, length)
	}
	t.payload = t.payload[:length]
	if _, err := t.file.ReadAt(t.payload, offset+FrameHeaderLen); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return Header{}, false, nil
		}
		return Header{}, false, fmt.Errorf("read frame payload: %w", err)
	}
	header, err := DecodeFrameHeader(t.frameHdr[:], t.payload)
	if err != nil {
		return Header{}, false, err
	}
	return header, true, nil
}

func (t *Tailer) seekToStart() error {
	if len(t.segments) == 0 {
		return nil
	}
	// The last segment whose base sequence is at or below what we want.
	t.index = 0
	for i, seg := range t.segments {
		if seg.baseSeq <= t.nextSeq {
			t.index = i
		}
	}
	t.offset = 0
	opened, err := t.openCurrent()
	if err != nil || !opened {
		return err
	}
	size, err := t.size()
	if err != nil {
		return err
	}
	// Walk forward inside the segment. Segments are bounded, so this is a scan
	// of at most one file, and only at startup.
	for {
		header, ok, err := t.readFrameAt(t.offset, size)
		if err != nil || !ok {
			return err
		}
		if header.Seq >= t.nextSeq {
			t.nextSeq = header.Seq
			return nil
		}
		t.offset += header.FrameLen()
	}
}

func (t *Tailer) openCurrent() (bool, error) {
	if t.index >= len(t.segments) {
		return false, nil
	}
	file, err := os.Open(t.segments[t.index].path)
	if err != nil {
		return false, fmt.Errorf("open segment: %w", err)
	}
	header := make([]byte, SegmentHeaderLen)
	if _, err := io.ReadFull(file, header); err != nil {
		file.Close()
		return false, fmt.Errorf("read segment header: %w", err)
	}
	if _, err := DecodeSegmentHeader(header); err != nil {
		file.Close()
		return false, err
	}
	if t.offset == 0 {
		t.offset = SegmentHeaderLen
	}
	t.file = file
	return true, nil
}

func (t *Tailer) advance() error {
	if err := t.Close(); err != nil {
		return err
	}
	t.index++
	t.offset = 0
	return nil
}

// rescan re-lists the directory and reports whether a segment appeared that we
// had not seen. The writer rolls between flush groups, so a tailer that has
// reached the end of one file has to look again before concluding anything.
func (t *Tailer) rescan() (bool, error) {
	found, err := ListSegments(t.dir)
	if err != nil {
		return false, err
	}
	grew := len(found) > len(t.segments)
	t.segments = found
	if grew && t.index+1 < len(t.segments) {
		if err := t.advance(); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

// Cursor is a consumer's position, persisted next to the journal.
//
// Written by rename, so it is never observed half-updated. The value is ASCII
// decimal with a newline: a human debugging a stuck consumer should be able to
// cat it.
type Cursor struct {
	path string
	seq  uint64
}

// LoadCursor reads a consumer's position, or starts from the beginning.
func LoadCursor(dir, consumer string) (*Cursor, error) {
	path := filepath.Join(dir, cursorDir, consumer)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return &Cursor{path: path}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read cursor: %w", err)
	}
	seq, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		// An unreadable cursor is the same situation as a missing one, and
		// replaying a projection is always safe.
		return &Cursor{path: path}, nil
	}
	return &Cursor{path: path, seq: seq}, nil
}

// Seq is the last sequence this consumer finished with.
func (c *Cursor) Seq() uint64 { return c.seq }

// Commit records that everything up to seq is done.
func (c *Cursor) Commit(seq uint64) error {
	if seq < c.seq {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(c.path), 0o755); err != nil {
		return fmt.Errorf("create cursor dir: %w", err)
	}
	tmp := c.path + ".tmp"
	if err := os.WriteFile(tmp, []byte(strconv.FormatUint(seq, 10)+"\n"), 0o644); err != nil {
		return fmt.Errorf("write cursor: %w", err)
	}
	if err := os.Rename(tmp, c.path); err != nil {
		return fmt.Errorf("rename cursor: %w", err)
	}
	c.seq = seq
	return nil
}
