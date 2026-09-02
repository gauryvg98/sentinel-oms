package journal

import (
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

// goldenDir is the fixture the Rust writer produces. Both sides read it, which
// is the only way a two-language format stays one format.
func goldenDir(t *testing.T) string {
	t.Helper()
	dir, err := filepath.Abs(filepath.Join("..", "..", "..", "testdata", "journal-golden"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, SegmentName(1))); err != nil {
		t.Skipf("golden journal absent; regenerate with "+
			"SENTINEL_REGOLD=1 cargo test -p sentinel-record --test golden (%v)", err)
	}
	return dir
}

func TestGoReadsTheFramingRustWrote(t *testing.T) {
	// Framing only. What the payloads *mean* is the record package's contract,
	// and asserting it here as well would be two tests that have to be edited
	// together and one that gets forgotten.
	tailer, err := OpenTailer(goldenDir(t), 0)
	if err != nil {
		t.Fatalf("open tailer: %v", err)
	}
	defer tailer.Close()

	records, err := tailer.Drain()
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if len(records) < 2 {
		t.Fatalf("read %d records; the fixture should hold more", len(records))
	}

	// Record 1 is always the writer's claim.
	if got := records[0].Header.Kind; got != KindEpochClaimed {
		t.Errorf("record 1 kind = %v, want %v", got, KindEpochClaimed)
	}

	// Sequences are gap-free from 1, the epoch is the writer's, every kind is
	// one we know, and time only moves forward.
	var lastNanos uint64
	for i, r := range records {
		if want := uint64(i + 1); r.Header.Seq != want {
			t.Errorf("record %d seq = %d, want %d", i, r.Header.Seq, want)
		}
		if r.Header.Epoch != 1 {
			t.Errorf("record %d epoch = %d, want 1", i, r.Header.Epoch)
		}
		if !r.Header.Kind.Valid() {
			t.Errorf("record %d kind %v is not known", i, r.Header.Kind)
		}
		if r.Header.Nanos < lastNanos {
			t.Errorf("record %d went back in time: %d after %d", i, r.Header.Nanos, lastNanos)
		}
		lastNanos = r.Header.Nanos
		if int(r.Header.Len) != len(r.Payload) {
			t.Errorf("record %d claims %d payload bytes, carries %d",
				i, r.Header.Len, len(r.Payload))
		}
	}
}

func TestTailerCanStartPartwayThrough(t *testing.T) {
	tailer, err := OpenTailer(goldenDir(t), 4)
	if err != nil {
		t.Fatalf("open tailer: %v", err)
	}
	defer tailer.Close()

	record, err := tailer.Next()
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if record == nil {
		t.Fatal("expected a record")
	}
	if record.Header.Seq != 5 {
		t.Errorf("seq = %d, want 5", record.Header.Seq)
	}
}

func TestCaughtUpIsNotAnError(t *testing.T) {
	tailer, err := OpenTailer(goldenDir(t), 0)
	if err != nil {
		t.Fatalf("open tailer: %v", err)
	}
	defer tailer.Close()

	if _, err := tailer.Drain(); err != nil {
		t.Fatalf("drain: %v", err)
	}
	record, err := tailer.Next()
	if err != nil {
		t.Errorf("caught up returned an error: %v", err)
	}
	if record != nil {
		t.Errorf("caught up returned a record: %+v", record)
	}
}

func TestChecksumAgreesWithTheRustSide(t *testing.T) {
	// Both implementations use the published Castagnoli polynomial, so the
	// standard check value is the thing that proves they agree — without
	// either of them porting the other's table.
	got := crc32.Update(0, castagnoli, []byte("123456789"))
	if got != 0xE3069283 {
		t.Errorf("CRC-32C(\"123456789\") = %#08x, want 0xe3069283", got)
	}
}

func TestAFlippedBitIsCaught(t *testing.T) {
	dir := goldenDir(t)
	raw, err := os.ReadFile(filepath.Join(dir, SegmentName(1)))
	if err != nil {
		t.Fatal(err)
	}

	// Corrupt one byte of the first record's payload region and confirm the
	// reader refuses it rather than handing it on.
	scratch := t.TempDir()
	raw[len(raw)-1] ^= 0x40
	if err := os.WriteFile(filepath.Join(scratch, SegmentName(1)), raw, 0o644); err != nil {
		t.Fatal(err)
	}

	tailer, err := OpenTailer(scratch, 0)
	if err != nil {
		t.Fatalf("open tailer: %v", err)
	}
	defer tailer.Close()

	if _, err := tailer.Drain(); !errors.Is(err, ErrCorrupt) {
		t.Errorf("drain error = %v, want ErrCorrupt", err)
	}
}

func TestForeignFilesAreRefused(t *testing.T) {
	scratch := t.TempDir()
	bogus := make([]byte, SegmentHeaderLen)
	copy(bogus, []byte("NOTOURS\x01"))
	if err := os.WriteFile(filepath.Join(scratch, SegmentName(1)), bogus, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenTailer(scratch, 0); !errors.Is(err, ErrNotAJournal) {
		t.Errorf("error = %v, want ErrNotAJournal", err)
	}
}

func TestAKindByteThatWasNeverWrittenIsRefused(t *testing.T) {
	if Kind(0).Valid() {
		t.Error("kind 0 is valid; a field that was never written would decode")
	}
	for k := Kind(1); k <= KindTicked; k++ {
		if !k.Valid() {
			t.Errorf("kind %d should be valid", k)
		}
	}
}

func TestSegmentNamesSortInSequenceOrder(t *testing.T) {
	names := []string{SegmentName(104729), SegmentName(1), SegmentName(16)}
	if names[0] != "0000000000019919.jrnl" {
		t.Errorf("SegmentName(104729) = %q", names[0])
	}
	if !(names[1] < names[2] && names[2] < names[0]) {
		t.Errorf("lexical order is not sequence order: %v", names)
	}
}

func TestCursorRoundTrips(t *testing.T) {
	dir := t.TempDir()
	cursor, err := LoadCursor(dir, "projectord")
	if err != nil {
		t.Fatal(err)
	}
	if cursor.Seq() != 0 {
		t.Errorf("a consumer with no cursor should start over, got %d", cursor.Seq())
	}
	if err := cursor.Commit(7); err != nil {
		t.Fatal(err)
	}

	reloaded, err := LoadCursor(dir, "projectord")
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.Seq() != 7 {
		t.Errorf("reloaded cursor = %d, want 7", reloaded.Seq())
	}

	// Backwards is refused: harmless for a projection, but it would hide the
	// bug that caused it.
	if err := reloaded.Commit(3); err != nil {
		t.Fatal(err)
	}
	if reloaded.Seq() != 7 {
		t.Errorf("cursor moved backwards to %d", reloaded.Seq())
	}
}

func TestEmptyDirectoryYieldsNothingRatherThanFailing(t *testing.T) {
	tailer, err := OpenTailer(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("open tailer: %v", err)
	}
	defer tailer.Close()
	record, err := tailer.Next()
	if err != nil || record != nil {
		t.Errorf("empty journal gave (%v, %v)", record, err)
	}
}
