package record

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/gauryvg98/sentinel-oms/go/internal/fixed"
	"github.com/gauryvg98/sentinel-oms/go/internal/journal"
)

// goldenDir is the fixture the Rust writer produces. Both sides read it, which
// is the only thing keeping a two-language format from becoming two formats.
func goldenDir(t *testing.T) string {
	t.Helper()
	dir, err := filepath.Abs(filepath.Join("..", "..", "..", "testdata", "journal-golden"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, journal.SegmentName(1))); err != nil {
		t.Skipf("golden journal absent; regenerate with "+
			"SENTINEL_REGOLD=1 cargo test -p sentinel-record --test golden (%v)", err)
	}
	return dir
}

// readGolden decodes the whole fixture, skipping the writer's claim record.
func readGolden(t *testing.T) []Record {
	t.Helper()
	tailer, err := journal.OpenTailer(goldenDir(t), 0)
	if err != nil {
		t.Fatalf("open tailer: %v", err)
	}
	defer tailer.Close()

	frames, err := tailer.Drain()
	if err != nil {
		t.Fatalf("drain: %v", err)
	}

	var out []Record
	for i, frame := range frames {
		decoded, err := Decode(frame.Header.Kind, frame.Payload)
		if err != nil {
			t.Fatalf("record %d (%v): %v", i, frame.Header.Kind, err)
		}
		if decoded.Kind == journal.KindEpochClaimed {
			continue
		}
		out = append(out, decoded)
	}
	return out
}

func TestGoDecodesWhatRustEncoded(t *testing.T) {
	records := readGolden(t)
	if len(records) == 0 {
		t.Fatal("the fixture decoded to nothing")
	}

	// Every kind the fixture carries must have decoded into its own field.
	// A record whose payload parsed but landed nowhere is worse than one that
	// failed, because it looks like it worked.
	for i, r := range records {
		switch r.Kind {
		case journal.KindIntentPersisted:
			if r.Intent == nil {
				t.Errorf("record %d: intent is nil", i)
			}
		case journal.KindOrderEvent:
			if r.Event == nil {
				t.Errorf("record %d: event is nil", i)
			}
		case journal.KindDecision:
			if r.Decision == nil {
				t.Errorf("record %d: decision is nil", i)
			}
		}
	}
}

func TestTheIntentSurvivesTheCrossing(t *testing.T) {
	for _, r := range readGolden(t) {
		if r.Kind != journal.KindIntentPersisted {
			continue
		}
		intent := r.Intent
		if intent.ClientOrderID != "UI-4bb5ba826e2e1" {
			t.Errorf("client order id = %q", intent.ClientOrderID)
		}
		if intent.Instrument != "BTCUSD" {
			t.Errorf("instrument = %q", intent.Instrument)
		}
		if intent.Side != Buy {
			t.Errorf("side = %v", intent.Side)
		}
		if intent.Kind != Limit {
			t.Errorf("kind = %v", intent.Kind)
		}
		if intent.Authority != Entry {
			t.Errorf("authority = %v", intent.Authority)
		}
		if intent.Qty != fixed.MustParse("0.003") {
			t.Errorf("qty = %v", intent.Qty)
		}
		if intent.LimitPrice == nil || *intent.LimitPrice != fixed.MustParse("109432.5") {
			t.Errorf("limit price = %v", intent.LimitPrice)
		}
		if intent.StopPrice != nil {
			t.Errorf("stop price should be absent, got %v", *intent.StopPrice)
		}
		if intent.TraceID != "0000000000000000000000006440ba96" {
			t.Errorf("trace = %q", intent.TraceID)
		}
		return
	}
	t.Fatal("the fixture has no intent")
}

func TestTheStatesAreReadRatherThanDerived(t *testing.T) {
	// The whole reason the record carries them: this package has no state
	// machine, and must not need one.
	want := []struct {
		from, to State
		kind     EventKind
	}{
		{Created, Submitting, SubmissionStarted},
		{Submitting, Unknown, SubmissionTimedOut},
		{Unknown, Reconciling, ReconcileStarted},
		{Reconciling, Working, ReconcileResolved},
		{Working, Partial, FillReceived},
		{Partial, CancelPending, CancelRequested},
		{CancelPending, Canceled, CancelConfirmed},
		{Canceled, Canceled, FillReceived},
		{Submitting, Rejected, VenueRejected},
		{Submitting, Working, VenueAcked},
		// A fill applied while reconciling: the state does not move, and the
		// record says it was applied anyway.
		{Reconciling, Reconciling, FillReceived},
		// The same shape, recorded as evidence only.
		{Filled, Filled, FillReceived},
	}

	var got []*Event
	for _, r := range readGolden(t) {
		if r.Kind == journal.KindOrderEvent {
			got = append(got, r.Event)
		}
	}
	if len(got) != len(want) {
		t.Fatalf("read %d events, want %d", len(got), len(want))
	}
	for i, w := range want {
		if got[i].From != w.from || got[i].To != w.to || got[i].Kind != w.kind {
			t.Errorf("event %d = %v %v->%v, want %v %v->%v",
				i, got[i].Kind, got[i].From, got[i].To, w.kind, w.from, w.to)
		}
	}
}

func TestEvidenceIsDistinguishableFromApplication(t *testing.T) {
	// A fill for an order believed terminal is recorded and applied to nothing.
	// The flag says so — and the fixture also holds a fill that is applied with
	// From == To, so this cannot pass by reading the states.
	var evidence, applied int
	for _, r := range readGolden(t) {
		if r.Kind != journal.KindOrderEvent {
			continue
		}
		if r.Event.Applied() {
			applied++
		} else {
			evidence++
		}
	}
	if evidence != 2 {
		t.Errorf("found %d evidence-only events, want 2", evidence)
	}
	if applied == 0 {
		t.Error("no event moved anything")
	}
}

func TestFillsCarryTheirExecutionIDAndExactNumbers(t *testing.T) {
	for _, r := range readGolden(t) {
		if r.Kind != journal.KindOrderEvent || r.Event.Kind != FillReceived {
			continue
		}
		if !r.Event.Applied() {
			continue
		}
		if r.Event.ExecID != "E-9911" {
			t.Errorf("exec id = %q", r.Event.ExecID)
		}
		if r.Event.Qty != fixed.MustParse("0.002") {
			t.Errorf("qty = %v", r.Event.Qty)
		}
		if r.Event.Price != fixed.MustParse("109432.5") {
			t.Errorf("price = %v", r.Event.Price)
		}
		return
	}
	t.Fatal("the fixture has no applied fill")
}

func TestRejectionsKeepTheVenuesOwnWords(t *testing.T) {
	for _, r := range readGolden(t) {
		if r.Kind != journal.KindOrderEvent || r.Event.Kind != VenueRejected {
			continue
		}
		if r.Event.RejectCode != -2019 {
			t.Errorf("code = %d", r.Event.RejectCode)
		}
		if r.Event.RejectText != "insufficient_margin" {
			t.Errorf("text = %q", r.Event.RejectText)
		}
		return
	}
	t.Fatal("the fixture has no rejection")
}

func TestDecisionsExplainThemselves(t *testing.T) {
	for _, r := range readGolden(t) {
		if r.Kind != journal.KindDecision {
			continue
		}
		if r.Decision.Kind != EntryBlocked {
			t.Errorf("kind = %v", r.Decision.Kind)
		}
		if r.Decision.Actor != 2 { // guards
			t.Errorf("actor = %v", r.Decision.Actor)
		}
		if r.Decision.Note != "instrument locked by UI-4bb5ba826e2e1" {
			t.Errorf("note = %q", r.Decision.Note)
		}
		if r.Decision.Value != fixed.MustParse("-118.81") {
			t.Errorf("value = %v", r.Decision.Value)
		}
		return
	}
	t.Fatal("the fixture has no decision")
}

func TestMarksAndBalancesDecode(t *testing.T) {
	var sawMark, sawBalance bool
	for _, r := range readGolden(t) {
		switch r.Kind {
		case journal.KindMark:
			sawMark = true
			if r.MarkInstrument != "BTCUSD" || r.MarkPrice != fixed.MustParse("109432.5") {
				t.Errorf("mark = %s %v", r.MarkInstrument, r.MarkPrice)
			}
		case journal.KindBalance:
			sawBalance = true
			if r.Asset != "USD" || r.Amount != fixed.MustParse("5942.65") {
				t.Errorf("balance = %s %v", r.Asset, r.Amount)
			}
		}
	}
	if !sawMark || !sawBalance {
		t.Errorf("mark %v, balance %v", sawMark, sawBalance)
	}
}

func TestATruncatedPayloadIsRefused(t *testing.T) {
	tailer, err := journal.OpenTailer(goldenDir(t), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer tailer.Close()
	frames, err := tailer.Drain()
	if err != nil {
		t.Fatal(err)
	}

	for _, frame := range frames {
		for cut := 0; cut < len(frame.Payload); cut++ {
			if _, err := Decode(frame.Header.Kind, frame.Payload[:cut]); err == nil {
				t.Fatalf("%v decoded from %d of %d bytes",
					frame.Header.Kind, cut, len(frame.Payload))
			}
		}
	}
}

func TestTrailingBytesAreRefused(t *testing.T) {
	// A record from another version of the format wearing the same kind byte.
	tailer, err := journal.OpenTailer(goldenDir(t), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer tailer.Close()
	frames, err := tailer.Drain()
	if err != nil {
		t.Fatal(err)
	}

	for _, frame := range frames {
		extended := append(append([]byte{}, frame.Payload...), 0)
		if _, err := Decode(frame.Header.Kind, extended); !errors.Is(err, ErrTrailing) {
			t.Errorf("%v accepted a trailing byte: %v", frame.Header.Kind, err)
		}
	}
}

func TestZeroIsNeverAValidDiscriminant(t *testing.T) {
	if State(0).Valid() {
		t.Error("state 0 is valid")
	}
	for s := Created; s <= Rejected; s++ {
		if !s.Valid() {
			t.Errorf("state %d should be valid", s)
		}
	}
}

func TestStateGroupingsMatchTheEngine(t *testing.T) {
	// If these drift from sentinel-domain, the gateway shows an order as live
	// that the engine has finished with.
	terminal := map[State]bool{Filled: true, Canceled: true, Rejected: true}
	locking := map[State]bool{Unknown: true, Reconciling: true}
	for s := Created; s <= Rejected; s++ {
		if s.IsTerminal() != terminal[s] {
			t.Errorf("%v: IsTerminal = %v", s, s.IsTerminal())
		}
		if s.LocksInstrument() != locking[s] {
			t.Errorf("%v: LocksInstrument = %v", s, s.LocksInstrument())
		}
	}
}
