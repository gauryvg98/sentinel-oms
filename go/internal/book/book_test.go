package book

import (
	"testing"

	"github.com/gauryvg98/sentinel-oms/go/internal/fixed"
	"github.com/gauryvg98/sentinel-oms/go/internal/journal"
	"github.com/gauryvg98/sentinel-oms/go/internal/record"
)

// TestPositionAccountingMatchesTheEngine runs the same scenarios as the Rust
// position tests, against the same expected numbers.
//
// Position accounting is the one thing this package genuinely duplicates. This
// table is what makes the duplication safe: if the two implementations drift,
// the numbers here stop matching and this fails. The cases and their answers
// are copied from sentinel-ledger/src/position.rs, deliberately by hand — a
// generated comparison would only prove the generator agrees with itself.
func TestPositionAccountingMatchesTheEngine(t *testing.T) {
	type fill struct {
		side  record.Side
		qty   string
		price string
	}
	cases := []struct {
		name         string
		fills        []fill
		wantQty      string
		wantBasis    string
		wantRealized string
	}{
		{
			name:         "buying opens a long at the fill price",
			fills:        []fill{{record.Buy, "3", "100"}},
			wantQty:      "3",
			wantBasis:    "300",
			wantRealized: "0",
		},
		{
			name:         "adding to a long averages the entry",
			fills:        []fill{{record.Buy, "2", "100"}, {record.Buy, "2", "110"}},
			wantQty:      "4",
			wantBasis:    "420",
			wantRealized: "0",
		},
		{
			name:         "selling a long books the difference",
			fills:        []fill{{record.Buy, "4", "100"}, {record.Sell, "1", "110"}},
			wantQty:      "3",
			wantBasis:    "300",
			wantRealized: "10",
		},
		{
			name:         "a short books profit when price falls",
			fills:        []fill{{record.Sell, "2", "100"}, {record.Buy, "2", "90"}},
			wantQty:      "0",
			wantBasis:    "0",
			wantRealized: "20",
		},
		{
			name:         "a short books a loss when price rises",
			fills:        []fill{{record.Sell, "2", "100"}, {record.Buy, "2", "110"}},
			wantQty:      "0",
			wantBasis:    "0",
			wantRealized: "-20",
		},
		{
			name:         "partially closing a short leaves the rest short",
			fills:        []fill{{record.Sell, "4", "100"}, {record.Buy, "1", "90"}},
			wantQty:      "-3",
			wantBasis:    "-300",
			wantRealized: "10",
		},
		{
			name:         "a fill larger than the position flips it",
			fills:        []fill{{record.Buy, "2", "100"}, {record.Sell, "5", "110"}},
			wantQty:      "-3",
			wantBasis:    "-330",
			wantRealized: "20",
		},
		{
			name:         "a short flipped long carries nothing across either",
			fills:        []fill{{record.Sell, "2", "100"}, {record.Buy, "5", "90"}},
			wantQty:      "3",
			wantBasis:    "270",
			wantRealized: "20",
		},
		{
			name: "closing flat leaves no residual basis",
			fills: []fill{
				{record.Buy, "3", "109432.5"},
				{record.Sell, "1", "109500"},
				{record.Sell, "1", "109400"},
				{record.Sell, "1", "109450"},
			},
			wantQty:      "0",
			wantBasis:    "0",
			wantRealized: "52.5",
		},
		{
			name:         "a round trip at the same price books nothing",
			fills:        []fill{{record.Buy, "7", "109432.5"}, {record.Sell, "7", "109432.5"}},
			wantQty:      "0",
			wantBasis:    "0",
			wantRealized: "0",
		},
		{
			name:         "fractional sizes stay exact",
			fills:        []fill{{record.Buy, "0.001", "109432.5"}, {record.Sell, "0.001", "109532.5"}},
			wantQty:      "0",
			wantBasis:    "0",
			wantRealized: "0.1",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var p Position
			for _, f := range c.fills {
				p.applyFill(f.side, fixed.MustParse(f.qty), fixed.MustParse(f.price))
			}
			if got := p.Qty.String(); got != c.wantQty {
				t.Errorf("qty = %s, want %s", got, c.wantQty)
			}
			if got := p.CostBasis.String(); got != c.wantBasis {
				t.Errorf("cost basis = %s, want %s", got, c.wantBasis)
			}
			if got := p.Realized.String(); got != c.wantRealized {
				t.Errorf("realized = %s, want %s", got, c.wantRealized)
			}
		})
	}
}

func TestUnrealisedTracksTheMarkInBothDirections(t *testing.T) {
	var long Position
	long.applyFill(record.Buy, fixed.Whole(2), fixed.MustParse("100"))
	if got := long.Unrealized(fixed.MustParse("110")).String(); got != "20" {
		t.Errorf("long at 110 = %s", got)
	}
	if got := long.Unrealized(fixed.MustParse("90")).String(); got != "-20" {
		t.Errorf("long at 90 = %s", got)
	}

	var short Position
	short.applyFill(record.Sell, fixed.Whole(2), fixed.MustParse("100"))
	if got := short.Unrealized(fixed.MustParse("90")).String(); got != "20" {
		t.Errorf("short at 90 = %s", got)
	}
}

func TestAFlatPositionHasNoAverageAndNoPnL(t *testing.T) {
	var p Position
	if !p.AvgEntry().IsZero() {
		t.Errorf("avg entry = %v", p.AvgEntry())
	}
	if !p.Unrealized(fixed.MustParse("109432.5")).IsZero() {
		t.Errorf("unrealized = %v", p.Unrealized(fixed.MustParse("109432.5")))
	}
}

// --------------------------------------------------------------- folding

func header(seq, nanos uint64, kind journal.Kind) journal.Header {
	return journal.Header{Seq: seq, Epoch: 1, Nanos: nanos, Kind: kind}
}

func intent(id string, side record.Side, qty string, authority record.Authority) record.Record {
	return record.Record{
		Kind: journal.KindIntentPersisted,
		Intent: &record.Intent{
			ClientOrderID: id,
			Instrument:    "BTCUSD",
			Side:          side,
			Kind:          record.Market,
			Authority:     authority,
			Qty:           fixed.MustParse(qty),
			TraceID:       "0000000000000000000000006440ba96",
		},
	}
}

// event builds an applied event, which is what almost every test means.
// Use evidence() for one the ledger recorded without booking.
func event(id string, from, to record.State, kind record.EventKind) record.Record {
	return record.Record{
		Kind: journal.KindOrderEvent,
		Event: &record.Event{
			ClientOrderID: id,
			From:          from,
			To:            to,
			IsApplied:     true,
			Kind:          kind,
		},
	}
}

// evidence builds an event the ledger recorded and applied to nothing.
func evidence(id string, state record.State, kind record.EventKind) record.Record {
	r := event(id, state, state, kind)
	r.Event.IsApplied = false
	return r
}

// evidenceFill is a fill recorded as evidence: real, and booked by nobody.
func evidenceFill(id, exec string, state record.State, qty, price string) record.Record {
	r := evidence(id, state, record.FillReceived)
	r.Event.ExecID = exec
	r.Event.Qty = fixed.MustParse(qty)
	r.Event.Price = fixed.MustParse(price)
	return r
}

func fillEvent(id, exec string, from, to record.State, qty, price string) record.Record {
	r := event(id, from, to, record.FillReceived)
	r.Event.ExecID = exec
	r.Event.Qty = fixed.MustParse(qty)
	r.Event.Price = fixed.MustParse(price)
	return r
}

// feed folds a sequence, numbering and stamping as the journal would.
func feed(b *Book, records ...record.Record) {
	for i, r := range records {
		seq := b.LastSeq() + 1
		_ = i
		b.Apply(header(seq, 1_000+seq, r.Kind), r)
	}
}

func TestAnOrderIsFoldedFromItsRecords(t *testing.T) {
	b := New()
	feed(b,
		intent("UI-1", record.Buy, "4", record.Entry),
		event("UI-1", record.Created, record.Submitting, record.SubmissionStarted),
		event("UI-1", record.Submitting, record.Working, record.VenueAcked),
		fillEvent("UI-1", "E-1", record.Working, record.Partial, "1", "100"),
		fillEvent("UI-1", "E-2", record.Partial, record.Filled, "3", "104"),
	)

	order, ok := b.Order("UI-1")
	if !ok {
		t.Fatal("the order is missing")
	}
	if order.State != "FILLED" {
		t.Errorf("state = %s", order.State)
	}
	if order.FilledQty != fixed.Whole(4) {
		t.Errorf("filled = %v", order.FilledQty)
	}
	if !order.RemainingQty.IsZero() {
		t.Errorf("remaining = %v", order.RemainingQty)
	}
	if got := b.PositionIn("BTCUSD").Qty; got != fixed.Whole(4) {
		t.Errorf("position = %v", got)
	}
	if got := b.PositionIn("BTCUSD").CostBasis.String(); got != "412" {
		t.Errorf("cost basis = %s", got)
	}
	if b.Fills() != 2 {
		t.Errorf("fills = %d", b.Fills())
	}
}

func TestTheStateIsTranscribedNotDerived(t *testing.T) {
	// The record says where the order went. This package has no transition
	// function and must not need one — so a state it could never compute is
	// still shown correctly.
	b := New()
	feed(b,
		intent("UI-1", record.Buy, "1", record.Entry),
		event("UI-1", record.Created, record.Submitting, record.SubmissionStarted),
		event("UI-1", record.Submitting, record.Unknown, record.SubmissionTimedOut),
	)
	order, _ := b.Order("UI-1")
	if order.State != "UNKNOWN" {
		t.Errorf("state = %s", order.State)
	}
	if !order.LocksInstrument() {
		t.Error("an unprovable order should hold its instrument")
	}
}

func TestAFillBookedWhileReconcilingMovesThePosition(t *testing.T) {
	// The bug this pins: a fill applied during reconciliation leaves the order
	// in Reconciling, so From == To. Inferring "nothing happened" from that
	// dropped real exposure from the projection while the engine had it — the
	// projection under-reported a position, silently, in a display, about
	// money. It is the flag that decides, not the states.
	b := New()
	feed(b,
		intent("UI-1", record.Buy, "2", record.Entry),
		event("UI-1", record.Created, record.Submitting, record.SubmissionStarted),
		event("UI-1", record.Submitting, record.Unknown, record.SubmissionTimedOut),
		event("UI-1", record.Unknown, record.Reconciling, record.ReconcileStarted),
		fillEvent("UI-1", "RCN-1", record.Reconciling, record.Reconciling, "2", "100"),
	)
	if got := b.PositionIn("BTCUSD").Qty; got != fixed.Whole(2) {
		t.Errorf("position = %v, want 2", got)
	}
	if b.Fills() != 1 {
		t.Errorf("fills = %d, want 1", b.Fills())
	}
}

func TestEvidenceThatWasNotAppliedMovesNothing(t *testing.T) {
	// A fill for an order believed terminal is recorded with applied clear.
	// The engine will reconcile it; booking it here would show a position the
	// engine does not have. Note it is the flag that says so, not From == To —
	// an applied fill mid-reconcile has From == To as well.
	b := New()
	feed(b,
		intent("UI-1", record.Buy, "2", record.Entry),
		event("UI-1", record.Created, record.Submitting, record.SubmissionStarted),
		event("UI-1", record.Submitting, record.Working, record.VenueAcked),
		fillEvent("UI-1", "E-1", record.Working, record.Filled, "2", "100"),
		evidenceFill("UI-1", "E-late", record.Filled, "1", "101"),
	)
	if got := b.PositionIn("BTCUSD").Qty; got != fixed.Whole(2) {
		t.Errorf("position = %v, want 2", got)
	}
	if b.Fills() != 1 {
		t.Errorf("fills = %d, want 1", b.Fills())
	}
	// But it is visible in the activity log: not applied is not the same as
	// not happened.
	activity := b.Activity()
	last := activity[len(activity)-1]
	if last.From != last.To {
		t.Errorf("the late fill should show from == to, got %s -> %s", last.From, last.To)
	}
}

func TestADuplicateExecutionMovesNothing(t *testing.T) {
	b := New()
	feed(b,
		intent("UI-1", record.Buy, "4", record.Entry),
		event("UI-1", record.Created, record.Submitting, record.SubmissionStarted),
		event("UI-1", record.Submitting, record.Working, record.VenueAcked),
		fillEvent("UI-1", "E-1", record.Working, record.Partial, "2", "100"),
		fillEvent("UI-1", "E-1", record.Partial, record.Filled, "2", "100"),
	)
	if got := b.PositionIn("BTCUSD").Qty; got != fixed.Whole(2) {
		t.Errorf("position = %v, want 2", got)
	}
}

func TestAReplayedIntentCreatesNoSecondOrder(t *testing.T) {
	b := New()
	feed(b,
		intent("UI-1", record.Buy, "3", record.Entry),
		intent("UI-1", record.Buy, "3", record.Entry),
	)
	if len(b.Orders()) != 1 {
		t.Errorf("orders = %d, want 1", len(b.Orders()))
	}
}

func TestAHaltIsVisibleAndReversible(t *testing.T) {
	b := New()
	if b.IsHalted() {
		t.Error("a fresh book should not be halted")
	}
	feed(b, record.Record{
		Kind: journal.KindDecision,
		Decision: &record.Decision{
			Instrument: "", Actor: 6, Kind: record.Halted, Note: "writer-lock lost",
		},
	})
	if !b.IsHalted() {
		t.Error("the halt was not seen")
	}
	feed(b, record.Record{
		Kind:     journal.KindDecision,
		Decision: &record.Decision{Actor: 6, Kind: record.Resumed},
	})
	if b.IsHalted() {
		t.Error("the resume was not seen")
	}
	if len(b.Decisions()) != 2 {
		t.Errorf("decisions = %d", len(b.Decisions()))
	}
}

func TestTheRingsAreBounded(t *testing.T) {
	// This projection is folded from a log that grows forever. A display that
	// grows with it is a leak with a chart on top.
	b := New()
	for i := 0; i < decisionRing*3; i++ {
		feed(b, record.Record{
			Kind:     journal.KindDecision,
			Decision: &record.Decision{Actor: 1, Kind: record.EntryBlocked, Note: "busy"},
		})
	}
	if len(b.Decisions()) != decisionRing {
		t.Errorf("decisions = %d, want %d", len(b.Decisions()), decisionRing)
	}
}

func TestListingsAreStableAcrossReads(t *testing.T) {
	b := New()
	for _, id := range []string{"UI-3", "UI-1", "UI-2"} {
		feed(b, intent(id, record.Buy, "1", record.Entry))
	}
	first := b.Orders()
	for range 5 {
		again := b.Orders()
		for i := range first {
			if again[i].ClientOrderID != first[i].ClientOrderID {
				t.Fatalf("listing reshuffled: %v then %v", first[i], again[i])
			}
		}
	}
	// Insertion order, not sorted order — the order the intents were written.
	if first[0].ClientOrderID != "UI-3" {
		t.Errorf("first = %s, want UI-3", first[0].ClientOrderID)
	}
}

func TestAnUnknownRecordIsSkippedRatherThanFatal(t *testing.T) {
	// A gateway that refuses to serve because it met a record kind from a newer
	// engine turns a forward-compatible change into an outage.
	b := New()
	feed(b, intent("UI-1", record.Buy, "1", record.Entry))
	b.Apply(header(99, 9_000, journal.Kind(250)), record.Record{Kind: journal.Kind(250)})
	if len(b.Orders()) != 1 {
		t.Errorf("the book lost its order")
	}
	if b.LastSeq() != 99 {
		t.Errorf("last seq = %d", b.LastSeq())
	}
}

func TestPnLAddsUpAcrossInstruments(t *testing.T) {
	b := New()
	feed(b,
		intent("UI-1", record.Buy, "2", record.Entry),
		event("UI-1", record.Created, record.Submitting, record.SubmissionStarted),
		event("UI-1", record.Submitting, record.Working, record.VenueAcked),
		fillEvent("UI-1", "E-1", record.Working, record.Filled, "2", "100"),
		intent("SL-1", record.Sell, "1", record.ProtectiveExit),
		event("SL-1", record.Created, record.Submitting, record.SubmissionStarted),
		event("SL-1", record.Submitting, record.Working, record.VenueAcked),
		fillEvent("SL-1", "E-2", record.Working, record.Filled, "1", "110"),
		record.Record{Kind: journal.KindMark, MarkInstrument: "BTCUSD", MarkPrice: fixed.MustParse("120")},
	)
	if got := b.Realized().String(); got != "10" {
		t.Errorf("realized = %s, want 10", got)
	}
	if got := b.Unrealized().String(); got != "20" {
		t.Errorf("unrealized = %s, want 20", got)
	}
}
