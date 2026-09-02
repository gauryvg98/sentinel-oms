// Package record decodes the journal's payloads.
//
// The Go half of the format contract. docs/rewrite/JOURNAL-FORMAT.md is the
// specification and testdata/journal-golden is what pins it: Rust writes that
// fixture and both sides read it, which is the only thing that keeps a
// two-language format from becoming two formats.
//
// Decode only. There is exactly one writer and it is in Rust; an encoder here
// would be a second implementation of the layout, and two implementations of a
// layout is one layout and one bug waiting to be found.
//
// Note what this package does *not* contain: an order state machine. Every
// lifecycle record carries the state the order was in and the state it moved
// to, written by the engine that computed them. Re-deriving those here would
// mean a second copy of the transition rules, and it would be wrong silently,
// in a display, about money.
package record

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/gauryvg98/sentinel-oms/go/internal/fixed"
	"github.com/gauryvg98/sentinel-oms/go/internal/journal"
)

// Errors decoding a payload.
var (
	// ErrTruncated means the payload ended before the record did.
	ErrTruncated = errors.New("record payload is truncated")
	// ErrTrailing means bytes remained after the record was fully decoded —
	// a record from a different version of the format wearing the same kind.
	ErrTrailing = errors.New("bytes remain after the record")
	// ErrDiscriminant means a byte is not a value of its type, zero included.
	ErrDiscriminant = errors.New("not a valid discriminant")
	// ErrText means padded text was not a valid inline string.
	ErrText = errors.New("padded text is not valid")
)

// Side is an order's direction.
type Side uint8

// The sides. Zero is not one of them.
const (
	Buy  Side = 1
	Sell Side = 2
)

func (s Side) String() string {
	switch s {
	case Buy:
		return "BUY"
	case Sell:
		return "SELL"
	default:
		return fmt.Sprintf("Side(%d)", uint8(s))
	}
}

// Sign is +1 for a buy and -1 for a sell: the multiplier that turns an order
// quantity into a signed position delta.
func (s Side) Sign() int64 {
	if s == Sell {
		return -1
	}
	return 1
}

// Authority is whose authority an order exists under.
type Authority uint8

// The authorities.
const (
	Entry          Authority = 1
	ProtectiveExit Authority = 2
)

func (a Authority) String() string {
	switch a {
	case Entry:
		return "ENTRY"
	case ProtectiveExit:
		return "PROTECTIVE_EXIT"
	default:
		return fmt.Sprintf("Authority(%d)", uint8(a))
	}
}

// OrderKind is how the venue was asked to execute.
type OrderKind uint8

// The kinds.
const (
	Market     OrderKind = 1
	Limit      OrderKind = 2
	StopMarket OrderKind = 3
)

func (k OrderKind) String() string {
	switch k {
	case Market:
		return "MARKET"
	case Limit:
		return "LIMIT"
	case StopMarket:
		return "STOP_MARKET"
	default:
		return fmt.Sprintf("OrderKind(%d)", uint8(k))
	}
}

// State is where an order is in its life.
type State uint8

// The states, matching sentinel-domain byte for byte.
const (
	Created       State = 1
	Submitting    State = 2
	Working       State = 3
	Partial       State = 4
	CancelPending State = 5
	Unknown       State = 6
	Reconciling   State = 7
	Filled        State = 8
	Canceled      State = 9
	Rejected      State = 10
)

var stateNames = map[State]string{
	Created: "CREATED", Submitting: "SUBMITTING", Working: "WORKING",
	Partial: "PARTIAL", CancelPending: "CANCEL_PENDING", Unknown: "UNKNOWN",
	Reconciling: "RECONCILING", Filled: "FILLED", Canceled: "CANCELED",
	Rejected: "REJECTED",
}

func (s State) String() string {
	if name, ok := stateNames[s]; ok {
		return name
	}
	return fmt.Sprintf("State(%d)", uint8(s))
}

// Valid reports whether this is a state we know.
func (s State) Valid() bool {
	_, ok := stateNames[s]
	return ok
}

// IsTerminal reports whether the order is finished, absent new evidence.
func (s State) IsTerminal() bool {
	return s == Filled || s == Canceled || s == Rejected
}

// LocksInstrument reports whether this state blocks new entries on the
// instrument — the states whose truth cannot be proven.
func (s State) LocksInstrument() bool {
	return s == Unknown || s == Reconciling
}

// EventKind is which lifecycle event a record holds.
type EventKind uint8

// The event kinds.
const (
	SubmissionStarted  EventKind = 1
	VenueAcked         EventKind = 2
	VenueRejected      EventKind = 3
	SubmissionTimedOut EventKind = 4
	FillReceived       EventKind = 5
	CancelRequested    EventKind = 6
	CancelConfirmed    EventKind = 7
	ReconcileStarted   EventKind = 8
	ReconcileResolved  EventKind = 9
)

var eventNames = map[EventKind]string{
	SubmissionStarted: "SUBMISSION_STARTED", VenueAcked: "VENUE_ACKED",
	VenueRejected: "VENUE_REJECTED", SubmissionTimedOut: "SUBMISSION_TIMED_OUT",
	FillReceived: "FILL_APPLIED", CancelRequested: "CANCEL_REQUESTED",
	CancelConfirmed: "CANCEL_CONFIRMED", ReconcileStarted: "RECONCILE_STARTED",
	ReconcileResolved: "RECONCILE_RESOLVED",
}

func (k EventKind) String() string {
	if name, ok := eventNames[k]; ok {
		return name
	}
	return fmt.Sprintf("EventKind(%d)", uint8(k))
}

// Actor is who made a decision.
type Actor uint8

var actorNames = map[Actor]string{
	1: "gateway", 2: "guards", 3: "reconciler", 4: "protection",
	5: "strategy", 6: "supervisor", 7: "sizing",
}

func (a Actor) String() string {
	if name, ok := actorNames[a]; ok {
		return name
	}
	return fmt.Sprintf("Actor(%d)", uint8(a))
}

// DecisionKind is what was decided.
type DecisionKind uint8

// The decision kinds that change how the account behaves.
const (
	EntryPlaced        DecisionKind = 1
	EntryBlocked       DecisionKind = 2
	ExitPlaced         DecisionKind = 3
	ExitClamped        DecisionKind = 4
	SizingComputed     DecisionKind = 5
	ReconcileScheduled DecisionKind = 6
	Halted             DecisionKind = 7
	Resumed            DecisionKind = 8
)

var decisionNames = map[DecisionKind]string{
	EntryPlaced: "ENTRY_PLACED", EntryBlocked: "ENTRY_BLOCKED",
	ExitPlaced: "EXIT_PLACED", ExitClamped: "EXIT_CLAMPED",
	SizingComputed: "SIZING_COMPUTED", ReconcileScheduled: "RECONCILE_SCHEDULED",
	Halted: "HALTED", Resumed: "RESUMED",
}

func (d DecisionKind) String() string {
	if name, ok := decisionNames[d]; ok {
		return name
	}
	return fmt.Sprintf("DecisionKind(%d)", uint8(d))
}

// Intent is an authorised order, durable before the venue saw it.
type Intent struct {
	ClientOrderID   string
	Instrument      string
	Side            Side
	Kind            OrderKind
	Authority       Authority
	Qty             fixed.Dec
	LimitPrice      *fixed.Dec
	StopPrice       *fixed.Dec
	QuoteAtDecision *fixed.Dec
	TraceID         string
}

// Event is one lifecycle transition, with the states the engine computed.
type Event struct {
	ClientOrderID string
	TraceID       string
	From          State
	To            State
	// IsApplied is whether the ledger booked this event or only recorded it.
	// Read from the record rather than inferred: an applied fill can leave the
	// order where it found it, so From == To proves nothing.
	IsApplied bool
	Kind      EventKind

	// Set for VenueAcked and ReconcileResolved.
	VenueOrderID string
	// Set for VenueRejected.
	RejectCode int32
	RejectText string
	// Set for FillReceived.
	ExecID string
	Qty    fixed.Dec
	Price  fixed.Dec
	// Set for ReconcileStarted.
	Cause uint8
	// Set for ReconcileResolved.
	ResolvedState State
	FilledQty     fixed.Dec
}

// Applied reports whether the ledger booked this event, as opposed to
// recording it as evidence that could not be applied.
//
// This was once `From != To`, which is wrong for the case that matters most:
// a fill booked while an order is reconciling — including the fill that adopts
// an inherited position — moves the position without moving the state. Reading
// it that way silently under-reported exposure in the projection while the
// engine had it right.
func (e Event) Applied() bool { return e.IsApplied }

// Decision is why something happened, or did not.
type Decision struct {
	Instrument string
	Actor      Actor
	Kind       DecisionKind
	TraceID    string
	Value      fixed.Dec
	Note       string
}

// Record is anything the journal can hold.
//
// Exactly one field is set, chosen by Kind.
type Record struct {
	Kind journal.Kind

	Epoch       uint64
	ResumedFrom uint64

	Intent   *Intent
	Event    *Event
	Decision *Decision

	// Set for Mark.
	MarkInstrument string
	MarkPrice      fixed.Dec
	// Set for Balance.
	Asset  string
	Amount fixed.Dec
}

// Decode reads one payload, given the kind the frame header carried.
//
// A payload that decodes with bytes left over is refused: trailing bytes mean
// this is a record from a different version of the format wearing the same kind
// byte, and guessing which fields it shares is how a log stops meaning what it
// says.
func Decode(kind journal.Kind, payload []byte) (Record, error) {
	r := &reader{buf: payload}
	out := Record{Kind: kind}

	switch kind {
	case journal.KindEpochClaimed:
		out.Epoch = r.u64()
		out.ResumedFrom = r.u64()
	case journal.KindIntentPersisted:
		intent, err := decodeIntent(r)
		if err != nil {
			return Record{}, err
		}
		out.Intent = intent
	case journal.KindOrderEvent:
		event, err := decodeEvent(r)
		if err != nil {
			return Record{}, err
		}
		out.Event = event
	case journal.KindDecision:
		decision, err := decodeDecision(r)
		if err != nil {
			return Record{}, err
		}
		out.Decision = decision
	case journal.KindMark:
		out.MarkInstrument = r.text(16)
		out.MarkPrice = fixed.FromRaw(r.i64())
	case journal.KindBalance:
		out.Asset = r.text(8)
		out.Amount = fixed.FromRaw(r.i64())
	case journal.KindTicked:
		// No payload at all. The instant is in the frame header.
	default:
		return Record{}, fmt.Errorf("%w: record kind %d", ErrDiscriminant, uint16(kind))
	}

	if r.err != nil {
		return Record{}, r.err
	}
	if r.at != len(r.buf) {
		return Record{}, fmt.Errorf("%w: %d left", ErrTrailing, len(r.buf)-r.at)
	}
	return out, nil
}

func decodeIntent(r *reader) (*Intent, error) {
	intent := &Intent{
		ClientOrderID: r.text(36),
		Instrument:    r.text(16),
		Side:          Side(r.u8()),
		Kind:          OrderKind(r.u8()),
		Authority:     Authority(r.u8()),
		Qty:           fixed.FromRaw(r.i64()),
	}
	intent.LimitPrice = r.optPrice()
	intent.StopPrice = r.optPrice()
	intent.QuoteAtDecision = r.optPrice()
	intent.TraceID = r.trace()
	if r.err != nil {
		return nil, r.err
	}
	if intent.Side != Buy && intent.Side != Sell {
		return nil, fmt.Errorf("%w: side %d", ErrDiscriminant, uint8(intent.Side))
	}
	if intent.Authority != Entry && intent.Authority != ProtectiveExit {
		return nil, fmt.Errorf("%w: authority %d", ErrDiscriminant, uint8(intent.Authority))
	}
	return intent, nil
}

func decodeEvent(r *reader) (*Event, error) {
	event := &Event{
		ClientOrderID: r.text(36),
		TraceID:       r.trace(),
		From:          State(r.u8()),
		To:            State(r.u8()),
		IsApplied:     r.u8() != 0,
		Kind:          EventKind(r.u8()),
	}
	if r.err != nil {
		return nil, r.err
	}
	if !event.From.Valid() || !event.To.Valid() {
		return nil, fmt.Errorf("%w: states %d -> %d", ErrDiscriminant, event.From, event.To)
	}

	switch event.Kind {
	case SubmissionStarted, SubmissionTimedOut, CancelRequested, CancelConfirmed:
		// Nothing more. In particular a timeout carries no field from which a
		// reader could form an opinion about what the venue actually did.
	case VenueAcked:
		event.VenueOrderID = r.text(32)
	case VenueRejected:
		event.RejectCode = r.i32()
		event.RejectText = r.text(64)
	case FillReceived:
		event.ExecID = r.text(40)
		event.Qty = fixed.FromRaw(r.i64())
		event.Price = fixed.FromRaw(r.i64())
	case ReconcileStarted:
		event.Cause = r.u8()
	case ReconcileResolved:
		event.ResolvedState = State(r.u8())
		present := r.u8()
		id := r.text(32)
		if present == 1 {
			event.VenueOrderID = id
		} else if present != 0 {
			return nil, fmt.Errorf("%w: flag %d", ErrDiscriminant, present)
		}
		event.FilledQty = fixed.FromRaw(r.i64())
	default:
		return nil, fmt.Errorf("%w: event kind %d", ErrDiscriminant, uint8(event.Kind))
	}
	if r.err != nil {
		return nil, r.err
	}
	return event, nil
}

func decodeDecision(r *reader) (*Decision, error) {
	decision := &Decision{
		Instrument: r.text(16),
		Actor:      Actor(r.u8()),
		Kind:       DecisionKind(r.u8()),
		TraceID:    r.trace(),
		Value:      fixed.FromRaw(r.i64()),
		Note:       r.text(96),
	}
	if r.err != nil {
		return nil, r.err
	}
	return decision, nil
}

// reader walks a payload, remembering the first thing that went wrong.
//
// Errors are accumulated rather than returned per field: a decoder written with
// a check after every read is a decoder where one missing check is invisible.
type reader struct {
	buf []byte
	at  int
	err error
}

func (r *reader) take(n int) []byte {
	if r.err != nil {
		return make([]byte, n)
	}
	if len(r.buf)-r.at < n {
		r.err = fmt.Errorf("%w: wanted %d, had %d", ErrTruncated, n, len(r.buf)-r.at)
		return make([]byte, n)
	}
	slice := r.buf[r.at : r.at+n]
	r.at += n
	return slice
}

func (r *reader) u8() uint8   { return r.take(1)[0] }
func (r *reader) i32() int32  { return int32(binary.LittleEndian.Uint32(r.take(4))) }
func (r *reader) i64() int64  { return int64(binary.LittleEndian.Uint64(r.take(8))) }
func (r *reader) u64() uint64 { return binary.LittleEndian.Uint64(r.take(8)) }

// text reads padded inline text of exactly n bytes.
func (r *reader) text(n int) string {
	raw := r.take(n)
	end := len(raw)
	for i, b := range raw {
		if b == 0 {
			end = i
			break
		}
	}
	// A zero byte followed by a non-zero byte is not padding — it is a corrupt
	// record claiming to be a short string.
	for _, b := range raw[end:] {
		if b != 0 && r.err == nil {
			r.err = ErrText
			return ""
		}
	}
	return string(raw[:end])
}

// trace reads a 16-byte trace id and renders it as hex.
func (r *reader) trace() string {
	raw := r.take(16)
	// Little-endian on the wire, printed most-significant first so it reads
	// the same way the Rust side prints it.
	var out [32]byte
	const hexDigits = "0123456789abcdef"
	for i := 0; i < 16; i++ {
		b := raw[15-i]
		out[i*2] = hexDigits[b>>4]
		out[i*2+1] = hexDigits[b&0x0f]
	}
	return string(out[:])
}

// optPrice reads a flag byte and a value, and returns nil when absent.
func (r *reader) optPrice() *fixed.Dec {
	present := r.u8()
	value := fixed.FromRaw(r.i64())
	if present == 1 {
		return &value
	}
	if present != 0 && r.err == nil {
		r.err = fmt.Errorf("%w: flag %d", ErrDiscriminant, present)
	}
	return nil
}
