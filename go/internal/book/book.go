// Package book is the gateway's read-only projection of the journal.
//
// It is not the engine's book and has no authority. The engine's is in Rust,
// folded from the same log, and it is the one that decides whether an order may
// exist. This one exists to be looked at.
//
// Almost all of it is transcription: an order's state is read from the record
// rather than derived, because the record carries the state the engine computed
// and re-deriving it would mean a second copy of the transition rules.
//
// The one exception is position accounting, which is genuinely duplicated —
// Rust computes cost basis and realised P&L to display them, and so does this.
// Two implementations of one rule is a thing to be uncomfortable about, so
// `book_test.go` runs the same scenarios as `sentinel-ledger`'s position tests
// against the same expected numbers. If they drift, that table fails.
package book

import (
	"sort"

	"github.com/gauryvg98/sentinel-oms/go/internal/fixed"
	"github.com/gauryvg98/sentinel-oms/go/internal/journal"
	"github.com/gauryvg98/sentinel-oms/go/internal/record"
)

// Order is what the gateway shows about one order.
type Order struct {
	ClientOrderID string     `json:"client_order_id"`
	Instrument    string     `json:"instrument"`
	Side          string     `json:"side"`
	Authority     string     `json:"authority"`
	Kind          string     `json:"kind"`
	State         string     `json:"state"`
	Qty           fixed.Dec  `json:"qty"`
	FilledQty     fixed.Dec  `json:"filled_qty"`
	RemainingQty  fixed.Dec  `json:"remaining_qty"`
	VenueOrderID  string     `json:"venue_order_id,omitempty"`
	LimitPrice    *fixed.Dec `json:"limit_price,omitempty"`
	StopPrice     *fixed.Dec `json:"stop_price,omitempty"`
	TraceID       string     `json:"trace_id"`
	CreatedAt     uint64     `json:"created_at"`
	UpdatedAt     uint64     `json:"updated_at"`

	state record.State
	side  record.Side
	exit  bool
}

// IsTerminal reports whether the order is finished, absent new evidence.
func (o *Order) IsTerminal() bool { return o.state.IsTerminal() }

// LocksInstrument reports whether this order holds its instrument against new
// entries — the states whose truth cannot be proven.
func (o *Order) LocksInstrument() bool { return o.state.LocksInstrument() }

// Fill is one execution.
type Fill struct {
	ExecID        string    `json:"exec_id"`
	ClientOrderID string    `json:"client_order_id"`
	Instrument    string    `json:"instrument"`
	Side          string    `json:"side"`
	Qty           fixed.Dec `json:"qty"`
	Price         fixed.Dec `json:"price"`
	At            uint64    `json:"at"`
}

// Position is what is held in one instrument.
type Position struct {
	Instrument string    `json:"instrument"`
	Qty        fixed.Dec `json:"qty"`
	CostBasis  fixed.Dec `json:"cost_basis"`
	Realized   fixed.Dec `json:"realized"`
}

// AvgEntry is the average entry price, or zero when flat.
func (p Position) AvgEntry() fixed.Dec {
	if p.Qty.IsZero() {
		return 0
	}
	// Both are signed the same way, so the quotient is positive.
	return fixed.FromRaw(fixed.MulDiv(p.CostBasis.Raw(), fixed.Scale, p.Qty.Raw()))
}

// Unrealized is the profit and loss at mark.
func (p Position) Unrealized(mark fixed.Dec) fixed.Dec {
	if p.Qty.IsZero() {
		return 0
	}
	return mark.Mul(p.Qty).Sub(p.CostBasis)
}

// Decision is why something happened, or did not.
type Decision struct {
	Seq        uint64    `json:"seq"`
	At         uint64    `json:"at"`
	Instrument string    `json:"instrument"`
	Actor      string    `json:"actor"`
	Kind       string    `json:"kind"`
	Value      fixed.Dec `json:"value"`
	Note       string    `json:"note"`
	TraceID    string    `json:"trace_id"`
}

// Activity is one lifecycle event, for the log a person reads.
type Activity struct {
	Seq           uint64 `json:"seq"`
	At            uint64 `json:"at"`
	ClientOrderID string `json:"client_order_id"`
	Event         string `json:"event"`
	From          string `json:"from"`
	To            string `json:"to"`
	TraceID       string `json:"trace_id"`
}

// How many of each ring the gateway keeps. Bounded: this projection is folded
// from a log that grows forever, and a display that grows with it is a leak
// with a chart on top.
const (
	decisionRing = 200
	activityRing = 200
)

// Book is the projection.
type Book struct {
	orders    map[string]*Order
	orderSeq  []string
	fills     map[string]Fill
	positions map[string]*Position
	marks     map[string]fixed.Dec
	balances  map[string]fixed.Dec

	decisions []Decision
	activity  []Activity

	epoch   uint64
	lastSeq uint64
	now     uint64
	halted  bool
}

// New returns an empty book.
func New() *Book {
	return &Book{
		orders:    map[string]*Order{},
		fills:     map[string]Fill{},
		positions: map[string]*Position{},
		marks:     map[string]fixed.Dec{},
		balances:  map[string]fixed.Dec{},
	}
}

// Apply folds one record.
//
// Never fails. This projection has no authority, so a record it cannot make
// sense of is skipped rather than escalated: a gateway that refuses to serve
// because it met a record kind from a newer engine is a gateway that turns a
// forward-compatible change into an outage.
func (b *Book) Apply(header journal.Header, r record.Record) {
	b.lastSeq = header.Seq
	b.epoch = header.Epoch
	if header.Nanos > b.now {
		b.now = header.Nanos
	}

	switch r.Kind {
	case journal.KindEpochClaimed:
		b.epoch = r.Epoch
	case journal.KindIntentPersisted:
		b.applyIntent(header, r.Intent)
	case journal.KindOrderEvent:
		b.applyEvent(header, r.Event)
	case journal.KindDecision:
		b.applyDecision(header, r.Decision)
	case journal.KindMark:
		b.marks[r.MarkInstrument] = r.MarkPrice
	case journal.KindBalance:
		b.balances[r.Asset] = r.Amount
	case journal.KindTicked:
		// Time only.
	}
}

func (b *Book) applyIntent(header journal.Header, intent *record.Intent) {
	if intent == nil {
		return
	}
	if _, seen := b.orders[intent.ClientOrderID]; seen {
		// A replayed intent creates nothing. The engine already decided that;
		// this only has to not double it.
		return
	}
	b.orders[intent.ClientOrderID] = &Order{
		ClientOrderID: intent.ClientOrderID,
		Instrument:    intent.Instrument,
		Side:          intent.Side.String(),
		Authority:     intent.Authority.String(),
		Kind:          intent.Kind.String(),
		State:         record.Created.String(),
		Qty:           intent.Qty,
		RemainingQty:  intent.Qty,
		LimitPrice:    intent.LimitPrice,
		StopPrice:     intent.StopPrice,
		TraceID:       intent.TraceID,
		CreatedAt:     header.Nanos,
		UpdatedAt:     header.Nanos,
		state:         record.Created,
		side:          intent.Side,
		exit:          intent.Authority == record.ProtectiveExit,
	}
	b.orderSeq = append(b.orderSeq, intent.ClientOrderID)
}

func (b *Book) applyEvent(header journal.Header, event *record.Event) {
	if event == nil {
		return
	}
	b.push(&b.activity, activityRing, Activity{
		Seq:           header.Seq,
		At:            header.Nanos,
		ClientOrderID: event.ClientOrderID,
		Event:         event.Kind.String(),
		From:          event.From.String(),
		To:            event.To.String(),
		TraceID:       event.TraceID,
	})

	order, ok := b.orders[event.ClientOrderID]
	if !ok {
		return
	}

	// The state is read, not derived. The engine computed it and the ledger
	// verified it against the transition function; this is transcription.
	order.state = event.To
	order.State = event.To.String()
	order.UpdatedAt = header.Nanos

	switch event.Kind {
	case record.VenueAcked:
		order.VenueOrderID = event.VenueOrderID
	case record.FillReceived:
		if !event.Applied() {
			// Recorded as evidence and applied to nothing. The engine will
			// reconcile it; booking it here would show a position the engine
			// does not have.
			return
		}
		if _, seen := b.fills[event.ExecID]; seen {
			return
		}
		b.fills[event.ExecID] = Fill{
			ExecID:        event.ExecID,
			ClientOrderID: event.ClientOrderID,
			Instrument:    order.Instrument,
			Side:          order.Side,
			Qty:           event.Qty,
			Price:         event.Price,
			At:            header.Nanos,
		}
		order.FilledQty = order.FilledQty.Add(event.Qty)
		if order.FilledQty.Raw() > order.Qty.Raw() {
			// The engine clamps a tolerated over-match on the order while the
			// position takes the venue's real figure. Same rule here.
			order.FilledQty = order.Qty
		}
		b.position(order.Instrument).applyFill(order.side, event.Qty, event.Price)
	case record.ReconcileResolved:
		if event.VenueOrderID != "" {
			order.VenueOrderID = event.VenueOrderID
		}
		order.FilledQty = event.FilledQty
		if order.FilledQty.Raw() > order.Qty.Raw() {
			order.FilledQty = order.Qty
		}
	}
	order.RemainingQty = order.Qty.Sub(order.FilledQty)
	if order.RemainingQty.Raw() < 0 {
		order.RemainingQty = 0
	}
}

func (b *Book) applyDecision(header journal.Header, decision *record.Decision) {
	if decision == nil {
		return
	}
	switch decision.Kind {
	case record.Halted:
		b.halted = true
	case record.Resumed:
		b.halted = false
	}
	b.push(&b.decisions, decisionRing, Decision{
		Seq:        header.Seq,
		At:         header.Nanos,
		Instrument: decision.Instrument,
		Actor:      decision.Actor.String(),
		Kind:       decision.Kind.String(),
		Value:      decision.Value,
		Note:       decision.Note,
		TraceID:    decision.TraceID,
	})
}

// push appends to a bounded ring, dropping the oldest.
func push[T any](ring *[]T, limit int, item T) {
	*ring = append(*ring, item)
	if len(*ring) > limit {
		*ring = (*ring)[len(*ring)-limit:]
	}
}

func (b *Book) push(ring any, limit int, item any) {
	switch r := ring.(type) {
	case *[]Decision:
		push(r, limit, item.(Decision))
	case *[]Activity:
		push(r, limit, item.(Activity))
	}
}

func (b *Book) position(instrument string) *Position {
	p, ok := b.positions[instrument]
	if !ok {
		p = &Position{Instrument: instrument}
		b.positions[instrument] = p
	}
	return p
}

// applyFill moves the position.
//
// The duplicated part, and the reason book_test.go runs the same table as the
// Rust position tests. Three cases, and the third is the one that gets written
// wrong: a fill larger than the open position closes it and opens the other
// side, priced at this fill rather than inheriting the old basis.
func (p *Position) applyFill(side record.Side, qty, price fixed.Dec) {
	signed := fixed.FromRaw(qty.Raw() * side.Sign())
	cost := price.Mul(qty)
	signedCost := fixed.FromRaw(cost.Raw() * side.Sign())

	opening := p.Qty.IsZero() || (p.Qty.Raw() > 0) == (signed.Raw() > 0)
	if opening {
		p.Qty = p.Qty.Add(signed)
		p.CostBasis = p.CostBasis.Add(signedCost)
		return
	}

	held := p.Qty.Abs()
	closing := qty
	if closing.Raw() > held.Raw() {
		closing = held
	}
	closedCost := p.sliceOfBasis(closing, held)
	proceeds := price.Mul(closing)

	direction := int64(1)
	if p.Qty.Raw() < 0 {
		direction = -1
	}
	gross := proceeds.Sub(closedCost)
	p.Realized = p.Realized.Add(fixed.FromRaw(gross.Raw() * direction))
	p.CostBasis = p.CostBasis.Sub(fixed.FromRaw(closedCost.Raw() * direction))
	p.Qty = p.Qty.Sub(fixed.FromRaw(closing.Raw() * direction))

	if p.Qty.IsZero() {
		// Flat means flat. A residual basis on nothing is reported as running
		// P&L, so a flat account shows a number it did not earn.
		p.CostBasis = 0
	}

	flipped := qty.Sub(closing)
	if flipped.Raw() > 0 {
		opened := price.Mul(flipped)
		p.Qty = fixed.FromRaw(flipped.Raw() * side.Sign())
		p.CostBasis = fixed.FromRaw(opened.Raw() * side.Sign())
	}
}

// sliceOfBasis is the share of the basis attributable to the closed part, as a
// positive magnitude. Truncates toward zero, leaving the remainder with the
// open size.
func (p *Position) sliceOfBasis(closing, held fixed.Dec) fixed.Dec {
	if held.IsZero() {
		return 0
	}
	if closing == held {
		// Closing everything takes everything, with no rounding at all.
		return p.CostBasis.Abs()
	}
	// Multiply first: dividing first would lose the remainder before it could
	// contribute. Matches Price::notional's ordering on the Rust side, and the
	// table in book_test.go is what holds the two to it.
	return fixed.FromRaw(fixed.MulDiv(p.CostBasis.Abs().Raw(), closing.Raw(), held.Raw()))
}

// ---------------------------------------------------------------- reading

// Orders returns every order, in the order its intent was written.
func (b *Book) Orders() []*Order {
	out := make([]*Order, 0, len(b.orderSeq))
	for _, id := range b.orderSeq {
		if order, ok := b.orders[id]; ok {
			out = append(out, order)
		}
	}
	return out
}

// LiveOrders returns the orders that are not finished.
func (b *Book) LiveOrders() []*Order {
	var out []*Order
	for _, order := range b.Orders() {
		if !order.IsTerminal() {
			out = append(out, order)
		}
	}
	return out
}

// Order returns one order.
func (b *Book) Order(id string) (*Order, bool) {
	order, ok := b.orders[id]
	return order, ok
}

// Positions returns every non-flat position, sorted.
func (b *Book) Positions() []Position {
	var out []Position
	for _, p := range b.positions {
		if !p.Qty.IsZero() {
			out = append(out, *p)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Instrument < out[j].Instrument })
	return out
}

// PositionIn returns the position in one instrument.
func (b *Book) PositionIn(instrument string) Position {
	if p, ok := b.positions[instrument]; ok {
		return *p
	}
	return Position{Instrument: instrument}
}

// Mark returns the last price seen.
func (b *Book) Mark(instrument string) fixed.Dec { return b.marks[instrument] }

// Marks returns every mark the book has seen.
//
// Every one, not only those with a position behind them: a screen that shows a
// price only while you hold something cannot tell you why you are not holding
// anything.
func (b *Book) Marks() map[string]fixed.Dec {
	out := make(map[string]fixed.Dec, len(b.marks))
	for instrument, price := range b.marks {
		out[instrument] = price
	}
	return out
}

// Balances returns every balance, sorted by asset.
func (b *Book) Balances() map[string]fixed.Dec {
	out := make(map[string]fixed.Dec, len(b.balances))
	for k, v := range b.balances {
		out[k] = v
	}
	return out
}

// Realized is realised P&L across every instrument.
func (b *Book) Realized() fixed.Dec {
	var total fixed.Dec
	keys := make([]string, 0, len(b.positions))
	for k := range b.positions {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		total = total.Add(b.positions[k].Realized)
	}
	return total
}

// Unrealized is unrealised P&L at the last marks seen.
func (b *Book) Unrealized() fixed.Dec {
	var total fixed.Dec
	for _, p := range b.Positions() {
		total = total.Add(p.Unrealized(b.marks[p.Instrument]))
	}
	return total
}

// Decisions returns the recent decisions, newest last.
func (b *Book) Decisions() []Decision { return append([]Decision(nil), b.decisions...) }

// Activity returns the recent lifecycle events, newest last.
func (b *Book) Activity() []Activity { return append([]Activity(nil), b.activity...) }

// Fills returns how many executions have been seen.
func (b *Book) Fills() int { return len(b.fills) }

// Epoch is the writer epoch of the last record folded.
func (b *Book) Epoch() uint64 { return b.epoch }

// LastSeq is the last sequence folded.
func (b *Book) LastSeq() uint64 { return b.lastSeq }

// Now is journal time as of the last record.
func (b *Book) Now() uint64 { return b.now }

// IsHalted reports whether the account has stopped trading.
func (b *Book) IsHalted() bool { return b.halted }
