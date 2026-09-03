// Package gateway serves the journal to browsers and forwards commands to the
// writer.
//
// Reads and writes take different paths on purpose.
//
// **Reads** come from the journal. The gateway tails it, folds a projection,
// and streams changes. It never asks the engine anything — the log is an
// ordered stream with a cursor per reader, which is the abstraction a
// request-response API would be built on top of anyway. So a browser watching
// this cannot slow the writer down, whatever it does.
//
// **Writes** go to the control socket, which is a Unix socket beside the
// journal with four operations on it. They are forwarded, not interpreted:
// every guard that decides whether an order may exist lives in the engine, and
// a second opinion here would be a second thing to keep in step.
//
// Server-sent events rather than a websocket. The stream is one-way — commands
// are ordinary POSTs — and SSE needs no library, survives proxies that mangle
// upgrades, and reconnects in the browser without anyone writing that code.
package gateway

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/gauryvg98/sentinel-oms/go/internal/bars"
	"github.com/gauryvg98/sentinel-oms/go/internal/book"
	"github.com/gauryvg98/sentinel-oms/go/internal/fanout"
	"github.com/gauryvg98/sentinel-oms/go/internal/fixed"
	"github.com/gauryvg98/sentinel-oms/go/internal/journal"
	"github.com/gauryvg98/sentinel-oms/go/internal/record"
)

// Config is where the gateway reads and writes.
type Config struct {
	// JournalDir is the log to tail. The control socket sits beside it.
	JournalDir string
	// AdminToken unlocks the write endpoints. Empty means the gateway is
	// read-only for everyone, which is what a public deployment wants.
	AdminToken string
	// Addr to listen on.
	Addr string
	// Leverage the venue was told to use, for display. The gateway has no say
	// in it — it reads the same variable sentineld does, so a screen showing
	// 100x is showing what the writer actually set.
	Leverage string
	// BarInterval is the candle size the chart draws.
	//
	// It follows the strategy's interval rather than being a display choice of
	// its own. The chart overlays the same two moving averages the strategy
	// trades, and averages computed over a different bar size would show
	// crossings that never happened — a chart explaining trades with arithmetic
	// nobody ran. Zero means one minute.
	BarInterval time.Duration
}

// Gateway serves the journal.
type Gateway struct {
	config    Config
	transport *fanout.RingTransport

	mu   sync.RWMutex
	book *book.Book
	// series is the candles per instrument, folded from the same marks the
	// book sees. Guarded by the same lock: it is part of the projection.
	series map[string]*bars.Series
	// caughtUp is false while folding the journal that already existed at
	// startup. Until it is true the book is a point in history, not the
	// present, and saying so is the difference between a screen that is late
	// and one that is confidently wrong.
	caughtUp bool
	// backfill fetches candle history for an instrument the first time one is
	// seen. Injected rather than dialled directly so the gateway stays
	// testable without a venue.
	backfill func(instrument string) ([]bars.Bar, error)
}

// Backfill installs the history source.
func (g *Gateway) Backfill(fn func(string) ([]bars.Bar, error)) { g.backfill = fn }

// How many candles are kept per instrument. At a minute that is six hours; at
// an hour, a fortnight. Enough either way to see the averages cross, without
// holding a year of candles for every instrument that ever traded.
const barsKept = 360

// barInterval is the configured candle size in nanoseconds.
func (g *Gateway) barInterval() uint64 {
	if g.config.BarInterval <= 0 {
		return uint64(time.Minute.Nanoseconds())
	}
	return uint64(g.config.BarInterval.Nanoseconds())
}

// New returns a gateway with an empty book.
func New(config Config) *Gateway {
	return &Gateway{
		config:    config,
		transport: fanout.NewRing(),
		book:      book.New(),
		series:    map[string]*bars.Series{},
	}
}

// Tail folds the journal forever, publishing as it goes.
//
// Blocks. Returns only when the journal cannot be read at all, which is a
// reason to stop rather than to retry: a gateway serving a projection it can no
// longer update is showing a screen that has quietly stopped being true.
func (g *Gateway) Tail(poll time.Duration) error {
	tailer, err := journal.OpenTailer(g.config.JournalDir, 0)
	if err != nil {
		return fmt.Errorf("open journal: %w", err)
	}
	defer tailer.Close()

	for {
		published := 0
		for {
			frame, err := tailer.Next()
			if err != nil {
				return fmt.Errorf("read journal: %w", err)
			}
			if frame == nil {
				break
			}
			g.fold(frame)
			published++
		}
		if !g.caughtUp {
			g.mu.Lock()
			g.caughtUp = true
			g.mu.Unlock()
		}
		if published > 0 {
			// The book is published once per drain, not once per record: it is
			// current state, so a burst of a hundred records is one snapshot's
			// worth of news.
			g.publishBook()
		}
		time.Sleep(poll)
	}
}

func (g *Gateway) fold(frame *journal.Record) {
	decoded, err := record.Decode(frame.Header.Kind, frame.Payload)
	if err != nil {
		// A record this build cannot read is skipped rather than fatal. The
		// engine is the authority; a gateway that refuses to serve because it
		// met a newer record kind turns a forward-compatible change into an
		// outage.
		log.Printf("gatewayd: skipping record %d: %v", frame.Header.Seq, err)
		return
	}

	g.mu.Lock()
	g.book.Apply(frame.Header, decoded)
	g.mu.Unlock()

	// Sequenced channels publish per record, because each one is an event that
	// happened and dropping it would lose it.
	switch decoded.Kind {
	case journal.KindOrderEvent:
		if decoded.Event.Kind == record.FillReceived && decoded.Event.Applied() {
			g.publish(fanout.Fills, map[string]any{
				"seq":             frame.Header.Seq,
				"at":              frame.Header.Nanos,
				"client_order_id": decoded.Event.ClientOrderID,
				"exec_id":         decoded.Event.ExecID,
				"qty":             decoded.Event.Qty,
				"price":           decoded.Event.Price,
			})
		}
		g.publish(fanout.Orders, map[string]any{
			"seq":             frame.Header.Seq,
			"at":              frame.Header.Nanos,
			"client_order_id": decoded.Event.ClientOrderID,
			"event":           decoded.Event.Kind.String(),
			"from":            decoded.Event.From.String(),
			"to":              decoded.Event.To.String(),
			"applied":         decoded.Event.Applied(),
		})
	case journal.KindDecision:
		g.publish(fanout.Account, map[string]any{
			"seq":        frame.Header.Seq,
			"at":         frame.Header.Nanos,
			"kind":       decoded.Decision.Kind.String(),
			"actor":      decoded.Decision.Actor.String(),
			"instrument": decoded.Decision.Instrument,
			"value":      decoded.Decision.Value,
			"note":       decoded.Decision.Note,
		})
	case journal.KindMark:
		// Candles are folded from the same records, so a chart opened now has
		// the history the log already holds rather than starting blank and
		// filling in from whatever arrives next.
		series, ok := g.series[decoded.MarkInstrument]
		if !ok {
			series = bars.NewSeries(g.barInterval(), barsKept)
			// Ask the venue for the history this journal predates. Without it
			// a chart shows only what we have watched, and a twenty-period
			// average over twenty candles is one point, which draws no line.
			if g.backfill != nil {
				if history, err := g.backfill(decoded.MarkInstrument); err != nil {
					log.Printf("gatewayd: no history for %s: %v", decoded.MarkInstrument, err)
				} else {
					series.Seed(history)
				}
			}
			g.series[decoded.MarkInstrument] = series
		}
		series.Observe(frame.Header.Nanos, decoded.MarkPrice)

		// Conflating: the newest price is the only one worth having.
		g.publish(fanout.Marks, map[string]any{
			"instrument": decoded.MarkInstrument,
			"price":      decoded.MarkPrice,
			"at":         frame.Header.Nanos,
		})
	case journal.KindBalance:
		g.publish(fanout.Account, map[string]any{
			"seq":     frame.Header.Seq,
			"at":      frame.Header.Nanos,
			"kind":    "BALANCE",
			"asset":   decoded.Asset,
			"balance": decoded.Amount,
		})
	}
}

func (g *Gateway) publish(ch fanout.Channel, payload map[string]any) {
	frame, err := json.Marshal(payload)
	if err != nil {
		return
	}
	g.transport.Publish(ch, frame)
}

func (g *Gateway) publishBook() {
	snapshot := g.Snapshot()
	frame, err := json.Marshal(snapshot)
	if err != nil {
		return
	}
	g.transport.Publish(fanout.Book, frame)
}

// State is everything a client needs to render from cold.
type State struct {
	Epoch   uint64 `json:"epoch"`
	LastSeq uint64 `json:"last_seq"`
	Now     uint64 `json:"now"`
	Halted  bool   `json:"halted"`
	// CatchingUp is true while the projection is still replaying the journal.
	// A client showing these numbers as current would be showing history.
	CatchingUp bool                 `json:"catching_up,omitempty"`
	HaltReason string               `json:"halt_reason,omitempty"`
	HaltedAt   uint64               `json:"halted_at,omitempty"`
	Orders     []*book.Order        `json:"orders"`
	Positions  []PositionView       `json:"positions"`
	Balances   map[string]fixed.Dec `json:"balances"`
	Marks      map[string]fixed.Dec `json:"marks"`
	Realized   fixed.Dec            `json:"realized"`
	Unrealized fixed.Dec            `json:"unrealized"`
	Fills      int                  `json:"fills"`
	FillList   []book.Fill          `json:"fill_list"`
	Leverage   string               `json:"leverage,omitempty"`
	Capital    Capital              `json:"capital"`
	Decisions  []book.Decision      `json:"decisions"`
	Activity   []book.Activity      `json:"activity"`
}

// Snapshot is the whole current picture.
//
// A client with nothing gets a complete one rather than a promise of one: the
// alternative is a screen that fills in as events happen to arrive, which for a
// quiet account means a screen that stays blank.
// Capital is where the money is, derived from the book.
//
// Every figure here is arithmetic over what the journal already holds — the
// wallet balance the venue reported, the marks it streamed, the positions and
// orders this system booked — rather than a second call to the venue. That
// makes it exact with respect to our own records and *not* a substitute for
// Binance's own account figures, which include funding and fees this does not
// see. Where they disagree, the venue is right and the difference is worth
// asking about.
type Capital struct {
	// Wallet is what the venue last said the balance was.
	Wallet fixed.Dec `json:"wallet"`
	// Equity is the wallet plus what open positions are currently worth.
	Equity fixed.Dec `json:"equity"`
	// Notional is the face value of what is held, unlevered.
	Notional fixed.Dec `json:"notional"`
	// Committed is margin behind open positions: notional over leverage.
	Committed fixed.Dec `json:"committed"`
	// Blocked is margin behind orders that are working but not yet filled.
	// Money that is neither free nor yet at risk.
	Blocked fixed.Dec `json:"blocked"`
	// Available is what is left to open something new with.
	Available fixed.Dec `json:"available"`
}

// capital works out where the money is.
//
// Margin is notional over leverage, which is the venue's own definition of
// initial margin. Blocked counts only orders still working: a filled order's
// margin has become committed, and counting it twice would show money that is
// not there. A protective exit is deliberately not counted as blocked — it
// releases margin rather than reserving it, and treating a stop as a claim on
// capital would make protecting a position look like risking more of it.
func (g *Gateway) capital(positions []PositionView) Capital {
	out := Capital{}
	for _, amount := range g.book.Balances() {
		out.Wallet = out.Wallet.Add(amount)
	}

	leverage := int64(1)
	if n, err := strconv.ParseInt(g.config.Leverage, 10, 64); err == nil && n > 0 {
		leverage = n
	}

	var unrealized fixed.Dec
	for _, p := range positions {
		out.Notional = out.Notional.Add(p.Qty.Abs().Mul(p.Mark))
		unrealized = unrealized.Add(p.Unrealized)
	}
	out.Committed = fixed.FromRaw(out.Notional.Raw() / leverage)

	for _, o := range g.book.LiveOrders() {
		if o.Authority == "PROTECTIVE_EXIT" {
			continue
		}
		price := g.book.Mark(o.Instrument)
		if o.LimitPrice != nil {
			price = *o.LimitPrice
		}
		notional := o.RemainingQty.Abs().Mul(price)
		out.Blocked = out.Blocked.Add(fixed.FromRaw(notional.Raw() / leverage))
	}

	out.Equity = out.Wallet.Add(unrealized)
	out.Available = out.Equity.Sub(out.Committed).Sub(out.Blocked)
	return out
}

// PositionView is a position with the two numbers a screen needs beside it.
//
// Computed here, in exact arithmetic, because the browser has none: a P&L put
// together from strings in a double is a number that disagrees with the ledger
// about money, and the disagreement would be invisible.
type PositionView struct {
	book.Position
	Mark       fixed.Dec `json:"mark"`
	Unrealized fixed.Dec `json:"unrealized"`
}

func (g *Gateway) Snapshot() State {
	g.mu.RLock()
	defer g.mu.RUnlock()

	// Every mark, not just the ones behind a position: the price is the thing
	// you look at to understand why there is no position.
	marks := g.book.Marks()

	haltReason, haltedAt := g.book.HaltReason()

	// Unrealised is attached per position here rather than left to the client.
	// The browser has no exact arithmetic, and a P&L rounded through a double
	// is a number that disagrees with the ledger about money.
	positions := make([]PositionView, 0, len(g.book.Positions()))
	for _, p := range g.book.Positions() {
		mark := g.book.Mark(p.Instrument)
		positions = append(positions, PositionView{
			Position:   p,
			Mark:       mark,
			Unrealized: p.Unrealized(mark),
		})
	}

	return State{
		Capital:    g.capital(positions),
		Epoch:      g.book.Epoch(),
		LastSeq:    g.book.LastSeq(),
		Now:        g.book.Now(),
		Halted:     g.book.IsHalted(),
		CatchingUp: !g.caughtUp,
		HaltReason: haltReason,
		HaltedAt:   haltedAt,
		Orders:     g.book.Orders(),
		Positions:  positions,
		Balances:   g.book.Balances(),
		Marks:      marks,
		Realized:   g.book.Realized(),
		Unrealized: g.book.Unrealized(),
		Fills:      g.book.Fills(),
		FillList:   g.book.FillList(),
		Leverage:   g.config.Leverage,
		Decisions:  g.book.Decisions(),
		Activity:   g.book.Activity(),
	}
}

// Handler is the HTTP surface.
func (g *Gateway) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", g.health)
	mux.HandleFunc("GET /api/state", g.state)
	mux.HandleFunc("GET /api/bars", g.bars)
	mux.HandleFunc("GET /api/stream", g.stream)
	mux.HandleFunc("POST /api/command", g.command)
	mux.Handle("GET /", http.FileServer(http.Dir("static")))
	return mux
}

// health proves the process is up, and says whether the account is trading.
//
// Deliberately 200 even when halted. A halt is an intentional safety state, and
// failing the check would make an orchestrator restart a process whose whole
// point is that it has stopped — the restart would not clear a real divergence,
// it would just hide it behind a crash loop. That mistake cost the previous
// system five days of looking healthy while refusing to trade.
func (g *Gateway) health(w http.ResponseWriter, _ *http.Request) {
	g.mu.RLock()
	halted, seq := g.book.IsHalted(), g.book.LastSeq()
	g.mu.RUnlock()

	writeJSON(w, http.StatusOK, map[string]any{
		"ok":       true,
		"halted":   halted,
		"last_seq": seq,
	})
}

// bars serves the candles for one instrument.
//
// Separate from /api/state deliberately: the book is small and read constantly,
// and candles are comparatively large and read when a chart opens. Putting them
// in one payload would make every poll carry six hours of history.
func (g *Gateway) bars(w http.ResponseWriter, r *http.Request) {
	instrument := r.URL.Query().Get("instrument")

	g.mu.RLock()
	defer g.mu.RUnlock()

	if instrument == "" {
		// No instrument named: say which ones there are, so a client does not
		// have to guess from the book.
		out := make([]string, 0, len(g.series))
		for name := range g.series {
			out = append(out, name)
		}
		sort.Strings(out)
		writeJSON(w, http.StatusOK, map[string]any{"instruments": out})
		return
	}

	series, ok := g.series[instrument]
	if !ok {
		writeJSON(w, http.StatusOK, map[string]any{
			"instrument": instrument,
			"bars":       []bars.Bar{},
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"instrument":       instrument,
		"interval_seconds": g.barInterval() / 1_000_000_000,
		"bars":             series.Bars(),
	})
}

func (g *Gateway) state(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, g.Snapshot())
}

// stream is the server-sent event feed.
func (g *Gateway) stream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	var subs []*fanout.Subscription
	for _, ch := range fanout.Channels() {
		sub := g.transport.Subscribe(ch)
		defer sub.Close()
		subs = append(subs, sub)
	}

	// A complete picture first, for the same reason /api/state exists.
	if snapshot, err := json.Marshal(g.Snapshot()); err == nil {
		fmt.Fprintf(w, "event: book\ndata: %s\n\n", snapshot)
		flusher.Flush()
	}

	// One goroutine per channel, all writing through one mutex: an
	// http.ResponseWriter is not safe for concurrent use, and a torn SSE frame
	// is a parse error in the browser rather than a missing update.
	var writeMu sync.Mutex
	done := r.Context().Done()
	frames := make(chan [2]string, 64)

	for _, sub := range subs {
		go func(sub *fanout.Subscription) {
			for {
				select {
				case <-done:
					return
				case <-sub.Resync():
					select {
					case frames <- [2]string{"resync", `{"reason":"fell behind"}`}:
					case <-done:
					}
					return
				case frame, ok := <-sub.Frames():
					if !ok {
						return
					}
					select {
					case frames <- [2]string{string(sub.Channel()), string(frame)}:
					case <-done:
						return
					}
				}
			}
		}(sub)
	}

	// A comment every fifteen seconds keeps proxies from closing an idle
	// stream. It is not a poll: it produces no data and asks no question.
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-done:
			return
		case f := <-frames:
			writeMu.Lock()
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", f[0], f[1])
			flusher.Flush()
			writeMu.Unlock()
		case <-heartbeat.C:
			writeMu.Lock()
			fmt.Fprint(w, ": keepalive\n\n")
			flusher.Flush()
			writeMu.Unlock()
		}
	}
}

// command forwards one operation to the writer.
//
// Forwarded, not interpreted. Every guard that decides whether an order may
// exist is in the engine, and a second opinion here would be a second thing to
// keep in step.
func (g *Gateway) command(w http.ResponseWriter, r *http.Request) {
	if !g.isAdmin(r) {
		writeJSON(w, http.StatusForbidden, map[string]any{
			"ok":    false,
			"error": "read-only",
		})
		return
	}

	body := make([]byte, 4096)
	n, _ := r.Body.Read(body)
	line := body[:n]
	if !json.Valid(line) {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"ok":    false,
			"error": "not json",
		})
		return
	}

	answer, err := g.forward(line)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{
			"ok":    false,
			"error": err.Error(),
		})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(answer)
}

// forward sends one line to the control socket and reads one line back.
func (g *Gateway) forward(line []byte) ([]byte, error) {
	socket := g.config.JournalDir + "/control.sock"
	conn, err := net.DialTimeout("unix", socket, 2*time.Second)
	if err != nil {
		return nil, fmt.Errorf("the writer is not listening: %w", err)
	}
	defer conn.Close()

	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write(append(line, '\n')); err != nil {
		return nil, err
	}
	answer := make([]byte, 4096)
	n, err := conn.Read(answer)
	if err != nil {
		return nil, err
	}
	return answer[:n], nil
}

// isAdmin reports whether this request may change anything.
//
// An empty token means nobody can, which is what a public deployment wants: the
// safe configuration is the one you get by not configuring anything.
func (g *Gateway) isAdmin(r *http.Request) bool {
	if g.config.AdminToken == "" {
		return false
	}
	supplied := r.Header.Get("X-Sentinel-Token")
	if supplied == "" {
		if cookie, err := r.Cookie("sentinel_admin"); err == nil {
			supplied = cookie.Value
		}
	}
	return constantTimeEqual(supplied, g.config.AdminToken)
}

// constantTimeEqual compares without leaking the answer in its timing.
func constantTimeEqual(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	var diff byte
	for i := range len(a) {
		diff |= a[i] ^ b[i]
	}
	return diff == 0
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
