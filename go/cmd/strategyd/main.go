// Command strategyd runs a strategy against the live journal.
//
// It tails the same log the gateway does, aggregates marks into bars, asks the
// strategy what stance it wants, and reconciles the actual position to it by
// posting to the control socket — the same door a human uses, so it faces
// exactly the guards a human does. It holds no state the journal does not
// already have, so it can be killed and restarted at any point: it rebuilds the
// book, replays the bars it can see, and carries on.
//
// It is deliberately outside the writer. Alpha is the part of a trading system
// most likely to be wrong, and it has no business sharing a process with the
// thing that owns the order log.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gauryvg98/sentinel-oms/go/internal/book"
	"github.com/gauryvg98/sentinel-oms/go/internal/fixed"
	"github.com/gauryvg98/sentinel-oms/go/internal/journal"
	"github.com/gauryvg98/sentinel-oms/go/internal/klines"
	"github.com/gauryvg98/sentinel-oms/go/internal/record"
	"github.com/gauryvg98/sentinel-oms/go/internal/strategy"
)

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v, err := strconv.Atoi(env(key, "")); err == nil {
		return v
	}
	return fallback
}

func envDec(key, fallback string) fixed.Dec {
	d, err := fixed.Parse(env(key, fallback))
	if err != nil {
		log.Fatalf("strategyd: %s: %v", key, err)
	}
	return d
}

func main() {
	log.SetFlags(0)

	journalDir := env("SENTINEL_JOURNAL", "/data/journal")
	instrument := env("SENTINEL_STRATEGY_INSTRUMENT", env("SENTINEL_INSTRUMENTS", "BTCUSDC"))
	// One instrument. A comma-separated list is the venue's business, not this
	// strategy's, and quietly trading the first of several would be worse than
	// refusing to guess.
	if strings.Contains(instrument, ",") {
		log.Fatalf("strategyd: SENTINEL_STRATEGY_INSTRUMENT must name one instrument, got %q", instrument)
	}

	interval, err := time.ParseDuration(env("SENTINEL_STRATEGY_INTERVAL", "1m"))
	if err != nil || interval <= 0 {
		log.Fatalf("strategyd: SENTINEL_STRATEGY_INTERVAL: %v", err)
	}
	fast := envInt("SENTINEL_STRATEGY_FAST", 5)
	slow := envInt("SENTINEL_STRATEGY_SLOW", 20)
	size := envDec("SENTINEL_STRATEGY_QTY", "0.002")
	// The Python sized by notional (SENTINEL_STRATEGY_USDT = 200), not by a
	// fixed quantity, so the position stayed the same money as the price moved.
	// A fixed 0.002 BTC is 160 USDC at 80k and 200 at 100k, which is a
	// different bet without anyone deciding to make one.
	budget := envDec("SENTINEL_STRATEGY_USDT", "0")
	// Size to a share of capital rather than a fixed notional: hold enough that
	// the margin behind the position is this fraction of equity. At 100x a
	// 200-notional order commits 2 USDC of a 4,300 account, which is not a
	// position, it is a rounding error.
	commitPct := envDec("SENTINEL_STRATEGY_COMMIT_PCT", "0")
	leverage := int64(1)
	if n, err := strconv.ParseInt(env("SENTINEL_LEVERAGE", "1"), 10, 64); err == nil && n > 0 {
		leverage = n
	}
	step := envDec("SENTINEL_STRATEGY_STEP", "0.001")
	allowShort := env("SENTINEL_STRATEGY_SHORT", "") == "true"

	strat, err := strategy.New(strategy.Config{
		Fast: fast, Slow: slow, Short: allowShort,
		StopFloorPct: envDec("SENTINEL_STRATEGY_STOP_FLOOR", "0.005"),
		StopCapPct:   envDec("SENTINEL_STRATEGY_STOP_CAP", "0.01"),
	})
	if err != nil {
		log.Fatalf("strategyd: %v", err)
	}

	// Dry run is the default. A process that starts trading the moment it is
	// deployed because someone forgot a flag is not a safe default, so the flag
	// says go rather than saying stop.
	live := env("SENTINEL_STRATEGY_LIVE", "") == "true"
	mode := "dry run — set SENTINEL_STRATEGY_LIVE=true to trade"
	if live {
		mode = "LIVE"
	}
	// Say what it is actually sizing on. Printing the fallback quantity while
	// sizing off a notional budget is a line that invites exactly the doubt it
	// should settle.
	sizing := "size " + size.String()
	if !budget.IsZero() {
		sizing = "sizing " + budget.String() + " notional"
	}
	log.Printf("strategyd: %s(%d/%d) on %s, %s bars, %s, stop %s-%s, %s",
		strat.Name(), fast, slow, instrument, interval, sizing,
		envDec("SENTINEL_STRATEGY_STOP_FLOOR", "0.005"),
		envDec("SENTINEL_STRATEGY_STOP_CAP", "0.01"), mode)

	r := &runner{
		journalDir: journalDir,
		instrument: instrument,
		size:       size,
		budget:     budget,
		commitPct:  commitPct,
		leverage:   leverage,
		step:       step,
		allowShort: allowShort,
		live:       live,
		strat:      strat,
		bars:       strategy.NewBars(uint64(interval.Nanoseconds())),
		book:       book.New(),
		tick:       envDec("SENTINEL_STRATEGY_TICK", "0.1"),
	}
	// Warm the averages from the venue's own history before reading a single
	// journal record. Without it the strategy averages over whatever this
	// journal has watched — twenty hours here, nothing at all on a fresh
	// deployment — and a 20-period average of 20 bars is not the indicator it
	// claims to be. The Python fetched three hundred bars for this reason.
	//
	// Not fatal if it fails: a strategy that warms slowly from live marks is
	// worse than one that starts informed, and better than one that refuses to
	// run because a public endpoint was briefly unreachable.
	seeded := 0
	history := klines.New(klines.BaseFor(os.Getenv("SENTINEL_BINANCE_ENV")))
	if fetched, err := history.Fetch(instrument, env("SENTINEL_STRATEGY_INTERVAL", "1h"), 300); err != nil {
		log.Printf("strategyd: no venue history, warming from live marks: %v", err)
	} else {
		for _, bar := range fetched {
			r.lastDecision = r.strat.OnBar(bar.C)
			r.lastClose = bar.C
			r.haveDecision = true
			seeded++
		}
		// The bar aggregator must not re-close a bucket the backfill covered.
		if len(fetched) > 0 {
			last := fetched[len(fetched)-1]
			r.bars.Observe(uint64(last.OpenTime)*1_000_000_000, last.C)
		}
		log.Printf("strategyd: warmed on %d bars of venue history, stance %s",
			seeded, r.lastDecision.Stance)
	}

	if err := r.run(); err != nil {
		log.Fatalf("strategyd: %v", err)
	}
}

type runner struct {
	journalDir string
	instrument string
	size       fixed.Dec
	budget     fixed.Dec
	commitPct  fixed.Dec
	leverage   int64
	step       fixed.Dec
	allowShort bool
	live       bool

	strat *strategy.SmaCross
	bars  *strategy.Bars
	book  *book.Book

	// caughtUp is false while folding the journal that already existed when
	// this process started.
	//
	// Those records are history. Their marks must reach the strategy — that is
	// what warms the averages up in a second instead of twenty minutes — but
	// the stances they produce are stances about the past, and acting on them
	// means placing a live order for every bar that ever closed. Replay to
	// learn; trade only on what happens next.
	caughtUp bool

	placed int
	// stopAt is where the working stop sits, so an unchanged one is not
	// re-placed every bar.
	stopAt fixed.Dec
	// tick is the venue's price grid.
	tick fixed.Dec
	// stopLevel is where the protective stop sits, whether or not the venue
	// agreed to hold it. When the venue refuses the order type — Binance Demo
	// Trading answers "Order type not supported for this endpoint" to
	// STOP_MARKET — this process watches the mark itself and exits at market
	// when the level is breached.
	//
	// A watched stop is strictly weaker than one resting at the venue: it dies
	// with this process, and it cannot fire between marks. It is not a choice
	// over a venue stop, it is what there is when the venue will not hold one,
	// and it is unambiguously better than nothing, which is what a rejected
	// order leaves behind.
	stopLevel fixed.Dec
	stopSide  string
	// venueHoldsStop is false once the venue has refused the order type, so
	// this does not re-attempt a rejection every bar.
	venueHoldsStop   bool
	venueRefusedStop bool
	// The last bar's answer, kept so a fill can be protected the moment it
	// lands rather than at the next bar. On an hourly timeframe that is the
	// difference between a stop in a second and a position naked for an hour.
	lastDecision strategy.Decision
	lastClose    fixed.Dec
	haveDecision bool
}

// run folds the journal forever, acting when a bar closes.
func (r *runner) run() error {
	tailer, err := journal.OpenTailer(r.journalDir, 0)
	if err != nil {
		return fmt.Errorf("open journal: %w", err)
	}
	defer tailer.Close()

	for {
		for {
			frame, err := tailer.Next()
			if err != nil {
				return fmt.Errorf("read journal: %w", err)
			}
			if frame == nil {
				break
			}
			r.fold(frame)
		}
		// Nothing left to read means the log that existed at startup has been
		// folded and everything from here is live.
		if !r.caughtUp {
			r.caughtUp = true
			log.Printf("strategyd: caught up with the journal, trading from here")

			// A position may already be open — this process restarts far more
			// often than an hourly bar closes, and until now the stop was only
			// ever placed on a fill or at a bar close. A deploy in between left
			// the position naked for up to an interval, which on this timeframe
			// is an hour. The replay has warmed the strategy, so its stop
			// distance is available immediately; use it.
			if held := r.book.PositionIn(r.instrument).Qty; !held.IsZero() && r.haveDecision {
				log.Printf("strategyd: resuming with %s held, checking its stop", held)
				r.maintainStop(r.lastDecision, r.lastClose, held)
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (r *runner) fold(frame *journal.Record) {
	decoded, err := record.Decode(frame.Header.Kind, frame.Payload)
	if err != nil {
		// Same rule as the gateway: the engine is the authority, and a record
		// this build cannot read is no reason to stop acting correctly on the
		// ones it can.
		return
	}
	r.book.Apply(frame.Header, decoded)

	// A fill changes the position, and a position without a stop is the thing
	// this must never leave lying around. Checked here rather than at the next
	// bar, because the next bar may be an hour away.
	if decoded.Kind == journal.KindOrderEvent && decoded.Event != nil &&
		decoded.Event.Kind == record.FillReceived && decoded.Event.Applied() && r.haveDecision {
		r.maintainStop(r.lastDecision, r.lastClose, r.book.PositionIn(r.instrument).Qty)
	}

	// The venue refusing the order type is a fact about the venue, not this
	// order: remember it, stop re-attempting, and watch the level here instead.
	if decoded.Kind == journal.KindOrderEvent && decoded.Event != nil &&
		decoded.Event.Kind == record.VenueRejected &&
		strings.HasPrefix(decoded.Event.ClientOrderID, "SL-") && !r.venueRefusedStop {
		r.venueRefusedStop = true
		log.Printf("strategyd: the venue refused a stop order (%s) — watching the level here instead",
			decoded.Event.RejectText)
	}

	if decoded.Kind != journal.KindMark || decoded.MarkInstrument != r.instrument {
		return
	}

	// Every mark, not every bar: a stop that only checked hourly would be a
	// stop in name.
	r.watchStop(decoded.MarkPrice)
	if closed, ok := r.bars.Observe(frame.Header.Nanos, decoded.MarkPrice); ok {
		r.onBar(closed)
	}
}

func (r *runner) onBar(closed fixed.Dec) {
	d := r.strat.OnBar(closed)
	r.lastDecision, r.lastClose, r.haveDecision = d, closed, true
	if d.Stance == strategy.NoOpinion {
		if r.caughtUp {
			log.Printf("strategyd: bar %s — warming up, %d/%d", closed, d.Have, d.Need)
		}
		return
	}

	desired, ok := strategy.Target(d.Stance, r.sizeAt(closed), r.allowShort)
	if !ok {
		return
	}
	desired = strategy.RoundToStep(desired, r.step)
	actual := r.book.PositionIn(r.instrument).Qty

	// Say what it wants, every bar, whether or not anything follows. Most bars
	// ask for the position already held, and those are exactly the ones the log
	// was silent about: without them, a strategy that has stopped deciding
	// looks the same as one that keeps deciding to hold.
	if r.caughtUp {
		r.declare(d, closed, desired, actual)
	}

	order, needed := strategy.Reconcile(desired, actual, r.step)
	if !needed {
		if r.caughtUp {
			log.Printf("strategyd: bar %s — %s, fast %s slow %s, holding %s",
				closed, d.Stance, d.Fast, d.Slow, actual)
		}
		r.maintainStop(d, closed, actual)
		return
	}

	if !r.caughtUp {
		// Replayed history: the book already contains whatever this bar led to
		// at the time, so there is nothing to say and nothing to do.
		return
	}

	exit := ""
	if order.Exit {
		exit = " (exit)"
	}
	log.Printf("strategyd: bar %s — %s, fast %s slow %s, %s -> %s: %s %s%s",
		closed, d.Stance, d.Fast, d.Slow, actual, desired, order.Side, order.Qty, exit)

	if !r.live || !r.caughtUp {
		return
	}
	if err := r.place(order); err != nil {
		// Not fatal. The next bar reconciles from whatever the position
		// actually is, so a refused or lost order corrects itself rather than
		// leaving this process reasoning from a fiction.
		log.Printf("strategyd: place failed: %v", err)
	}
}

// maintainStop keeps a protective stop under an open position.
//
// The strategy has computed a stop distance on every directional bar since it
// was written and nothing ever placed one, so positions ran naked. The stop is
// anchored on the slow average — the trend line a long is wrong once price
// falls back to — floored and capped into a tight band by the strategy itself.
//
// It is placed as a StopMarket under ProtectiveExit authority, which is what
// lets it run while the account is halted: R1.13 says a halt must not disarm
// the stops, because a halt that did would turn a bookkeeping problem into an
// unbounded position.
//
// Replaced rather than trailed on every bar. Cancel-then-place leaves a window
// with no protection at all, so the new one goes on first and the old is
// cancelled after — briefly two stops for the same position, which the exit
// guard bounds to the position actually held (R1.10), against a window with
// none, which nothing bounds.
func (r *runner) maintainStop(d strategy.Decision, closed, actual fixed.Dec) {
	if !r.live || !r.caughtUp {
		return
	}
	live := r.liveStops()

	if actual.IsZero() {
		// Flat: a stop under nothing is an order waiting to open a position.
		for _, id := range live {
			r.cancel(id)
		}
		r.stopAt = 0
		return
	}
	if d.StopDist.IsZero() {
		return
	}

	// Long stops below, short stops above.
	want := closed.Sub(d.StopDist)
	side := "sell"
	if actual.Raw() < 0 {
		want = closed.Add(d.StopDist)
		side = "buy"
	}
	want = fixed.FromRaw(strategy.RoundToStep(want, r.tick).Raw())

	// Only move it when it has actually moved. Re-placing an identical stop
	// every bar is churn the venue charges for and the log has to explain.
	if len(live) > 0 && want == r.stopAt {
		return
	}

	// The level is armed here regardless of what the venue says about it, so a
	// refused order downgrades the stop rather than removing it.
	r.stopLevel, r.stopSide, r.stopAt = want, side, want

	if !r.venueRefusedStop {
		if err := r.placeStop(side, actual.Abs(), want); err != nil {
			log.Printf("strategyd: could not place the stop: %v", err)
		}
	}
	for _, id := range live {
		r.cancel(id)
	}
	if r.venueRefusedStop {
		log.Printf("strategyd: watching the stop at %s (%s) — the venue will not hold one",
			want, side)
	}
	// Say so now rather than at the next bar. On an hourly timeframe a screen
	// that learns about the stop an hour late is a screen that spends an hour
	// reporting none.
	if r.haveDecision {
		r.declare(r.lastDecision, r.lastClose, want, actual)
	}
}

// watchStop fires the stop this process is holding, when the venue would not.
//
// Called on every mark. Market, not stop-market: the level has already been
// breached, so the order that gets out is the one that does not need the venue
// to accept a condition.
func (r *runner) watchStop(price fixed.Dec) {
	if !r.live || !r.caughtUp || r.stopLevel.IsZero() {
		return
	}
	held := r.book.PositionIn(r.instrument).Qty
	if held.IsZero() {
		r.stopLevel = 0
		return
	}

	breached := (r.stopSide == "sell" && price.Raw() <= r.stopLevel.Raw()) ||
		(r.stopSide == "buy" && price.Raw() >= r.stopLevel.Raw())
	if !breached {
		return
	}

	log.Printf("strategyd: stop breached at %s (level %s) — closing %s",
		price, r.stopLevel, held)
	// Disarm first. A second mark under the level before the fill lands would
	// otherwise send a second exit for a position already being closed.
	r.stopLevel = 0
	if err := r.place(strategy.Order{Side: r.stopSide, Qty: held.Abs(), Exit: true}); err != nil {
		log.Printf("strategyd: stop exit failed: %v", err)
	}
}

// sizeAt is how much to hold, at this price.
//
// A notional budget divides by the price; without one it is a fixed quantity.
// Rounded down to the venue's step by the caller, which is where a size that
// rounds to nothing becomes no trade rather than a rejection.
func (r *runner) sizeAt(price fixed.Dec) fixed.Dec {
	if price.IsZero() {
		return r.size
	}

	// A share of capital wins over a fixed notional, which wins over a fixed
	// quantity. Each is a weaker statement about intent than the one before.
	if !r.commitPct.IsZero() {
		margin := r.equity().Mul(r.commitPct)
		notional := fixed.FromRaw(margin.Raw() * r.leverage)
		return fixed.FromRaw(fixed.MulDiv(notional.Raw(), fixed.Scale, price.Raw()))
	}
	if !r.budget.IsZero() {
		return fixed.FromRaw(fixed.MulDiv(r.budget.Raw(), fixed.Scale, price.Raw()))
	}
	return r.size
}

// equity is the wallet the venue reported plus what open positions are worth.
//
// From the book, so it is the same number the dashboard shows and moves with
// the account rather than being a constant somebody set once. A position sized
// off stale equity is a position sized off someone's memory.
func (r *runner) equity() fixed.Dec {
	var total fixed.Dec
	for _, amount := range r.book.Balances() {
		total = total.Add(amount)
	}
	return total.Add(r.book.Unrealized())
}

// stopNote reports the armed level, if there is one.
func (r *runner) stopNote() string {
	if r.stopLevel.IsZero() {
		return ""
	}
	return " stop " + r.stopLevel.String()
}

// liveStops is the protective stops this strategy has working on its
// instrument, read from the book rather than remembered — a restart must find
// the stop it left behind rather than place a second one.
func (r *runner) liveStops() []string {
	var out []string
	for _, o := range r.book.LiveOrders() {
		if o.Instrument == r.instrument && o.Authority == "PROTECTIVE_EXIT" && o.StopPrice != nil {
			out = append(out, o.ClientOrderID)
		}
	}
	return out
}

// declare records the stance in the journal, through the same socket the
// orders go through. Best effort: failing to explain a decision must never stop
// the decision being acted on.
func (r *runner) declare(d strategy.Decision, closed, desired, actual fixed.Dec) {
	line, err := json.Marshal(map[string]any{
		"op":         "decide",
		"instrument": r.instrument,
		"value":      desired.String(),
		// The armed stop rides along in the stance, because a watched stop
		// exists nowhere else a reader can see it: the venue refused the order,
		// so a screen reading orders finds a rejection and reports no stop
		// while one is being held here. A display that says "none" over a live
		// stop is the same lie as one that says "none" over a live position.
		"note": fmt.Sprintf("%s bar %s fast %s slow %s holding %s%s",
			d.Stance, closed, d.Fast, d.Slow, actual, r.stopNote()),
	})
	if err != nil {
		return
	}
	if _, err := r.send(line); err != nil {
		log.Printf("strategyd: could not record the stance: %v", err)
	}
}

// placeStop puts a protective StopMarket under the position.
func (r *runner) placeStop(side string, qty, at fixed.Dec) error {
	r.placed++
	line, err := json.Marshal(map[string]any{
		"op":              "place",
		"client_order_id": fmt.Sprintf("SL-%d-%d", time.Now().Unix(), r.placed),
		"instrument":      r.instrument,
		"side":            side,
		"qty":             qty.String(),
		"authority":       "exit",
		"stop_price":      at.String(),
		"trace_id":        time.Now().UnixNano(),
	})
	if err != nil {
		return err
	}
	answer, err := r.send(line)
	if err != nil {
		return err
	}
	log.Printf("strategyd: stop %s %s at %s -> %s", side, qty, at, answer)
	return nil
}

// cancel withdraws an order, best effort. A stop that will not cancel is not a
// reason to stop trading: it is bounded by the position it can close.
func (r *runner) cancel(clientOrderID string) {
	line, err := json.Marshal(map[string]any{
		"op":              "cancel",
		"client_order_id": clientOrderID,
	})
	if err != nil {
		return
	}
	if _, err := r.send(line); err != nil {
		log.Printf("strategyd: could not cancel %s: %v", clientOrderID, err)
	}
}

func (r *runner) place(o strategy.Order) error {
	r.placed++
	authority := "entry"
	if o.Exit {
		authority = "exit"
	}
	line, err := json.Marshal(map[string]any{
		"op":              "place",
		"client_order_id": fmt.Sprintf("SMA-%d-%d", time.Now().Unix(), r.placed),
		"instrument":      r.instrument,
		"side":            o.Side,
		"qty":             o.Qty.String(),
		"authority":       authority,
		"trace_id":        time.Now().UnixNano(),
	})
	if err != nil {
		return err
	}

	answer, err := r.send(line)
	if err != nil {
		return err
	}
	log.Printf("strategyd: %s", answer)
	return nil
}

// send writes one line to the control socket and reads one line back.
func (r *runner) send(line []byte) ([]byte, error) {
	conn, err := net.DialTimeout("unix", r.journalDir+"/control.sock", 2*time.Second)
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
