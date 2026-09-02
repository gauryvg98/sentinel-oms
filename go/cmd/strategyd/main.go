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
	log.Printf("strategyd: %s(%d/%d) on %s, %s bars, size %s, %s",
		strat.Name(), fast, slow, instrument, interval, size, mode)

	r := &runner{
		journalDir: journalDir,
		instrument: instrument,
		size:       size,
		step:       step,
		allowShort: allowShort,
		live:       live,
		strat:      strat,
		bars:       strategy.NewBars(uint64(interval.Nanoseconds())),
		book:       book.New(),
	}
	if err := r.run(); err != nil {
		log.Fatalf("strategyd: %v", err)
	}
}

type runner struct {
	journalDir string
	instrument string
	size       fixed.Dec
	step       fixed.Dec
	allowShort bool
	live       bool

	strat *strategy.SmaCross
	bars  *strategy.Bars
	book  *book.Book

	placed int
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

	if decoded.Kind != journal.KindMark || decoded.MarkInstrument != r.instrument {
		return
	}
	if closed, ok := r.bars.Observe(frame.Header.Nanos, decoded.MarkPrice); ok {
		r.onBar(closed)
	}
}

func (r *runner) onBar(closed fixed.Dec) {
	d := r.strat.OnBar(closed)
	if d.Stance == strategy.NoOpinion {
		log.Printf("strategyd: bar %s — warming up, %d/%d", closed, d.Have, d.Need)
		return
	}

	desired, ok := strategy.Target(d.Stance, r.size, r.allowShort)
	if !ok {
		return
	}
	desired = strategy.RoundToStep(desired, r.step)
	actual := r.book.PositionIn(r.instrument).Qty

	order, needed := strategy.Reconcile(desired, actual, r.step)
	if !needed {
		log.Printf("strategyd: bar %s — %s, fast %s slow %s, holding %s",
			closed, d.Stance, d.Fast, d.Slow, actual)
		return
	}

	exit := ""
	if order.Exit {
		exit = " (exit)"
	}
	log.Printf("strategyd: bar %s — %s, fast %s slow %s, %s -> %s: %s %s%s",
		closed, d.Stance, d.Fast, d.Slow, actual, desired, order.Side, order.Qty, exit)

	if !r.live {
		return
	}
	if err := r.place(order); err != nil {
		// Not fatal. The next bar reconciles from whatever the position
		// actually is, so a refused or lost order corrects itself rather than
		// leaving this process reasoning from a fiction.
		log.Printf("strategyd: place failed: %v", err)
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

	conn, err := net.DialTimeout("unix", r.journalDir+"/control.sock", 2*time.Second)
	if err != nil {
		return fmt.Errorf("the writer is not listening: %w", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write(append(line, '\n')); err != nil {
		return err
	}
	answer := make([]byte, 4096)
	n, err := conn.Read(answer)
	if err != nil {
		return err
	}
	log.Printf("strategyd: %s", answer[:n])
	return nil
}
