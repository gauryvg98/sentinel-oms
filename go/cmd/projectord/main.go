// Command projectord copies the journal into a database.
//
// The cold path. Nothing reads from Supabase to make a decision, so this can be
// down for an hour and the engine will not notice — which is the whole of "no
// Postgres in the hot path". Postgres was never slow; it was load bearing.
//
// The cursor is committed only after a batch is durable at the far end, so a
// crash re-sends rather than skips. Re-sending is safe because the sink ignores
// duplicate sequences: at-least-once delivery into an idempotent sink is
// exactly-once, and the alternative is a two-phase commit with a service that
// does not offer one.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/gauryvg98/sentinel-oms/go/internal/fixed"
	"github.com/gauryvg98/sentinel-oms/go/internal/journal"
	"github.com/gauryvg98/sentinel-oms/go/internal/pg"
	"github.com/gauryvg98/sentinel-oms/go/internal/record"
	"github.com/gauryvg98/sentinel-oms/go/internal/supabase"
)

// Sink is somewhere the journal can be copied to.
//
// Two exist: Postgres, which is what a Fly deployment already has, and
// Supabase, which is Postgres with a REST layer in front. The projector does
// not care which — it batches, sends, and moves a cursor. Both must ignore
// duplicate sequences, because that is what makes a restart safe.
type Sink interface {
	Insert(rows []pg.Row) error
	Close() error
}

// supabaseSink adapts the REST client to the same interface.
type supabaseSink struct{ client *supabase.Client }

func (s supabaseSink) Close() error { return nil }

func (s supabaseSink) Insert(rows []pg.Row) error {
	out := make([]supabase.Row, 0, len(rows))
	for _, r := range rows {
		out = append(out, supabase.Row{
			Seq: r.Seq, Epoch: r.Epoch, Nanos: r.Nanos, Kind: r.Kind,
			Instrument: r.Instrument, OrderID: r.OrderID, Payload: r.Payload,
		})
	}
	return s.client.Insert(out)
}

// consumer is this projector's name in the journal's cursor directory.
const consumer = "projectord"

func main() {
	journalDir := env("SENTINEL_JOURNAL", "/data/journal")

	if len(os.Args) > 1 && os.Args[1] == "schema" {
		// Printed rather than applied. Creating tables from a process that
		// also writes to them is how a bad migration reaches production at
		// three in the morning.
		os.Stdout.WriteString(pg.Schema)
		return
	}

	client, err := openSink()
	if err != nil {
		log.Fatalf("projectord: %v", err)
	}
	defer client.Close()

	cursor, err := journal.LoadCursor(journalDir, consumer)
	if err != nil {
		log.Fatalf("projectord: %v", err)
	}
	tailer, err := journal.OpenTailer(journalDir, cursor.Seq())
	if err != nil {
		log.Fatalf("projectord: %v", err)
	}
	defer tailer.Close()
	log.Printf("projectord: resuming after seq %d", cursor.Seq())

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)

	// Batch size and linger, in that order of importance. A batch of 500 rows
	// is one request instead of five hundred; the linger is what stops a quiet
	// account from holding one row for an hour.
	const batchSize = 500
	const linger = 5 * time.Second

	batch := make([]pg.Row, 0, batchSize)
	var highest uint64
	lingerUntil := time.Time{}

	for {
		select {
		case <-stop:
			// Flush what is in hand. Losing it would be harmless — it would be
			// re-read next boot — but a clean exit is one that leaves nothing
			// to re-do.
			flush(client, cursor, batch, highest)
			log.Print("projectord: stopped")
			return
		default:
		}

		frame, err := tailer.Next()
		if err != nil {
			log.Fatalf("projectord: read journal: %v", err)
		}
		if frame == nil {
			// Caught up. Flush anything lingering, then wait.
			if len(batch) > 0 && !lingerUntil.IsZero() && time.Now().After(lingerUntil) {
				if flush(client, cursor, batch, highest) {
					batch = batch[:0]
					lingerUntil = time.Time{}
				}
			}
			time.Sleep(200 * time.Millisecond)
			continue
		}

		row, ok := toRow(frame)
		if !ok {
			// A record this build cannot read is still history. Skipping it
			// would leave a hole in a table whose primary key is a sequence,
			// which is a hole somebody will eventually notice and not be able
			// to explain.
			log.Printf("projectord: seq %d did not decode; storing it raw", frame.Header.Seq)
		}
		batch = append(batch, row)
		highest = frame.Header.Seq
		if lingerUntil.IsZero() {
			// Armed on the batch's *first* row, not refreshed by each one — an
			// account trickling one record a second would otherwise never
			// reach the deadline.
			lingerUntil = time.Now().Add(linger)
		}

		if len(batch) >= batchSize {
			if flush(client, cursor, batch, highest) {
				batch = batch[:0]
				lingerUntil = time.Time{}
			}
		}
	}
}

// flush sends a batch and commits the cursor, in that order.
//
// The order is the guarantee: the cursor moves only after the rows are durable
// at the far end, so a crash between them re-sends rather than skips.
func flush(client Sink, cursor *journal.Cursor, batch []pg.Row, highest uint64) bool {
	if len(batch) == 0 {
		return true
	}
	if err := client.Insert(batch); err != nil {
		// Not fatal. Supabase being unreachable is exactly the situation this
		// design is meant to survive, and the cursor has not moved, so the
		// records are still there to send.
		log.Printf("projectord: %v (will retry)", err)
		return false
	}
	if err := cursor.Commit(highest); err != nil {
		log.Printf("projectord: cursor: %v", err)
		return false
	}
	return true
}

// openSink picks a destination from the environment.
//
// DATABASE_URL wins, because a Fly deployment already has a Postgres and
// pointing at it needs no third party. Running without a projector at all is a
// deployment choice; running with a broken one is a fault, so an unreachable
// database is fatal here rather than a warning every five seconds.
func openSink() (Sink, error) {
	if url := os.Getenv("DATABASE_URL"); url != "" {
		client, err := pg.New(pg.Config{
			URL:   url,
			Table: env("SENTINEL_PROJECTION_TABLE", "journal_records"),
		})
		if err != nil {
			return nil, err
		}
		if err := client.Ping(); err != nil {
			return nil, fmt.Errorf("cannot reach the database: %w", err)
		}
		log.Print("projectord: projecting into postgres")
		return client, nil
	}

	client, err := supabase.New(supabase.Config{
		URL:        os.Getenv("SUPABASE_URL"),
		ServiceKey: os.Getenv("SUPABASE_SERVICE_KEY"),
		Table:      env("SUPABASE_TABLE", "journal_records"),
	})
	if errors.Is(err, supabase.ErrNotConfigured) {
		return nil, errors.New("set DATABASE_URL, or SUPABASE_URL with SUPABASE_SERVICE_KEY")
	}
	if err != nil {
		return nil, err
	}
	log.Print("projectord: projecting into supabase")
	return supabaseSink{client: client}, nil
}

func toRow(frame *journal.Record) (pg.Row, bool) {
	row := pg.Row{
		Seq:     frame.Header.Seq,
		Epoch:   frame.Header.Epoch,
		Nanos:   frame.Header.Nanos,
		Kind:    frame.Header.Kind.String(),
		Payload: json.RawMessage("null"),
	}

	decoded, err := record.Decode(frame.Header.Kind, frame.Payload)
	if err != nil {
		row.Payload = json.RawMessage(strconv.Quote("undecodable: " + err.Error()))
		return row, false
	}

	switch decoded.Kind {
	case journal.KindIntentPersisted:
		row.Instrument = decoded.Intent.Instrument
		row.OrderID = decoded.Intent.ClientOrderID
	case journal.KindOrderEvent:
		row.OrderID = decoded.Event.ClientOrderID
	case journal.KindDecision:
		row.Instrument = decoded.Decision.Instrument
	case journal.KindMark:
		row.Instrument = decoded.MarkInstrument
	}

	if payload, err := json.Marshal(readable(decoded)); err == nil {
		row.Payload = payload
	}
	return row, true
}

// readable turns a decoded record into JSON somebody would want to query.
//
// Marshalling the struct directly is the obvious thing and it produces
// `{"Kind":5,"To":8,"From":3}` — technically the whole record, practically
// unusable, because answering "show me the fills" means first knowing that
// five is a fill and eight is filled. Enums go in by name, the event or
// decision is flattened to the top level rather than nested under the variant
// that happened to carry it, and empty fields are dropped so a query can tell
// absent from zero.
func readable(r record.Record) map[string]any {
	out := map[string]any{}
	put := func(key string, value any) {
		switch v := value.(type) {
		case string:
			if v == "" {
				return
			}
		case fixed.Dec:
			if v.IsZero() {
				return
			}
			out[key] = v.String()
			return
		}
		out[key] = value
	}

	switch r.Kind {
	case journal.KindIntentPersisted:
		i := r.Intent
		put("client_order_id", i.ClientOrderID)
		put("instrument", i.Instrument)
		put("side", i.Side.String())
		put("authority", i.Authority.String())
		put("order_kind", i.Kind.String())
		put("qty", i.Qty)
		put("limit_price", i.LimitPrice)
		put("stop_price", i.StopPrice)
		put("trace_id", i.TraceID)
	case journal.KindOrderEvent:
		e := r.Event
		put("client_order_id", e.ClientOrderID)
		put("event", e.Kind.String())
		put("from_state", e.From.String())
		put("to_state", e.To.String())
		out["applied"] = e.Applied()
		put("qty", e.Qty)
		put("price", e.Price)
		put("filled_qty", e.FilledQty)
		put("exec_id", e.ExecID)
		put("venue_order_id", e.VenueOrderID)
		put("reject_text", e.RejectText)
		if e.RejectCode != 0 {
			out["reject_code"] = e.RejectCode
		}
		put("resolved_state", e.ResolvedState.String())
		put("trace_id", e.TraceID)
	case journal.KindDecision:
		d := r.Decision
		put("decision", d.Kind.String())
		put("actor", d.Actor.String())
		put("instrument", d.Instrument)
		put("value", d.Value)
		put("note", d.Note)
		put("trace_id", d.TraceID)
	case journal.KindMark:
		put("instrument", r.MarkInstrument)
		put("price", r.MarkPrice)
	case journal.KindBalance:
		put("asset", r.Asset)
		put("amount", r.Amount)
	case journal.KindEpochClaimed:
		out["epoch"] = r.Epoch
		out["resumed_from"] = r.ResumedFrom
	}
	return out
}

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
