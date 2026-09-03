// Command adversary checks the account's invariants from outside the engine.
//
// In-process assertions share the engine's assumptions: if the check and the
// thing it checks are wrong the same way, the assertion agrees with the bug.
// This one reads the journal as an outsider, recomputes everything by the
// simplest possible route, and exits non-zero on a breach so it can run against
// a live deployment on a schedule.
//
// Simplest possible route matters. Where the engine keeps a running cost basis,
// this sums the raw fill records; where the engine tracks an order's state
// machine, this reads the state the record carries. Two paths to one number is
// the only thing that makes agreement mean something.
package main

import (
	"fmt"
	"os"
	"sort"

	"github.com/gauryvg98/sentinel-oms/go/internal/book"
	"github.com/gauryvg98/sentinel-oms/go/internal/fixed"
	"github.com/gauryvg98/sentinel-oms/go/internal/journal"
	"github.com/gauryvg98/sentinel-oms/go/internal/record"
)

func main() {
	dir := "/data/journal"
	if len(os.Args) > 1 {
		dir = os.Args[1]
	} else if env := os.Getenv("SENTINEL_JOURNAL"); env != "" {
		dir = env
	}

	breaches, err := check(dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "adversary: %v\n", err)
		os.Exit(2)
	}
	if len(breaches) == 0 {
		fmt.Println("adversary: all invariants hold")
		return
	}
	for _, breach := range breaches {
		fmt.Fprintf(os.Stderr, "adversary: %s\n", breach)
	}
	os.Exit(1)
}

// check folds the journal and recomputes every invariant independently.
func check(dir string) ([]string, error) {
	tailer, err := journal.OpenTailer(dir, 0)
	if err != nil {
		return nil, fmt.Errorf("open journal: %w", err)
	}
	defer tailer.Close()

	b := book.New()
	// Computed from the raw fill records, by a different route than the book
	// takes. If these two disagree, one of them is wrong and it matters which.
	fillSums := map[string]fixed.Dec{}
	seenExec := map[string]bool{}
	intents := map[string]*record.Intent{}
	states := map[string]record.State{}
	filled := map[string]fixed.Dec{}

	for {
		frame, err := tailer.Next()
		if err != nil {
			return nil, fmt.Errorf("read journal: %w", err)
		}
		if frame == nil {
			break
		}
		decoded, err := record.Decode(frame.Header.Kind, frame.Payload)
		if err != nil {
			return nil, fmt.Errorf("record %d: %w", frame.Header.Seq, err)
		}
		b.Apply(frame.Header, decoded)

		switch decoded.Kind {
		case journal.KindIntentPersisted:
			if _, seen := intents[decoded.Intent.ClientOrderID]; !seen {
				intents[decoded.Intent.ClientOrderID] = decoded.Intent
				states[decoded.Intent.ClientOrderID] = record.Created
			}
		case journal.KindOrderEvent:
			event := decoded.Event
			states[event.ClientOrderID] = event.To
			if event.Kind == record.FillReceived && event.Applied() && !seenExec[event.ExecID] {
				seenExec[event.ExecID] = true
				intent := intents[event.ClientOrderID]
				if intent == nil {
					continue
				}
				signed := fixed.FromRaw(event.Qty.Raw() * intent.Side.Sign())
				fillSums[intent.Instrument] = fillSums[intent.Instrument].Add(signed)
				filled[event.ClientOrderID] = filled[event.ClientOrderID].Add(event.Qty)
			}
			if event.Kind == record.ReconcileResolved {
				filled[event.ClientOrderID] = event.FilledQty
			}
		}
	}

	var breaches []string

	// Every position equals the signed sum of its fills. The check that catches
	// a posting applied twice, a fill dropped, or arithmetic gone astray.
	instruments := map[string]bool{}
	for name := range fillSums {
		instruments[name] = true
	}
	for _, p := range b.Positions() {
		instruments[p.Instrument] = true
	}
	for _, name := range sorted(instruments) {
		held := b.PositionIn(name).Qty
		if held != fillSums[name] {
			breaches = append(breaches, fmt.Sprintf(
				"%s: position %s but fills sum to %s", name, held, fillSums[name]))
		}
	}

	// No order records more filled quantity than it authorised.
	for id, intent := range intents {
		if got := filled[id]; got.Raw() > intent.Qty.Raw() {
			breaches = append(breaches, fmt.Sprintf(
				"%s: filled %s against an order of %s", id, got, intent.Qty))
		}
	}

	// Live protective exits never add up to more than the position they close.
	exits := map[string]fixed.Dec{}
	for id, intent := range intents {
		if intent.Authority != record.ProtectiveExit || states[id].IsTerminal() {
			continue
		}
		remaining := intent.Qty.Sub(filled[id])
		if remaining.Raw() < 0 {
			remaining = 0
		}
		exits[intent.Instrument] = exits[intent.Instrument].Add(remaining)
	}
	for _, name := range sorted(exits) {
		held := b.PositionIn(name).Qty.Abs()
		if exits[name].Raw() > held.Raw() {
			breaches = append(breaches, fmt.Sprintf(
				"%s: %s of live exits against a position of %s", name, exits[name], held))
		}
	}

	// Every order is traceable to the decision that authorised it.
	for id, intent := range intents {
		if intent.TraceID == "" || intent.TraceID == "00000000000000000000000000000000" {
			breaches = append(breaches, fmt.Sprintf("%s: no trace id", id))
		}
	}

	// And the account should say so if it has stopped.
	if b.IsHalted() {
		breaches = append(breaches, "the account is HALTED and is not trading")
	}

	return breaches, nil
}

func sorted[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
