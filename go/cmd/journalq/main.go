// Command journalq answers questions about a journal after the fact.
//
//	journalq /data/journal-fmt2                  every decision
//	journalq /data/journal-fmt2 HALTED           only halts
//	journalq /data/journal-fmt2 HALTED,RESUMED   either
//	journalq /data/journal-fmt2 events           every order event and fill
//	journalq /data/journal-fmt2 events SMA-17    only those orders
//
// "events" is the one that answers how a position came to disagree with the
// venue: it prints the lifecycle, so a fill booked against an order that never
// filled, or a fill that arrived and moved nothing, is visible as a line.
//
// It exists because the live projection keeps a bounded ring — two hundred
// decisions — and the most important record in the log is the one explaining
// why the account stopped trading. That record ages out within an hour of
// ordinary activity, and after that "why is it halted" had no answer anywhere.
// A log you cannot query is a log you are only pretending to keep.
package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/gauryvg98/sentinel-oms/go/internal/journal"
	"github.com/gauryvg98/sentinel-oms/go/internal/record"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: journalq <journal-dir> [KIND[,KIND...]]")
		os.Exit(2)
	}
	dir := os.Args[1]

	events := len(os.Args) > 2 && strings.EqualFold(os.Args[2], "events")
	match := ""
	if events && len(os.Args) > 3 {
		match = os.Args[3]
	}

	want := map[string]bool{}
	if !events && len(os.Args) > 2 {
		for _, k := range strings.Split(os.Args[2], ",") {
			want[strings.ToUpper(strings.TrimSpace(k))] = true
		}
	}

	tailer, err := journal.OpenTailer(dir, 0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "journalq: %v\n", err)
		os.Exit(1)
	}
	defer tailer.Close()

	var read, shown int
	for {
		frame, err := tailer.Next()
		if err != nil {
			fmt.Fprintf(os.Stderr, "journalq: %v\n", err)
			os.Exit(1)
		}
		if frame == nil {
			break // caught up
		}
		read++

		if events {
			if frame.Header.Kind != journal.KindOrderEvent {
				continue
			}
			decoded, err := record.Decode(frame.Header.Kind, frame.Payload)
			if err != nil || decoded.Event == nil {
				continue
			}
			e := decoded.Event
			if match != "" && !strings.Contains(e.ClientOrderID, match) {
				continue
			}
			shown++
			applied := "applied"
			if !e.Applied() {
				applied = "EVIDENCE ONLY"
			}
			extra := ""
			if e.Kind == record.FillReceived {
				extra = fmt.Sprintf("  qty %s @ %s  exec %s", e.Qty, e.Price, e.ExecID)
			}
			if e.RejectText != "" {
				extra += "  reject " + e.RejectText
			}
			fmt.Printf("%s  seq %-8d  %-24s %-20s %-11s -> %-11s %-13s%s\n",
				stamp(frame.Header.Nanos), frame.Header.Seq, e.ClientOrderID,
				e.Kind, e.From, e.To, applied, extra)
			continue
		}

		if frame.Header.Kind != journal.KindDecision {
			continue
		}
		decoded, err := record.Decode(frame.Header.Kind, frame.Payload)
		if err != nil || decoded.Decision == nil {
			continue
		}
		kind := decoded.Decision.Kind.String()
		if len(want) > 0 && !want[kind] {
			continue
		}
		shown++
		fmt.Printf("%s  seq %-8d  %-10s %-18s %s %s\n",
			stamp(frame.Header.Nanos),
			frame.Header.Seq,
			decoded.Decision.Actor.String(),
			kind,
			decoded.Decision.Instrument,
			decoded.Decision.Note)
	}
	fmt.Fprintf(os.Stderr, "journalq: %d records read, %d shown\n", read, shown)
}

func stamp(nanos uint64) string {
	return time.Unix(0, int64(nanos)).UTC().Format("2006-01-02 15:04:05")
}
