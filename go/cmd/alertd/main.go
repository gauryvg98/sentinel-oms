// Command alertd watches the journal and tells somebody when it matters.
//
// It reads the same log everything else does and holds no state of its own, so
// it can be killed and restarted freely. It is not one of the processes the
// container depends on: a dead alerter must not stop a working supervisor —
// though it does mean nobody is watching, which is why it says so loudly on the
// way out.
//
// Three things page:
//
//   - the account halted, with the reason. This is the one that cost ten hours.
//   - the journal stopped advancing. The writer ticks about once a second; if
//     it has said nothing for two minutes the supervisor is wedged or dead, and
//     a supervisor that is neither trading nor halted is the worst of the three
//     states because nothing else reports it.
//   - the strategy stopped deciding. It declares a stance every bar; silence
//     for several bars means it died while the writer carried on looking
//     perfectly healthy.
//   - the venue refused an order. On 2026-09-03 it refused every protective
//     stop — "Order type not supported for this endpoint" — and the system
//     carried on reporting healthy, because every check here verified that
//     what happened was recorded faithfully and none asked whether a thing
//     that had to happen did. A position ran unprotected for an hour.
//   - a position is held with no stop under it. That is the invariant the
//     rejection violated, and the one worth watching directly: it does not
//     care why there is no stop, only that there is exposure nobody is
//     protecting.
//
// A resume clears the halt and says so, because "it is fixed" is news too.
package main

import (
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gauryvg98/sentinel-oms/go/internal/alert"
	"github.com/gauryvg98/sentinel-oms/go/internal/journal"
	"github.com/gauryvg98/sentinel-oms/go/internal/record"
)

// readStance pulls "holding X" and "stop Y" out of a stance note, reporting
// whether a position is held and whether a stop is armed under it.
//
// Parsing a note is not how this ought to work — the numbers should be fields.
// It is done this way because the alternative was not watching at all, and the
// note is what the strategy already writes. If the format changes this reports
// no stop, which fails towards paging rather than towards silence.
func readStance(note string) (held bool, stopped bool) {
	for _, part := range []struct {
		key string
		out *bool
	}{{"holding ", &held}, {"stop ", &stopped}} {
		i := strings.Index(note, part.key)
		if i < 0 {
			continue
		}
		value := note[i+len(part.key):]
		if end := strings.IndexByte(value, ' '); end >= 0 {
			value = value[:end]
		}
		if value == "" {
			continue
		}
		// "holding 0" is not holding; any stop at all counts as a stop.
		*part.out = part.key != "holding " || (value != "0" && value != "0.0")
	}
	return held, stopped
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	if v, err := time.ParseDuration(env(key, "")); err == nil && v > 0 {
		return v
	}
	return fallback
}

func main() {
	log.SetFlags(0)

	journalDir := env("SENTINEL_JOURNAL", "/data/journal")
	notifier, err := alert.New(
		os.Getenv("SENTINEL_ALERT_WEBHOOK"),
		envDuration("SENTINEL_ALERT_REPEAT", time.Hour),
	)
	if err != nil {
		// Running without alerting is a deployment choice. Saying so plainly
		// beats a process that exits 0 and leaves someone assuming it works.
		log.Fatal("alertd: SENTINEL_ALERT_WEBHOOK is not set — nothing will be reported")
	}

	// Two minutes of silence from a writer that ticks every second is not a
	// quiet market, it is a stopped process.
	stallAfter := envDuration("SENTINEL_ALERT_STALL_AFTER", 2*time.Minute)
	// Several bars, so a single missed evaluation is not a page.
	silentAfter := envDuration("SENTINEL_ALERT_STRATEGY_SILENT_AFTER", 15*time.Minute)

	name := env("SENTINEL_ALERT_NAME", "sentinel")
	log.Printf("alertd: watching %s, stall %s, strategy silence %s",
		journalDir, stallAfter, silentAfter)

	tailer, err := journal.OpenTailer(journalDir, 0)
	if err != nil {
		log.Fatalf("alertd: open journal: %v", err)
	}
	defer tailer.Close()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)

	var (
		halted     bool
		haltReason string
		caughtUp   bool
		lastRecord = time.Now()
		lastStance = time.Now()
	)

	for {
		select {
		case <-stop:
			log.Print("alertd: stopped — nothing is watching now")
			return
		default:
		}

		frame, err := tailer.Next()
		if err != nil {
			// The log itself is unreadable. Report it, then stop: a watcher
			// that cannot read is worse than one that is honestly absent.
			notifier.Send("journal-unreadable",
				name+": cannot read the journal: "+err.Error())
			log.Fatalf("alertd: read journal: %v", err)
		}

		if frame != nil {
			lastRecord = time.Now()
			if decoded, err := record.Decode(frame.Header.Kind, frame.Payload); err == nil {
				switch {
				case decoded.Decision != nil && decoded.Decision.Kind == record.Halted:
					halted, haltReason = true, decoded.Decision.Note
					if caughtUp {
						notifier.Send("halted", name+" HALTED at seq "+
							strconv.FormatUint(frame.Header.Seq, 10)+": "+haltReason)
					}
				case decoded.Decision != nil && decoded.Decision.Kind == record.Resumed:
					halted, haltReason = false, ""
					notifier.Clear("halted")
					if caughtUp {
						notifier.Send("", name+" resumed: "+decoded.Decision.Note)
					}
				case decoded.Decision != nil && decoded.Decision.Kind == record.StanceDeclared:
					lastStance = time.Now()
					notifier.Clear("strategy-silent")

					// The stance carries what is held and, when there is one,
					// the armed stop. Exposure with nothing under it is the
					// condition that matters; the reason is secondary.
					held, stop := readStance(decoded.Decision.Note)
					if caughtUp && held && !stop {
						notifier.Send("naked-position",
							name+" is holding a position with no stop: "+decoded.Decision.Note)
					} else if stop {
						notifier.Clear("naked-position")
					}

				case decoded.Event != nil && decoded.Event.Kind == record.VenueRejected:
					// Every rejection, not only the stops. An order the system
					// decided to place and the venue would not take is a gap
					// between what this thinks it did and what happened.
					if caughtUp {
						notifier.Send("rejected:"+decoded.Event.ClientOrderID,
							name+" order "+decoded.Event.ClientOrderID+
								" was REFUSED by the venue: "+decoded.Event.RejectText)
					}
				}
			}
			continue
		}

		// Caught up with the log that already existed. Everything before this
		// is history: replaying it must not page anybody for a halt that was
		// cleared last week.
		if !caughtUp {
			caughtUp = true
			lastRecord, lastStance = time.Now(), time.Now()
			log.Printf("alertd: caught up; account is %s",
				map[bool]string{true: "HALTED: " + haltReason, false: "trading"}[halted])
			// A process that starts up into an account that is already halted
			// should still say so — that is exactly the case where the first
			// alert was missed or nobody was listening.
			if halted {
				notifier.Send("halted", name+" is HALTED: "+haltReason)
			}
		}

		if time.Since(lastRecord) > stallAfter {
			notifier.Send("stalled", name+" has written nothing for "+
				time.Since(lastRecord).Round(time.Second).String()+
				" — the writer is wedged or gone")
		} else {
			notifier.Clear("stalled")
		}

		// Only worth saying while the account can actually trade. A halted
		// account has a strategy declaring into a wall, and the halt is the
		// news, not the silence that follows it.
		if !halted && time.Since(lastStance) > silentAfter {
			notifier.Send("strategy-silent", name+" strategy has declared nothing for "+
				time.Since(lastStance).Round(time.Second).String())
		}

		time.Sleep(time.Second)
	}
}
