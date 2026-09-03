// Command gatewayd serves the journal to browsers.
//
// Read-only unless SENTINEL_ADMIN_TOKEN is set, and read-only for everyone if
// it is not — the safe configuration is the one you get by not configuring
// anything.
//
// It never asks the engine for state. It tails the same log the engine writes
// and folds its own projection, so a browser attached to this cannot slow the
// writer down, whatever it does.
package main

import (
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gauryvg98/sentinel-oms/go/internal/gateway"
)

// barInterval reads the strategy's bar size, which the chart mirrors.
// SENTINEL_CHART_INTERVAL overrides it for a deployment that wants a different
// view — but the default is to agree, because a chart that disagrees with the
// strategy is worse than no chart.
func barInterval() time.Duration {
	for _, key := range []string{"SENTINEL_CHART_INTERVAL", "SENTINEL_STRATEGY_INTERVAL"} {
		if d, err := time.ParseDuration(os.Getenv(key)); err == nil && d > 0 {
			return d
		}
	}
	return time.Minute
}

func main() {
	config := gateway.Config{
		JournalDir: env("SENTINEL_JOURNAL", "/data/journal"),
		AdminToken: os.Getenv("SENTINEL_ADMIN_TOKEN"),
		Addr:       env("SENTINEL_GATEWAY_ADDR", ":8000"),
		// The chart follows the strategy's clock, so the averages drawn over
		// the candles are the averages the strategy actually traded on.
		BarInterval: barInterval(),
	}

	g := gateway.New(config)
	if config.AdminToken == "" {
		log.Print("gatewayd: read-only — set SENTINEL_ADMIN_TOKEN to enable controls")
	}

	// The tailer runs alongside the server rather than before it: a gateway
	// that refused to serve until the journal existed would be unreachable
	// during exactly the boot it is most useful for watching.
	go func() {
		// 20ms: fast enough that a fill appears on screen before anyone
		// notices, slow enough to cost nothing. It is a poll only because
		// there is no portable way to be woken by a file growing; everything
		// above it is event-driven.
		if err := g.Tail(20 * time.Millisecond); err != nil {
			log.Fatalf("gatewayd: %v", err)
		}
	}()

	server := &http.Server{
		Addr:              config.Addr,
		Handler:           g.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		// No write timeout. The event stream is a long-lived response by
		// design, and a write deadline would cut it at a fixed interval
		// forever.
	}
	log.Printf("gatewayd: listening on %s, journal %s", config.Addr, config.JournalDir)
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("gatewayd: %v", err)
	}
}

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
