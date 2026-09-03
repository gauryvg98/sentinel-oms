// Package klines backfills candle history from the venue.
//
// The journal only knows what it has seen. A fresh deployment has no history at
// all, and this one had twenty hours of it — enough that a 20-period average
// produced exactly one point, which draws no line and, far worse, means the
// strategy is averaging over a window it only partly has.
//
// The Python fetched three hundred bars from the venue on startup for exactly
// this reason. The venue has the history; asking for it is a request, not a
// guess.
//
// This is read-only public data: no key, no signature, nothing that could place
// an order.
package klines

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/gauryvg98/sentinel-oms/go/internal/fixed"
)

// Bar is one candle, in the shape the chart and the strategy both want.
type Bar struct {
	// OpenTime is the bucket start, in seconds.
	OpenTime           int64
	Open, High, Low, C fixed.Dec
}

// Client reads public candles.
type Client struct {
	base string
	http *http.Client
}

// New points at a venue. An empty base means Binance USD-M Demo Trading, which
// is what this deployment runs on.
func New(base string) *Client {
	if base == "" {
		base = "https://demo-fapi.binance.com"
	}
	return &Client{base: base, http: &http.Client{Timeout: 20 * time.Second}}
}

// BaseFor picks the endpoint from the same variable the writer reads, so the
// history comes from the venue actually being traded.
func BaseFor(env string) string {
	if env == "production" {
		return "https://fapi.binance.com"
	}
	return "https://demo-fapi.binance.com"
}

// Fetch returns up to limit closed candles, oldest first.
//
// The most recent candle is still forming, and a forming bar is not a closed
// one: feeding it to a strategy that decides on closes would have it act on a
// price that is still moving. So it is dropped.
func (c *Client) Fetch(symbol, interval string, limit int) ([]Bar, error) {
	if limit < 1 {
		limit = 300
	}
	query := url.Values{}
	query.Set("symbol", symbol)
	query.Set("interval", interval)
	// One extra, since the forming one is discarded.
	query.Set("limit", fmt.Sprint(limit+1))

	resp, err := c.http.Get(c.base + "/fapi/v1/klines?" + query.Encode())
	if err != nil {
		return nil, fmt.Errorf("klines: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("klines: venue answered %s", resp.Status)
	}

	// Binance sends an array of arrays with mixed types: numbers for the times,
	// strings for the prices. Decoded into json.RawMessage so the prices stay
	// text all the way into fixed-point — parsing them as JSON numbers would
	// route every price through a float64 on the way in.
	var raw [][]json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, fmt.Errorf("klines: %w", err)
	}

	out := make([]Bar, 0, len(raw))
	for _, row := range raw {
		if len(row) < 5 {
			continue
		}
		var openMillis int64
		if err := json.Unmarshal(row[0], &openMillis); err != nil {
			continue
		}
		bar := Bar{OpenTime: openMillis / 1000}
		ok := true
		for i, target := range []*fixed.Dec{&bar.Open, &bar.High, &bar.Low, &bar.C} {
			var text string
			if err := json.Unmarshal(row[i+1], &text); err != nil {
				ok = false
				break
			}
			value, err := fixed.Parse(text)
			if err != nil {
				ok = false
				break
			}
			*target = value
		}
		if ok {
			out = append(out, bar)
		}
	}

	// Drop the forming candle.
	if len(out) > 0 {
		out = out[:len(out)-1]
	}
	if len(out) > limit {
		out = out[len(out)-limit:]
	}
	return out, nil
}
