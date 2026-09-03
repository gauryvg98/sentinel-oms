// Package bars folds marks into OHLC candles.
//
// The journal carries a mark about once a second and nothing else; candles are
// a view of that, not a second source of truth. Building them here — in the
// gateway, from the log — means a chart has history the moment it loads rather
// than filling in from whatever arrives after you opened the page, and means
// the candles a replay produces are the candles that were shown live.
//
// Boundaries come from the mark's own journal timestamp, never a clock read
// here. An aggregator that read the wall clock would draw a different chart on
// every replay.
package bars

import "github.com/gauryvg98/sentinel-oms/go/internal/fixed"

// Bar is one candle. Time is seconds, which is what chart libraries want.
type Bar struct {
	Time  int64     `json:"time"`
	Open  fixed.Dec `json:"open"`
	High  fixed.Dec `json:"high"`
	Low   fixed.Dec `json:"low"`
	Close fixed.Dec `json:"close"`
}

// Series is the candles for one instrument.
type Series struct {
	intervalNanos uint64
	max           int
	bars          []Bar
	bucket        uint64
}

// NewSeries keeps at most max candles of the given interval.
func NewSeries(intervalNanos uint64, max int) *Series {
	return &Series{intervalNanos: intervalNanos, max: max}
}

// Observe folds one mark in.
//
// A mark in the current bucket extends the open candle; one in a later bucket
// starts a new candle. An interval in which no mark arrived produces no candle
// rather than a flat one copied from its neighbour: a candle nobody saw is not
// a candle whose price may be invented.
func (s *Series) Observe(atNanos uint64, price fixed.Dec) {
	if s.intervalNanos == 0 {
		return
	}
	bucket := atNanos / s.intervalNanos

	// A mark older than the newest candle is history we already hold — the
	// backfill covered it, from the venue, more completely than our own marks
	// could. Appending it would drive the series backwards in time, which is
	// not merely untidy: a chart library given descending times draws nothing
	// at all, and the whole screen goes blank over one stale record.
	//
	// This matters on every restart, because the gateway replays the journal
	// from the beginning while the backfill has already reached the present.
	if len(s.bars) > 0 && bucket < s.bucket {
		return
	}

	if len(s.bars) > 0 && bucket == s.bucket {
		last := &s.bars[len(s.bars)-1]
		if price.Raw() > last.High.Raw() {
			last.High = price
		}
		if price.Raw() < last.Low.Raw() {
			last.Low = price
		}
		last.Close = price
		return
	}

	s.bucket = bucket
	s.bars = append(s.bars, Bar{
		Time:  int64(bucket * s.intervalNanos / 1_000_000_000),
		Open:  price,
		High:  price,
		Low:   price,
		Close: price,
	})
	if len(s.bars) > s.max {
		s.bars = s.bars[len(s.bars)-s.max:]
	}
}

// Seed installs history fetched from elsewhere, oldest first.
//
// Only ever into an empty series, and only candles older than anything already
// held: backfill fills in what was never seen, it does not overwrite what was.
// The journal is the record of what this system observed; the venue's history
// is context in front of it.
func (s *Series) Seed(history []Bar) {
	if len(s.bars) > 0 || len(history) == 0 {
		return
	}
	if len(history) > s.max {
		history = history[len(history)-s.max:]
	}
	s.bars = append(s.bars, history...)
	last := s.bars[len(s.bars)-1]
	if s.intervalNanos > 0 {
		//nolint:gosec // a bucket index from a second-resolution timestamp
		s.bucket = uint64(last.Time) * 1_000_000_000 / s.intervalNanos
	}
}

// Bars returns a copy, oldest first.
func (s *Series) Bars() []Bar {
	out := make([]Bar, len(s.bars))
	copy(out, s.bars)
	return out
}

// Len is how many candles are held.
func (s *Series) Len() int { return len(s.bars) }
