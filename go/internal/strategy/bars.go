package strategy

import "github.com/gauryvg98/sentinel-oms/go/internal/fixed"

// Bars turns a stream of marks into closed bars.
//
// The venue sends a mark about once a second; a strategy wants the close of a
// minute. The boundary is computed from the mark's own timestamp — the one the
// journal recorded — rather than from a clock read here, so replaying a journal
// produces exactly the bars that were produced live. A bar aggregator that read
// the wall clock would give a different answer on every replay and quietly make
// every backtest a fiction.
//
// A bar closes when a mark arrives that belongs to a later interval. That means
// the close is the last mark actually seen in the interval, and an interval in
// which nothing arrived is skipped rather than invented: a bar this never saw
// is not a bar whose price it may guess at.
type Bars struct {
	intervalNanos uint64
	bucket        uint64
	last          fixed.Dec
	open          bool
}

// NewBars builds an aggregator for the given interval.
func NewBars(intervalNanos uint64) *Bars {
	return &Bars{intervalNanos: intervalNanos}
}

// Observe takes one mark. It returns the close of the bar this mark ended, if
// this mark ended one.
func (b *Bars) Observe(atNanos uint64, price fixed.Dec) (fixed.Dec, bool) {
	if b.intervalNanos == 0 {
		return 0, false
	}
	bucket := atNanos / b.intervalNanos

	if !b.open {
		b.bucket, b.last, b.open = bucket, price, true
		return 0, false
	}
	if bucket == b.bucket {
		b.last = price
		return 0, false
	}
	// A later interval: whatever we last saw is the close of the old one.
	closed := b.last
	b.bucket, b.last = bucket, price
	return closed, true
}
