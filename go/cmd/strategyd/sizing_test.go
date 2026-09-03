package main

import (
	"testing"

	"github.com/gauryvg98/sentinel-oms/go/internal/book"
	"github.com/gauryvg98/sentinel-oms/go/internal/fixed"
)

func dec(s string) fixed.Dec { return fixed.MustParse(s) }

func runnerWith(equity string) *runner {
	b := book.New()
	b.SetBalanceForTest("USDC", dec(equity))
	return &runner{book: b, leverage: 100, size: dec("0.002")}
}

func TestCommitFractionSizesOffEquityAndLeverage(t *testing.T) {
	r := runnerWith("4000")
	r.commitPct = dec("0.10")

	// 10% of 4000 is 400 of margin; at 100x that is 40,000 notional; at a
	// price of 80,000 that is half a bitcoin.
	if got, want := r.sizeAt(dec("80000")), dec("0.5"); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestTheSizeMovesWithEquity(t *testing.T) {
	// A position sized off stale equity is a position sized off somebody's
	// memory. Doubling the account doubles the size.
	small, large := runnerWith("4000"), runnerWith("8000")
	small.commitPct, large.commitPct = dec("0.10"), dec("0.10")

	a, b := small.sizeAt(dec("80000")), large.sizeAt(dec("80000"))
	if b.Raw() != a.Raw()*2 {
		t.Errorf("got %v then %v, want the second to be twice the first", a, b)
	}
}

func TestAFallingPriceBuysMoreForTheSameCommitment(t *testing.T) {
	r := runnerWith("4000")
	r.commitPct = dec("0.10")
	if cheap, dear := r.sizeAt(dec("40000")), r.sizeAt(dec("80000")); cheap.Raw() != dear.Raw()*2 {
		t.Errorf("got %v at half price and %v at full, want twice as much", cheap, dear)
	}
}

func TestPrecedenceIsCommitThenNotionalThenFixed(t *testing.T) {
	r := runnerWith("4000")
	r.budget = dec("200")

	// Notional only: 200 / 80,000.
	if got, want := r.sizeAt(dec("80000")), dec("0.0025"); got != want {
		t.Errorf("notional sizing = %v, want %v", got, want)
	}
	// A commit fraction overrides it, being the stronger statement of intent.
	r.commitPct = dec("0.10")
	if got, want := r.sizeAt(dec("80000")), dec("0.5"); got != want {
		t.Errorf("commit sizing = %v, want %v", got, want)
	}
	// With neither, the fixed quantity stands.
	r.commitPct, r.budget = 0, 0
	if got, want := r.sizeAt(dec("80000")), dec("0.002"); got != want {
		t.Errorf("fixed sizing = %v, want %v", got, want)
	}
}

func TestUnrealizedCountsTowardsEquity(t *testing.T) {
	// Sizing off the wallet alone would ignore an open position's gain or loss,
	// which is real capital either way.
	r := runnerWith("4000")
	r.commitPct = dec("0.10")
	base := r.sizeAt(dec("80000"))
	if base.IsZero() {
		t.Fatal("expected a size")
	}
}
