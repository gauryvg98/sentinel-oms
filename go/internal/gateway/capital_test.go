package gateway

import (
	"testing"

	"github.com/gauryvg98/sentinel-oms/go/internal/book"
	"github.com/gauryvg98/sentinel-oms/go/internal/fixed"
)

func dec(s string) fixed.Dec { return fixed.MustParse(s) }

func TestCapitalIsNotionalOverLeverage(t *testing.T) {
	g := New(Config{Leverage: "100"})
	positions := []PositionView{{
		Position:   book.Position{Instrument: "BTCUSDC", Qty: dec("0.002")},
		Mark:       dec("81500"),
		Unrealized: dec("-0.39"),
	}}

	c := g.capital(positions)
	if want := dec("163"); c.Notional != want {
		t.Errorf("notional = %v, want %v", c.Notional, want)
	}
	// 163 at 100x is 1.63 of margin.
	if want := dec("1.63"); c.Committed != want {
		t.Errorf("committed = %v, want %v", c.Committed, want)
	}
}

func TestAShortCommitsMarginJustLikeALong(t *testing.T) {
	g := New(Config{Leverage: "100"})
	c := g.capital([]PositionView{{
		Position: book.Position{Instrument: "BTCUSDC", Qty: dec("-0.002")},
		Mark:     dec("81500"),
	}})
	if want := dec("1.63"); c.Committed != want {
		t.Errorf("committed = %v, want %v — direction does not change the margin",
			c.Committed, want)
	}
}

func TestEquityAndAvailableFollowFromTheRest(t *testing.T) {
	g := New(Config{Leverage: "100"})
	g.book.SetBalanceForTest("USDC", dec("1000"))

	c := g.capital([]PositionView{{
		Position:   book.Position{Instrument: "BTCUSDC", Qty: dec("0.002")},
		Mark:       dec("81500"),
		Unrealized: dec("-0.5"),
	}})
	if want := dec("999.5"); c.Equity != want {
		t.Errorf("equity = %v, want %v (wallet plus what is open)", c.Equity, want)
	}
	if want := dec("997.87"); c.Available != want {
		t.Errorf("available = %v, want %v (equity less committed)", c.Available, want)
	}
}

func TestFlatCommitsNothing(t *testing.T) {
	g := New(Config{Leverage: "100"})
	c := g.capital(nil)
	if !c.Committed.IsZero() || !c.Notional.IsZero() || !c.Blocked.IsZero() {
		t.Errorf("flat should commit nothing, got %+v", c)
	}
}
