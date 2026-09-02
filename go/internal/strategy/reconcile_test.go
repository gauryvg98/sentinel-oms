package strategy

import (
	"testing"

	"github.com/gauryvg98/sentinel-oms/go/internal/fixed"
)

var step = fixed.MustParse("0.001")

func dec(s string) fixed.Dec { return fixed.MustParse(s) }

func TestNoOpinionProducesNoTarget(t *testing.T) {
	if _, ok := Target(NoOpinion, dec("0.01"), true); ok {
		t.Error("warming up must not produce a target — it would flatten a live position")
	}
}

func TestShortClampsToFlatWhereShortingIsNotAllowed(t *testing.T) {
	got, ok := Target(Short, dec("0.01"), false)
	if !ok || !got.IsZero() {
		t.Errorf("got %v ok=%v, want flat", got, ok)
	}
	if got, _ := Target(Short, dec("0.01"), true); got != dec("-0.01") {
		t.Errorf("got %v, want -0.01 where shorting is allowed", got)
	}
}

func TestAnAlreadyCorrectPositionProducesNoOrder(t *testing.T) {
	if _, ok := Reconcile(dec("0.01"), dec("0.01"), step); ok {
		t.Error("ordered against a position that already matches")
	}
}

func TestADifferenceBelowTheStepIsLeftAlone(t *testing.T) {
	if _, ok := Reconcile(dec("0.0105"), dec("0.01"), step); ok {
		t.Error("a difference the venue cannot express should not be traded")
	}
}

func TestOpeningFromFlatIsAnEntry(t *testing.T) {
	o, ok := Reconcile(dec("0.01"), 0, step)
	if !ok || o.Side != "buy" || o.Qty != dec("0.01") || o.Exit {
		t.Errorf("got %+v, want a buy 0.01 entry", o)
	}
}

func TestClosingToFlatIsAnExit(t *testing.T) {
	o, ok := Reconcile(0, dec("0.01"), step)
	if !ok || o.Side != "sell" || o.Qty != dec("0.01") || !o.Exit {
		t.Errorf("got %+v, want a sell 0.01 exit", o)
	}
	o, ok = Reconcile(0, dec("-0.01"), step)
	if !ok || o.Side != "buy" || !o.Exit {
		t.Errorf("got %+v, want a buy exit to close a short", o)
	}
}

func TestTrimmingAPositionIsAnExitAndAddingIsAnEntry(t *testing.T) {
	if o, _ := Reconcile(dec("0.005"), dec("0.01"), step); !o.Exit || o.Side != "sell" {
		t.Errorf("trim = %+v, want a sell exit", o)
	}
	if o, _ := Reconcile(dec("0.02"), dec("0.01"), step); o.Exit || o.Side != "buy" {
		t.Errorf("add = %+v, want a buy entry", o)
	}
}

func TestAFlipFlattensFirstRatherThanCrossingZero(t *testing.T) {
	// Long 0.01 wanting short 0.01: the single order is the flatten, not 0.02.
	o, ok := Reconcile(dec("-0.01"), dec("0.01"), step)
	if !ok {
		t.Fatal("a flip must produce an order")
	}
	if o.Qty != dec("0.01") || o.Side != "sell" || !o.Exit {
		t.Errorf("got %+v, want a sell 0.01 exit — a flip is two trades, not one", o)
	}
	// Next evaluation, now flat, opens the other side as an entry.
	o, ok = Reconcile(dec("-0.01"), 0, step)
	if !ok || o.Side != "sell" || o.Exit {
		t.Errorf("second leg = %+v, want a sell entry", o)
	}
}

func TestRoundingIsAlwaysTowardZero(t *testing.T) {
	if got := RoundToStep(dec("0.0129"), step); got != dec("0.012") {
		t.Errorf("got %v, want 0.012", got)
	}
	if got := RoundToStep(dec("-0.0129"), step); got != dec("-0.012") {
		t.Errorf("got %v, want -0.012 — magnitude never rounds up", got)
	}
	if got := RoundToStep(dec("0.0009"), step); !got.IsZero() {
		t.Errorf("got %v, want 0 — below one step there is nothing to trade", got)
	}
}
