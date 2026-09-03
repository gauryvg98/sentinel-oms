package strategy

import "github.com/gauryvg98/sentinel-oms/go/internal/fixed"

// Order is one instruction for the control socket.
type Order struct {
	Side string // "buy" or "sell"
	Qty  fixed.Dec
	// Exit is true when this order reduces exposure. It decides which guards
	// apply, so it is stated rather than inferred downstream: an exit is
	// bounded by the reconciled position and is allowed while halted, and an
	// entry is neither.
	Exit bool
}

// Target is the signed position a stance implies at a given size.
//
// Flat is zero and needs no price, so a flatten never blocks on a feed. A short
// on a venue that does not allow shorting clamps to flat rather than erroring —
// the strategy asked for something the venue cannot express, and refusing to be
// in the market is the answer that cannot lose money by accident.
func Target(stance Stance, size fixed.Dec, allowShort bool) (fixed.Dec, bool) {
	switch stance {
	case NoOpinion:
		return 0, false
	case Flat:
		return 0, true
	case Long:
		return size, true
	case Short:
		if !allowShort {
			return 0, true
		}
		return size.Neg(), true
	default:
		return 0, false
	}
}

// Reconcile works out the single order that moves actual toward desired.
//
// One order per evaluation, and never one that crosses zero. A flip from long
// to short is two trades wearing one number: it closes a position and opens an
// opposite one, under different authority and different guards. Emitting it as
// a single order would have the exit guards sized against a position that is
// about to become an entry. So a crossing reconcile flattens first and opens on
// the next bar, which costs a bar and buys an unambiguous audit trail.
//
// A difference smaller than the venue's step is left alone: it cannot be traded
// and re-deciding it every bar would be a stream of rejections.
func Reconcile(desired, actual, step fixed.Dec) (Order, bool) {
	delta := desired.Sub(actual)
	if delta.Abs().Raw() < step.Raw() || delta.IsZero() {
		return Order{}, false
	}

	// Never cross zero in one order.
	if !actual.IsZero() && signOf(desired) != 0 && signOf(desired) != signOf(actual) {
		delta = actual.Neg()
	}

	// Reducing exposure means ending nearer flat than we started.
	reducing := !actual.IsZero() && actual.Add(delta).Abs().Raw() < actual.Abs().Raw()

	side := "buy"
	if delta.Raw() < 0 {
		side = "sell"
	}
	return Order{Side: side, Qty: delta.Abs(), Exit: reducing}, true
}

// RoundToStep truncates a size onto the venue's grid, toward zero.
//
// Down, never up: rounding a target up would ask for exposure nobody decided
// on, and the venue would refuse it anyway.
func RoundToStep(qty, step fixed.Dec) fixed.Dec {
	if step.Raw() <= 0 {
		return qty
	}
	n := qty.Abs().Raw() / step.Raw()
	out := fixed.FromRaw(n * step.Raw())
	if qty.Raw() < 0 {
		return out.Neg()
	}
	return out
}

func signOf(d fixed.Dec) int {
	switch {
	case d.Raw() > 0:
		return 1
	case d.Raw() < 0:
		return -1
	default:
		return 0
	}
}
