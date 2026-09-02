// Package strategy is the part the OMS deliberately does not contain.
//
// A strategy declares a desired STANCE each closed bar; the runner reconciles
// the actual position to it. That target-position model — rather than acting on
// signal edges — is what makes it crash-safe: on restart you bring actual into
// line with desired and a pre-existing position is accounted for by
// construction, with no memory of which edge created it.
//
// Strategies here are pure and deterministic: a bar in, a stance out, no I/O,
// no clock, no randomness. That is what makes them unit-testable and replayable
// — feed the same bars, get the same stances. Orders reach the venue through
// the same control socket a human uses, so a strategy faces exactly the guards
// a human does. A bad strategy trades badly; it cannot corrupt state.
package strategy

import "github.com/gauryvg98/sentinel-oms/go/internal/fixed"

// Stance is the exposure a strategy wants to hold.
type Stance int

const (
	// NoOpinion is warming up: the runner does nothing at all, so a warm-up
	// never flattens a position that is already open.
	NoOpinion Stance = iota
	// Flat wants no exposure.
	Flat
	// Long wants positive exposure.
	Long
	// Short wants negative exposure. Perps only; a spot venue clamps it flat.
	Short
)

func (s Stance) String() string {
	switch s {
	case Long:
		return "LONG"
	case Short:
		return "SHORT"
	case Flat:
		return "FLAT"
	default:
		return "NONE"
	}
}

// Decision is one bar's answer.
type Decision struct {
	Stance Stance
	// Fast and Slow are the two averages, for the operator and the chart.
	Fast, Slow fixed.Dec
	// StopDist is how far price is from the trend line, floored and capped.
	// Zero when there is no directional stance to protect.
	StopDist fixed.Dec
	// Have and Need are non-zero only while warming up.
	Have, Need int
}

// SmaCross is the reference strategy: fast average above slow means hold long,
// below means flat — or short, if this is a venue that allows it.
//
// The point of this system is execution integrity, not alpha. The rule is
// deliberately the simplest thing that still has to be executed correctly.
type SmaCross struct {
	fast, slow int
	short      bool
	stopFloor  fixed.Dec
	stopCap    fixed.Dec
	prices     []fixed.Dec
}

// Config is how an SmaCross is built.
type Config struct {
	Fast, Slow int
	// Short turns the rule into stop-and-reverse: short below the cross rather
	// than flat, so it is always in the market.
	Short bool
	// StopFloorPct keeps a near-cross entry from handing the risk layer a stop
	// of almost nothing, which would size itself straight into the leverage cap.
	StopFloorPct fixed.Dec
	// StopCapPct keeps a trend that has run far from its average from producing
	// a 3-5% stop. Trend-following cuts losses short. Zero disables the cap and
	// uses the raw distance to the slow average.
	StopCapPct fixed.Dec
}

// New builds a strategy, or reports why the configuration is not one.
func New(c Config) (*SmaCross, error) {
	if c.Fast < 1 {
		return nil, ErrConfig{"fast period must be at least one bar"}
	}
	if c.Fast >= c.Slow {
		return nil, ErrConfig{"fast period must be shorter than slow"}
	}
	return &SmaCross{
		fast:      c.Fast,
		slow:      c.Slow,
		short:     c.Short,
		stopFloor: c.StopFloorPct,
		stopCap:   c.StopCapPct,
		prices:    make([]fixed.Dec, 0, c.Slow),
	}, nil
}

// ErrConfig is a strategy that could never have run.
type ErrConfig struct{ Why string }

func (e ErrConfig) Error() string { return "strategy: " + e.Why }

// Name identifies the strategy in a log.
func (s *SmaCross) Name() string {
	if s.short {
		return "sma-ls"
	}
	return "sma-cross"
}

// Reset drops every accumulated bar.
//
// The indicator has to forget when the timeframe changes, or the average blends
// two timeframes' closes into a number that describes neither.
func (s *SmaCross) Reset() { s.prices = s.prices[:0] }

// OnBar takes one closed bar and returns the stance it now wants.
func (s *SmaCross) OnBar(close fixed.Dec) Decision {
	s.prices = append(s.prices, close)
	if len(s.prices) > s.slow {
		s.prices = s.prices[len(s.prices)-s.slow:]
	}
	if len(s.prices) < s.slow {
		return Decision{Stance: NoOpinion, Have: len(s.prices), Need: s.slow}
	}

	fast := mean(s.prices[len(s.prices)-s.fast:])
	slow := mean(s.prices)

	stance := Flat
	if s.short {
		stance = Short
	}
	if fast.Raw() > slow.Raw() {
		stance = Long
	}

	d := Decision{Stance: stance, Fast: fast, Slow: slow}
	if stance == Long || stance == Short {
		// Anchored on the slow average — the trend line a long is wrong once
		// price falls back to — then floored and capped into a tight band, so
		// size and stop stay consistent however far the trend has run.
		gap := close.Sub(slow).Abs()
		if floor := close.Mul(s.stopFloor); floor.Raw() > gap.Raw() {
			gap = floor
		}
		if s.stopCap.Raw() > 0 {
			if cap := close.Mul(s.stopCap); cap.Raw() < gap.Raw() {
				gap = cap
			}
		}
		d.StopDist = gap
	}
	return d
}

// mean averages exactly, without going through float.
//
// The sum of a window of prices cannot overflow int64 at any size this accepts:
// a thousand bars of a million-dollar price is 1e17 raw, well inside the range.
func mean(xs []fixed.Dec) fixed.Dec {
	var total int64
	for _, x := range xs {
		total += x.Raw()
	}
	return fixed.FromRaw(total / int64(len(xs)))
}
