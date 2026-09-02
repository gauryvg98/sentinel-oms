package strategy

import (
	"testing"

	"github.com/gauryvg98/sentinel-oms/go/internal/fixed"
)

// feed runs a series of closes and reports the stance after each.
func feed(t *testing.T, s *SmaCross, prices ...string) []Stance {
	t.Helper()
	out := make([]Stance, 0, len(prices))
	for _, p := range prices {
		out = append(out, s.OnBar(fixed.MustParse(p)).Stance)
	}
	return out
}

func build(t *testing.T, fast, slow int, short bool) *SmaCross {
	t.Helper()
	s, err := New(Config{
		Fast: fast, Slow: slow, Short: short,
		StopFloorPct: fixed.MustParse("0.005"),
		StopCapPct:   fixed.MustParse("0.01"),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

func TestAPeriodPairThatCouldNeverCrossIsRefused(t *testing.T) {
	for _, c := range []Config{{Fast: 5, Slow: 5}, {Fast: 6, Slow: 5}, {Fast: 0, Slow: 5}} {
		if _, err := New(c); err == nil {
			t.Errorf("fast=%d slow=%d was accepted", c.Fast, c.Slow)
		}
	}
}

func TestNoOpinionUntilTheSlowWindowIsFull(t *testing.T) {
	s := build(t, 2, 4, false)
	for i, got := range feed(t, s, "10", "11", "12") {
		if got != NoOpinion {
			t.Errorf("bar %d = %v, want NoOpinion", i, got)
		}
	}
	// The runner must do nothing here — a warm-up that returned FLAT would
	// close a position the operator opened deliberately.
	d := s.OnBar(fixed.MustParse("13"))
	if d.Stance != Long && d.Stance != Flat {
		t.Errorf("the fourth bar fills the window, got %v", d.Stance)
	}
}

func TestWantsLongInAnUptrendAndStaysLong(t *testing.T) {
	s := build(t, 2, 4, false)
	got := feed(t, s, "10", "10", "10", "10", "11", "12", "13", "14")
	if got[len(got)-1] != Long || got[len(got)-2] != Long {
		t.Errorf("tail = %v %v, want Long Long — this is a target position, not an edge",
			got[len(got)-2], got[len(got)-1])
	}
}

func TestWantsFlatInADowntrend(t *testing.T) {
	s := build(t, 2, 4, false)
	got := feed(t, s, "20", "20", "20", "20", "18", "16", "14", "12")
	if last := got[len(got)-1]; last != Flat {
		t.Errorf("last = %v, want Flat", last)
	}
}

func TestFlipsLongToFlatAcrossAReversal(t *testing.T) {
	s := build(t, 2, 4, false)
	got := feed(t, s, "10", "10", "10", "10", "12", "14", "16", "14", "12", "10", "8")

	lastLong := -1
	for i, st := range got {
		if st == Long {
			lastLong = i
		}
	}
	if lastLong < 0 {
		t.Fatalf("never went long: %v", got)
	}
	flat := false
	for _, st := range got[lastLong+1:] {
		if st == Flat {
			flat = true
		}
	}
	if !flat {
		t.Errorf("no Flat after the last Long: %v", got)
	}
}

func TestTheLongShortVariantShortsBelowTheCross(t *testing.T) {
	plain := feed(t, build(t, 2, 4, false), "10", "11", "12", "5", "4", "3")
	ls := feed(t, build(t, 2, 4, true), "10", "11", "12", "5", "4", "3")

	has := func(in []Stance, want Stance) bool {
		for _, s := range in {
			if s == want {
				return true
			}
		}
		return false
	}
	if !has(plain, Flat) || has(plain, Short) {
		t.Errorf("plain = %v, want Flat and never Short", plain)
	}
	if !has(ls, Short) || has(ls, Flat) {
		t.Errorf("long-short = %v, want Short and never Flat", ls)
	}
}

func TestTheStopIsFlooredAndCapped(t *testing.T) {
	s := build(t, 2, 4, false)
	// A cross that has only just happened: price sits almost on the slow
	// average, so the raw gap is nearly nothing.
	feed(t, s, "100", "100", "100", "100")
	d := s.OnBar(fixed.MustParse("100.01"))
	floor := fixed.MustParse("100.01").Mul(fixed.MustParse("0.005"))
	if d.StopDist.Raw() < floor.Raw() {
		t.Errorf("stop %v is under the floor %v — a near-zero stop sizes into the leverage cap",
			d.StopDist, floor)
	}

	// A trend that has run a long way from its average.
	far := build(t, 2, 4, false)
	feed(t, far, "100", "100", "100", "100")
	d = far.OnBar(fixed.MustParse("200"))
	if capped := fixed.MustParse("200").Mul(fixed.MustParse("0.01")); d.StopDist.Raw() > capped.Raw() {
		t.Errorf("stop %v exceeds the cap %v — trend-following cuts losses short",
			d.StopDist, capped)
	}
}

func TestFlatCarriesNoStop(t *testing.T) {
	s := build(t, 2, 4, false)
	got := feed(t, s, "20", "20", "20", "20", "18", "16")
	if got[len(got)-1] != Flat {
		t.Fatalf("expected Flat, got %v", got[len(got)-1])
	}
	if d := s.OnBar(fixed.MustParse("14")); !d.StopDist.IsZero() {
		t.Errorf("flat carries stop %v; there is no exposure to protect", d.StopDist)
	}
}

func TestTheSameBarsGiveTheSameStances(t *testing.T) {
	// Purity is what makes a replay meaningful and a backtest honest.
	prices := []string{"10", "12", "11", "13", "15", "14", "16", "12", "9", "8"}
	a := feed(t, build(t, 3, 5, true), prices...)
	b := feed(t, build(t, 3, 5, true), prices...)
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("bar %d: %v then %v", i, a[i], b[i])
		}
	}
}

func TestResetForgetsTheOldTimeframe(t *testing.T) {
	s := build(t, 2, 4, false)
	feed(t, s, "10", "10", "10", "10")
	s.Reset()
	// Warming up again: blending two timeframes' closes would average
	// something that describes neither.
	if d := s.OnBar(fixed.MustParse("50")); d.Stance != NoOpinion {
		t.Errorf("after Reset = %v, want NoOpinion", d.Stance)
	}
}
