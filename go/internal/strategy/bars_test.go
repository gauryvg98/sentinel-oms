package strategy

import (
	"testing"

	"github.com/gauryvg98/sentinel-oms/go/internal/fixed"
)

const minute = uint64(60_000_000_000)

func TestABarClosesOnTheFirstMarkOfTheNextInterval(t *testing.T) {
	b := NewBars(minute)
	if _, ok := b.Observe(0, fixed.MustParse("100")); ok {
		t.Error("the first mark cannot close anything")
	}
	if _, ok := b.Observe(30*1_000_000_000, fixed.MustParse("101")); ok {
		t.Error("still inside the first minute")
	}
	close, ok := b.Observe(minute+1, fixed.MustParse("200"))
	if !ok {
		t.Fatal("crossing into the next minute must close a bar")
	}
	if close != fixed.MustParse("101") {
		t.Errorf("close = %v, want 101 — the last mark seen in the interval", close)
	}
}

func TestAnIntervalWithNoMarksIsSkippedNotInvented(t *testing.T) {
	b := NewBars(minute)
	b.Observe(0, fixed.MustParse("100"))
	// Nothing at all for three minutes, then one mark.
	close, ok := b.Observe(3*minute+5, fixed.MustParse("140"))
	if !ok {
		t.Fatal("the pending bar should close")
	}
	if close != fixed.MustParse("100") {
		t.Errorf("close = %v, want 100", close)
	}
	// The two empty minutes produce nothing: a bar this never saw is not a bar
	// whose price it may make up.
	if _, ok := b.Observe(3*minute+6, fixed.MustParse("141")); ok {
		t.Error("an empty interval was invented")
	}
}

func TestTheBoundaryComesFromTheMarkNotAClock(t *testing.T) {
	// The same timestamps must give the same bars however long the replay
	// takes, which is what makes a backtest honest.
	run := func() []fixed.Dec {
		b := NewBars(minute)
		var out []fixed.Dec
		for i, p := range []string{"10", "11", "12", "13", "14"} {
			at := uint64(i) * 40 * 1_000_000_000 // 40s apart: straddles minutes
			if c, ok := b.Observe(at, fixed.MustParse(p)); ok {
				out = append(out, c)
			}
		}
		return out
	}
	a, c := run(), run()
	if len(a) != len(c) {
		t.Fatalf("%d bars then %d", len(a), len(c))
	}
	for i := range a {
		if a[i] != c[i] {
			t.Errorf("bar %d: %v then %v", i, a[i], c[i])
		}
	}
	if len(a) == 0 {
		t.Fatal("no bars closed at all")
	}
}
