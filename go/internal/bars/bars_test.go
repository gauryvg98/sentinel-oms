package bars

import (
	"testing"

	"github.com/gauryvg98/sentinel-oms/go/internal/fixed"
)

const minute = uint64(60_000_000_000)

func TestOneCandlePerIntervalWithTheExtremesSeen(t *testing.T) {
	s := NewSeries(minute, 100)
	// Four marks inside one minute: 100 opens, 120 is the high, 90 the low,
	// 110 closes.
	for i, p := range []string{"100", "120", "90", "110"} {
		s.Observe(uint64(i)*10*1_000_000_000, fixed.MustParse(p))
	}
	if s.Len() != 1 {
		t.Fatalf("got %d candles, want 1", s.Len())
	}
	b := s.Bars()[0]
	if b.Open != fixed.MustParse("100") || b.High != fixed.MustParse("120") ||
		b.Low != fixed.MustParse("90") || b.Close != fixed.MustParse("110") {
		t.Errorf("got %+v, want O100 H120 L90 C110", b)
	}
}

func TestALaterMarkStartsANewCandle(t *testing.T) {
	s := NewSeries(minute, 100)
	s.Observe(0, fixed.MustParse("100"))
	s.Observe(minute+1, fixed.MustParse("200"))
	if s.Len() != 2 {
		t.Fatalf("got %d candles, want 2", s.Len())
	}
	if got := s.Bars()[1].Open; got != fixed.MustParse("200") {
		t.Errorf("second candle opens at %v, want 200", got)
	}
}

func TestAnEmptyIntervalIsNotInvented(t *testing.T) {
	s := NewSeries(minute, 100)
	s.Observe(0, fixed.MustParse("100"))
	// Nothing for three minutes.
	s.Observe(3*minute+1, fixed.MustParse("140"))
	if s.Len() != 2 {
		t.Errorf("got %d candles, want 2 — a candle nobody saw is not a candle", s.Len())
	}
}

func TestTheSeriesIsBounded(t *testing.T) {
	s := NewSeries(minute, 5)
	for i := 0; i < 50; i++ {
		s.Observe(uint64(i)*minute, fixed.MustParse("100"))
	}
	if s.Len() != 5 {
		t.Errorf("got %d candles, want the cap of 5", s.Len())
	}
	// And it keeps the newest, not the oldest.
	if got := s.Bars()[4].Time; got != int64(49*60) {
		t.Errorf("last candle at %d, want %d", got, 49*60)
	}
}

func TestCandleTimesComeFromTheMarkNotAClock(t *testing.T) {
	one, two := NewSeries(minute, 100), NewSeries(minute, 100)
	for i, p := range []string{"10", "11", "12", "13"} {
		at := uint64(i) * 40 * 1_000_000_000
		one.Observe(at, fixed.MustParse(p))
		two.Observe(at, fixed.MustParse(p))
	}
	a, b := one.Bars(), two.Bars()
	if len(a) != len(b) {
		t.Fatalf("%d then %d candles", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Errorf("candle %d differs: %+v vs %+v", i, a[i], b[i])
		}
	}
}

func TestSeedFillsAnEmptySeriesAndThenLeavesItAlone(t *testing.T) {
	s := NewSeries(minute, 100)
	history := []Bar{
		{Time: 0, Open: fixed.MustParse("10"), High: fixed.MustParse("11"),
			Low: fixed.MustParse("9"), Close: fixed.MustParse("10.5")},
		{Time: 60, Open: fixed.MustParse("10.5"), High: fixed.MustParse("12"),
			Low: fixed.MustParse("10"), Close: fixed.MustParse("11")},
	}
	s.Seed(history)
	if s.Len() != 2 {
		t.Fatalf("got %d candles, want 2", s.Len())
	}

	// A second seed must not double the history.
	s.Seed(history)
	if s.Len() != 2 {
		t.Errorf("got %d after a second seed, want 2 — backfill fills what was "+
			"never seen, it does not overwrite what was", s.Len())
	}
}

func TestAMarkAfterSeedingContinuesRatherThanRestarting(t *testing.T) {
	s := NewSeries(minute, 100)
	s.Seed([]Bar{{Time: 0, Open: fixed.MustParse("10"), High: fixed.MustParse("10"),
		Low: fixed.MustParse("10"), Close: fixed.MustParse("10")}})

	// A mark inside the seeded bucket extends it rather than opening a second
	// candle for the same minute.
	s.Observe(30*1_000_000_000, fixed.MustParse("12"))
	if s.Len() != 1 {
		t.Fatalf("got %d candles, want 1", s.Len())
	}
	if got := s.Bars()[0].High; got != fixed.MustParse("12") {
		t.Errorf("high = %v, want 12", got)
	}
}

func TestTheSeriesNeverGoesBackwards(t *testing.T) {
	// The gateway seeds from the venue up to now, then replays a journal that
	// starts hours earlier. Appending those older marks drove the series
	// backwards, and a chart given descending times renders nothing — the
	// whole screen went blank over one stale record.
	s := NewSeries(minute, 100)
	s.Seed([]Bar{
		{Time: 600, Open: fixed.MustParse("10"), High: fixed.MustParse("10"),
			Low: fixed.MustParse("10"), Close: fixed.MustParse("10")},
		{Time: 660, Open: fixed.MustParse("11"), High: fixed.MustParse("11"),
			Low: fixed.MustParse("11"), Close: fixed.MustParse("11")},
	})

	// A mark from long before the seeded history.
	s.Observe(60*1_000_000_000, fixed.MustParse("99"))
	// And one from after it, which must still be taken.
	s.Observe(720*1_000_000_000, fixed.MustParse("12"))

	times := make([]int64, 0, s.Len())
	for _, b := range s.Bars() {
		times = append(times, b.Time)
	}
	for i := 1; i < len(times); i++ {
		if times[i] <= times[i-1] {
			t.Fatalf("times go backwards at %d: %v", i, times)
		}
	}
	if len(times) != 3 || times[2] != 720 {
		t.Errorf("got %v, want the two seeded candles and the newer mark", times)
	}
}
