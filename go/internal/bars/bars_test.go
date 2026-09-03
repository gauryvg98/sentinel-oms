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
