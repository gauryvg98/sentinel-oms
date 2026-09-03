package fixed

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestParsesTheFormsVenuesSend(t *testing.T) {
	cases := map[string]int64{
		"0":           0,
		"1":           Scale,
		"-1":          -Scale,
		"+1":          Scale,
		"12.5":        1_250_000_000,
		".5":          50_000_000,
		"0.00000001":  1,
		"-0.00000001": -1,
		"109432.5":    10_943_250_000_000,
	}
	for text, want := range cases {
		got, err := Parse(text)
		if err != nil {
			t.Errorf("Parse(%q): %v", text, err)
			continue
		}
		if got.Raw() != want {
			t.Errorf("Parse(%q) = %d, want %d", text, got.Raw(), want)
		}
	}
}

func TestRefusesRatherThanRounds(t *testing.T) {
	cases := map[string]error{
		"1.234567891":   ErrTooPrecise,
		"1e5":           ErrMalformed,
		"1,000":         ErrMalformed,
		"12.":           ErrMalformed,
		"":              ErrEmpty,
		"-":             ErrEmpty,
		".":             ErrMalformed,
		"99999999999.0": ErrOutOfRange,
	}
	for text, want := range cases {
		_, err := Parse(text)
		if !errors.Is(err, want) {
			t.Errorf("Parse(%q) error = %v, want %v", text, err, want)
		}
	}
}

func TestTrailingZerosBeyondPrecisionAreNotALoss(t *testing.T) {
	// Venues pad. "1.500000000000" is 1.5 and nothing has been discarded.
	a := MustParse("1.500000000000")
	b := MustParse("1.5")
	if a != b {
		t.Errorf("%v != %v", a, b)
	}
	if got := MustParse("0.000000000"); got != 0 {
		t.Errorf("got %v, want 0", got)
	}
}

func TestDisplayRoundTripsThroughParse(t *testing.T) {
	// Including the bounds: the most negative value has no positive
	// counterpart, and a naive implementation loses it.
	for _, raw := range []int64{
		0, 1, -1, Scale, -Scale, 1_250_000_000, 10_943_250_000_000,
		9223372036854775807, -9223372036854775808,
	} {
		d := FromRaw(raw)
		back, err := Parse(d.String())
		if err != nil {
			t.Errorf("Parse(%q): %v", d.String(), err)
			continue
		}
		if back != d {
			t.Errorf("round trip of %d gave %d via %q", raw, back.Raw(), d.String())
		}
	}
}

func TestDisplayIsTheShortForm(t *testing.T) {
	cases := map[int64]string{
		Scale:          "1",
		1_250_000_000:  "12.5",
		1:              "0.00000001",
		-1:             "-0.00000001",
		0:              "0",
		-1_250_000_000: "-12.5",
	}
	for raw, want := range cases {
		if got := FromRaw(raw).String(); got != want {
			t.Errorf("FromRaw(%d).String() = %q, want %q", raw, got, want)
		}
	}
}

func TestNumbersCrossTheWireAsStrings(t *testing.T) {
	// JavaScript has one numeric type and it is a float64. A value sent as a
	// JSON number has already been rounded by the time the client parses it.
	payload, err := json.Marshal(struct {
		Qty Dec `json:"qty"`
	}{MustParse("0.00000001")})
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != `{"qty":"0.00000001"}` {
		t.Errorf("marshalled to %s", payload)
	}

	var back struct {
		Qty Dec `json:"qty"`
	}
	if err := json.Unmarshal(payload, &back); err != nil {
		t.Fatal(err)
	}
	if back.Qty != MustParse("0.00000001") {
		t.Errorf("round tripped to %v", back.Qty)
	}
}

func TestMultiplicationMatchesTheRustSide(t *testing.T) {
	// Three BTCUSD contracts at 109,432.50. If this and Price::notional
	// disagree, the gateway shows a number the engine never computed.
	got := MustParse("109432.5").Mul(Whole(3))
	if got.String() != "328297.5" {
		t.Errorf("got %v, want 328297.5", got)
	}
}

func TestMultiplicationTruncatesTowardZero(t *testing.T) {
	half := MustParse("0.5")
	if got := half.Mul(FromRaw(1)); got != 0 {
		t.Errorf("got %v, want 0", got)
	}
	if got := half.Mul(FromRaw(-1)); got != 0 {
		t.Errorf("got %v, want 0", got)
	}
	if got := half.Mul(FromRaw(2)); got != FromRaw(1) {
		t.Errorf("got %v, want 1 raw unit", got)
	}
}

func TestArithmeticIsPlain(t *testing.T) {
	if got := Whole(2).Sub(Whole(3)); got != Whole(-1) {
		t.Errorf("got %v", got)
	}
	if got := Whole(-3).Abs(); got != Whole(3) {
		t.Errorf("got %v", got)
	}
	if got := MustParse("0.1").Add(MustParse("0.2")); got.String() != "0.3" {
		// The one every float gets wrong.
		t.Errorf("0.1 + 0.2 = %v", got)
	}
}
