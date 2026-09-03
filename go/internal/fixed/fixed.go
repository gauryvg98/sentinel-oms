// Package fixed is the Go side of the engine's money representation.
//
// An int64 at 1e8, exactly as sentinel-types holds it. There is no float64
// anywhere in this package, and nothing above it should introduce one: a
// quantity that has been through a binary float is a rounding error with a
// settlement date.
//
// The string form is what crosses the wire to a browser, because JavaScript has
// one numeric type and it is a float64 — a value sent as a JSON number has
// already been rounded by the time the client parses it.
package fixed

import (
	"errors"
	"fmt"
	"math/bits"
	"strings"
)

// Scale is units per whole. The same 1e8 the Rust side uses.
const Scale int64 = 100_000_000

// ScaleDigits is the number of decimal places Scale represents.
const ScaleDigits = 8

// Errors parsing a decimal.
var (
	// ErrEmpty means there was nothing, or nothing but a sign.
	ErrEmpty = errors.New("empty decimal")
	// ErrMalformed means it was not a plain decimal.
	ErrMalformed = errors.New("malformed decimal")
	// ErrTooPrecise means more decimal places than the scale can hold.
	//
	// Refused rather than rounded. A venue sending more precision than we model
	// is telling us the model is wrong, and truncating the message hides that
	// until settlement.
	ErrTooPrecise = errors.New("more than 8 decimal places")
	// ErrOutOfRange means it does not fit an int64 at this scale.
	ErrOutOfRange = errors.New("out of range")
)

// Dec is a fixed-point decimal in raw 1e8 units.
type Dec int64

// FromRaw wraps raw units.
func FromRaw(raw int64) Dec { return Dec(raw) }

// Raw is the underlying units.
func (d Dec) Raw() int64 { return int64(d) }

// IsZero reports whether the value is zero.
func (d Dec) IsZero() bool { return d == 0 }

// Whole builds from a whole number of units.
func Whole(n int64) Dec { return Dec(n * Scale) }

// Parse reads an exact decimal string.
//
// Accepts "-12", "12.5", ".5", "+0.00000001". Rejects exponents, separators and
// anything with more precision than the scale holds — refused rather than
// rounded, for the same reason the Rust side refuses it.
func Parse(s string) (Dec, error) {
	if s == "" {
		return 0, ErrEmpty
	}
	i := 0
	negative := false
	switch s[0] {
	case '-':
		negative, i = true, 1
	case '+':
		i = 1
	}
	if i >= len(s) {
		return 0, ErrEmpty
	}

	// Unsigned magnitude throughout; the sign is applied once, at the end, so
	// the most negative value round-trips.
	var magnitude int64
	seenDigit := false
	const maxWhole = 92_233_720_368 // floor(2^63 / 1e8)

	for ; i < len(s) && s[i] != '.'; i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, ErrMalformed
		}
		seenDigit = true
		magnitude = magnitude*10 + int64(s[i]-'0')
		if magnitude > maxWhole {
			return 0, ErrOutOfRange
		}
	}
	magnitude *= Scale

	if i < len(s) {
		i++ // consume '.'
		place := Scale
		digits := 0
		for ; i < len(s); i++ {
			if s[i] < '0' || s[i] > '9' {
				return 0, ErrMalformed
			}
			digits++
			if digits > ScaleDigits {
				// A trailing zero beyond our precision carries no value.
				if s[i] != '0' {
					return 0, ErrTooPrecise
				}
				continue
			}
			seenDigit = true
			place /= 10
			magnitude += int64(s[i]-'0') * place
		}
		if digits == 0 {
			return 0, ErrMalformed // "12." promised digits that never came
		}
	}
	if !seenDigit {
		return 0, ErrEmpty
	}
	if negative {
		return Dec(-magnitude), nil
	}
	return Dec(magnitude), nil
}

// MustParse is Parse for constants, and panics on anything else.
func MustParse(s string) Dec {
	d, err := Parse(s)
	if err != nil {
		panic(fmt.Sprintf("fixed.MustParse(%q): %v", s, err))
	}
	return d
}

// String renders the shortest exact decimal.
//
// 100000000 is "1", not "1.00000000". Exactness is about the value, not the
// digit count, and the short form is what crosses the wire.
func (d Dec) String() string {
	negative := d < 0
	// Widened before taking the modulus: the most negative int64 has no
	// positive counterpart.
	magnitude := int64(d)
	var whole, frac int64
	if negative {
		// -(d) would overflow at the bound, so divide first.
		whole = -(magnitude / Scale)
		frac = -(magnitude % Scale)
	} else {
		whole = magnitude / Scale
		frac = magnitude % Scale
	}

	var b strings.Builder
	if negative {
		b.WriteByte('-')
	}
	fmt.Fprintf(&b, "%d", whole)
	if frac != 0 {
		digits := fmt.Sprintf("%08d", frac)
		digits = strings.TrimRight(digits, "0")
		b.WriteByte('.')
		b.WriteString(digits)
	}
	return b.String()
}

// MarshalJSON writes the value as a quoted string.
//
// Liberal in, strict out: the wire always carries a string, so a browser cannot
// silently round it on the way in.
func (d Dec) MarshalJSON() ([]byte, error) {
	return []byte(`"` + d.String() + `"`), nil
}

// UnmarshalJSON reads a quoted string or a bare number.
func (d *Dec) UnmarshalJSON(data []byte) error {
	text := strings.Trim(string(data), `"`)
	parsed, err := Parse(text)
	if err != nil {
		return err
	}
	*d = parsed
	return nil
}

// Add returns the sum.
func (d Dec) Add(other Dec) Dec { return d + other }

// Sub returns the difference.
func (d Dec) Sub(other Dec) Dec { return d - other }

// Neg returns the negation.
func (d Dec) Neg() Dec { return -d }

// Abs returns the magnitude.
func (d Dec) Abs() Dec {
	if d < 0 {
		return -d
	}
	return d
}

// Mul multiplies two fixed-point values, rescaling once.
//
// Truncates toward zero, so a notional is never reported larger than it is.
func (d Dec) Mul(other Dec) Dec { return Dec(MulDiv(int64(d), int64(other), Scale)) }

// MulDiv computes a*b/d exactly, with the product held in 128 bits.
//
// Multiply first, then divide — the order matters. Dividing first loses the
// remainder before it can contribute, which on a cost basis of 328297.5 split
// three ways is half a dollar that never comes back. The Rust side computes
// these in i128 for the same reason, and the two must agree to the unit or the
// gateway shows a number the engine never computed.
//
// Truncates toward zero. Returns 0 when d is 0, and saturates rather than
// wrapping if the quotient does not fit — a wrapped price is worse than an
// obviously pinned one.
func MulDiv(a, b, d int64) int64 {
	if d == 0 {
		return 0
	}
	negative := (a < 0) != (b < 0)
	if d < 0 {
		negative = !negative
		d = -d
	}
	ua, ub := abs64(a), abs64(b)

	hi, lo := bits.Mul64(ua, ub)
	ud := uint64(d)
	if hi >= ud {
		// The quotient does not fit in 64 bits.
		if negative {
			return -9223372036854775808
		}
		return 9223372036854775807
	}
	quotient, _ := bits.Div64(hi, lo, ud)
	if negative {
		if quotient > 9223372036854775808 {
			return -9223372036854775808
		}
		return -int64(quotient)
	}
	if quotient > 9223372036854775807 {
		return 9223372036854775807
	}
	return int64(quotient)
}

// abs64 is the magnitude of an int64 as a uint64, which the most negative value
// needs — its positive counterpart does not fit back into an int64.
func abs64(v int64) uint64 {
	if v < 0 {
		return uint64(-(v + 1)) + 1
	}
	return uint64(v)
}
