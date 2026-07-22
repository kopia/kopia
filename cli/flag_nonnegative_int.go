package cli

import (
	"strconv"

	"github.com/alecthomas/kingpin/v2"
	"github.com/pkg/errors"
)

// nonNegativeIntValue is a kingpin.Value that parses an integer and rejects
// negative values at parse time, so an invalid --parallel=-1 surfaces an error
// instead of panicking once the command runs. Zero is preserved as valid.
type nonNegativeIntValue struct {
	target *int
}

func (v *nonNegativeIntValue) Set(s string) error {
	// Parse the same way kingpin's built-in int flag does (ParseFloat then
	// truncate), so the only behavior change versus the previous IntVar is that
	// negative values are rejected rather than accepted.
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return errors.Wrapf(err, "invalid integer value %q", s)
	}

	n := int(f)

	if n < 0 {
		return errors.Errorf("must not be negative, got %d", n)
	}

	*v.target = n

	return nil
}

func (v *nonNegativeIntValue) String() string {
	return strconv.Itoa(*v.target)
}

// nonNegativeIntVar returns a kingpin.Value that stores a non-negative integer
// into target, rejecting negative inputs.
func nonNegativeIntVar(target *int) kingpin.Value {
	return &nonNegativeIntValue{target}
}
