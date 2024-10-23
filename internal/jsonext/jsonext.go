// Package jsonext defines common types with JSON marshalers.
package jsonext

import (
	"fmt"
	"time"
)

// Duration adds text/json (un)marshaling functions to time.Duration.
type Duration struct {
	time.Duration
}

// MarshalText writes d as text.
func (d Duration) MarshalText() ([]byte, error) {
	return []byte(d.String()), nil
}

// UnmarshalText read d from a text representation.
func (d *Duration) UnmarshalText(b []byte) error {
	var err error

	d.Duration, err = time.ParseDuration(string(b))
	if err != nil {
		return fmt.Errorf("unmarshaling %s: %w", b, err)
	}

	return nil
}
