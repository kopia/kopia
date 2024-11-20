// Package jsonencoding defines common types with JSON marshalers.
package jsonencoding

import (
	"bytes"
	"fmt"
	"strconv"
	"time"
)

// Duration adds text/json (un)marshaling functions to time.Duration.
type Duration struct { //nolint: recvcheck
	time.Duration
}

// MarshalText writes d as text.
func (d Duration) MarshalText() ([]byte, error) {
	return []byte(d.String()), nil
}

// UnmarshalText read d from a text representation.
func (d *Duration) UnmarshalText(b []byte) error {
	s := string(bytes.TrimSpace(b))

	f, err := strconv.ParseFloat(s, 64)
	if err == nil {
		d.Duration = time.Duration(f)

		return nil
	}

	d.Duration, err = time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %s: %w", s, err)
	}

	return nil
}
