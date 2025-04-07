package fs

import (
	"encoding/json"
	"time"

	"github.com/pkg/errors"
)

// UTCTimestamp stores the UTC timestamp in nanoseconds and provides JSON serializability.
//
//nolint:recvcheck
type UTCTimestamp int64

// UnmarshalJSON implements json.Unmarshaler.
func (u *UTCTimestamp) UnmarshalJSON(v []byte) error {
	var t time.Time

	if err := t.UnmarshalJSON(v); err != nil {
		return errors.Wrap(err, "unable to unmarshal time")
	}

	*u = UTCTimestamp(t.UnixNano())

	return nil
}

// MarshalJSON implements json.Marshaler.
func (u UTCTimestamp) MarshalJSON() ([]byte, error) {
	//nolint:wrapcheck
	return u.ToTime().UTC().MarshalJSON()
}

// ToTime returns time.Time representation of the time.
func (u UTCTimestamp) ToTime() time.Time {
	return time.Unix(0, int64(u))
}

// Add adds the specified duration to UTC time.
func (u UTCTimestamp) Add(dur time.Duration) UTCTimestamp {
	return u + UTCTimestamp(dur)
}

// Sub returns the difference between two specified durations.
func (u UTCTimestamp) Sub(u2 UTCTimestamp) time.Duration {
	return time.Duration(u - u2)
}

// After returns true if the timestamp is after another timestamp.
func (u UTCTimestamp) After(other UTCTimestamp) bool {
	return u > other
}

// Before returns true if the timestamp is before another timestamp.
func (u UTCTimestamp) Before(other UTCTimestamp) bool {
	return u < other
}

// Equal returns true if the timestamp is equal to another timestamp.
func (u UTCTimestamp) Equal(other UTCTimestamp) bool {
	return u == other
}

// Format formats the timestamp according to the provided layout.
func (u UTCTimestamp) Format(layout string) string {
	return u.ToTime().UTC().Format(layout)
}

// UTCTimestampFromTime converts time.Time to UTCTimestamp.
func UTCTimestampFromTime(t time.Time) UTCTimestamp {
	return UTCTimestamp(t.UnixNano())
}

var (
	_ json.Marshaler   = UTCTimestamp(0)
	_ json.Unmarshaler = (*UTCTimestamp)(nil)
)
