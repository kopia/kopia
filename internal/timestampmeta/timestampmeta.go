// Package timestampmeta provides utilities for preserving timestamps
// using per-blob key-value-pairs (metadata, tags, etc.)
package timestampmeta

import (
	"strconv"
	"time"
)

// ToMap returns a map containing single entry representing the provided time or nil map
// if the time is zero. The key-value pair map should be stored alongside the blob.
func ToMap(t time.Time, mapKey string) map[string]string {
	if t.IsZero() {
		return nil
	}

	return map[string]string{
		mapKey: strconv.FormatInt(t.UnixNano(), 10),
	}
}

// FromValue attempts to convert the provided value stored in metadata into time.Time.
func FromValue(v string) (t time.Time, ok bool) {
	nanos, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return time.Time{}, false
	}

	return time.Unix(0, nanos), true
}
