package maintenancestats

import (
	"log"

	"golang.org/x/time/rate"
)

var limit = rate.Sometimes{First: 10} //nolint:mnd

// ToUint64 converts v from a signed integer type T to uint64 while checking that
// the value is non-negative.
func ToUint64[T int32 | int | int64](v T) uint64 {
	if v < 0 {
		limit.Do(func() {
			log.Println("warning, converting negative value to uint64:", v)
		})
	}

	return uint64(v)
}
