package maintenancestats

import (
	"log"

	"golang.org/x/time/rate"
)

var limit = rate.Sometimes{First: 10} //nolint:mnd

// ToUint64 converts v from a signed integer type T to uint64 while checking that
// the value is non-negative. It returns 0 for negative values.
func ToUint64[T int8 | int16 | int32 | int | int64](v T) uint64 {
	if v >= 0 {
		return uint64(v)
	}

	limit.Do(func() {
		log.Println("warning, converting negative value to uint64:", v)
	})

	return 0
}
