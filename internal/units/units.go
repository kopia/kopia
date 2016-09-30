package units

import (
	"fmt"
	"strings"
)

var unitPrefixes = []string{"", "K", "M", "G", "T"}

func niceNumber(f float64) string {
	return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.1f", f), "0"), ".")
}

func toDecimalUnitString(f float64, suffix string) string {
	for i := range unitPrefixes {
		if f < 900 {
			return fmt.Sprintf("%v %v%v", niceNumber(f), unitPrefixes[i], suffix)
		}
		f = f / float64(1000)
	}

	return fmt.Sprintf("%v %v%v", niceNumber(f), unitPrefixes[len(unitPrefixes)-1], suffix)
}

// BytesString formats the given value as bytes with the appropriate suffix (KB, MB, GB, ...)
func BytesString(b int64) string {
	return toDecimalUnitString(float64(b), "B")
}

// BitsPerSecondsString formats the given value bits per second with the appropriate suffix (Kbit/s, Mbit/s, Gbit/s, ...)
func BitsPerSecondsString(bps float64) string {
	return toDecimalUnitString(bps, "bit/s")
}
