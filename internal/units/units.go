// Package units contains helpers to convert sizes to humand-readable strings.
package units

import (
	"fmt"
	"strings"
)

var (
	base10UnitPrefixes = []string{"", "K", "M", "G", "T"}
	base2UnitPrefixes  = []string{"", "Ki", "Mi", "Gi", "Ti"}
)

func niceNumber(f float64) string {
	return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.1f", f), "0"), ".")
}

func toDecimalUnitString(f, thousand float64, prefixes []string, suffix string) string {
	for i := range prefixes {
		if f < 0.9*thousand {
			return fmt.Sprintf("%v %v%v", niceNumber(f), prefixes[i], suffix)
		}

		f /= thousand
	}

	return fmt.Sprintf("%v %v%v", niceNumber(f), prefixes[len(prefixes)-1], suffix)
}

// BytesStringBase10 formats the given value as bytes with the appropriate base-10 suffix (KB, MB, GB, ...)
func BytesStringBase10(b int64) string {
	return toDecimalUnitString(float64(b), 1000, base10UnitPrefixes, "B")
}

// BytesStringBase2 formats the given value as bytes with the appropriate base-2 suffix (KiB, MiB, GiB, ...)
func BytesStringBase2(b int64) string {
	return toDecimalUnitString(float64(b), 1024.0, base2UnitPrefixes, "B")
}

// BitsPerSecondsString formats the given value bits per second with the appropriate suffix (Kbit/s, Mbit/s, Gbit/s, ...)
func BitsPerSecondsString(bps float64) string {
	return toDecimalUnitString(bps, 1000, base10UnitPrefixes, "bit/s")
}

// Count returns the given number with the appropriate base-10 suffix (K, M, G, ...)
func Count(v int64) string {
	return toDecimalUnitString(float64(v), 1000, base10UnitPrefixes, "")
}
