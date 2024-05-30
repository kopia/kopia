// Package units contains helpers to convert sizes to human-readable strings.
package units

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

//nolint:gochecknoglobals
var (
	base10UnitPrefixes = []string{"", "K", "M", "G", "T"}
	base2UnitPrefixes  = []string{"", "Ki", "Mi", "Gi", "Ti"}
)

const (
	bytesStringBase2Envar = "KOPIA_BYTES_STRING_BASE_2"
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
	//nolint:mnd
	return toDecimalUnitString(float64(b), 1000, base10UnitPrefixes, "B")
}

// BytesStringBase2 formats the given value as bytes with the appropriate base-2 suffix (KiB, MiB, GiB, ...)
func BytesStringBase2(b int64) string {
	//nolint:mnd
	return toDecimalUnitString(float64(b), 1024.0, base2UnitPrefixes, "B")
}

// BytesString formats the given value as bytes with the unit provided from the environment.
func BytesString(b int64) string {
	if v, _ := strconv.ParseBool(os.Getenv(bytesStringBase2Envar)); v {
		return BytesStringBase2(b)
	}

	return BytesStringBase10(b)
}

// BytesPerSecondsString formats the given value bytes per second with the appropriate base-10 suffix (KB/s, MB/s, GB/s, ...)
func BytesPerSecondsString(bps float64) string {
	//nolint:mnd
	return toDecimalUnitString(bps, 1000, base10UnitPrefixes, "B/s")
}

// Count returns the given number with the appropriate base-10 suffix (K, M, G, ...)
func Count(v int64) string {
	//nolint:mnd
	return toDecimalUnitString(float64(v), 1000, base10UnitPrefixes, "")
}
