//go:build darwin || (linux && amd64)

package robustness

import "strconv"

// GetOptAsIntOrDefault extracts an integer value from a configuration map
// if present, or else returns a default.
func GetOptAsIntOrDefault(key string, opts map[string]string, def int) int {
	if opts == nil {
		return def
	}

	if opts[key] == "" {
		return def
	}

	retInt, err := strconv.Atoi(opts[key])
	if err != nil {
		return def
	}

	return retInt
}
