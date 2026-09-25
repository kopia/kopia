// Package envflag contains helpers for getting values from environment variables.
package envflag

import (
	"os"
	"strconv"
	"strings"
)

// Bool returns true only if varName is set to a "truthy" value, that is, "true"
// (case independent) or 1, and returns false otherwise, including when the
// variable is unset, empty, set to "false" or a value that cannot be parsed.
func Bool(varName string) bool {
	// equivalent to return BoolDefault(varName, false)
	b, err := strconv.ParseBool(strings.ToLower(os.Getenv(varName)))

	return b && (err == nil)
}

// BoolDefault return the boolean value for the content of the varName environment
// variable. It returns defaultValue when varName is unset, empty or unparseable.
func BoolDefault(varName string, defaultValue bool) bool {
	v := os.Getenv(varName)

	if v == "" {
		return defaultValue
	}

	if b, err := strconv.ParseBool(strings.ToLower(v)); err == nil {
		return b
	}

	// TODO: log error
	return defaultValue
}
