//go:build linux

package tempfile

import (
	"testing"
)

// Explicitly test the create fallback function.
// This test only applies to Linux to explicitly test the fallback function.
// In other unix platforms the fallback is the default implementation, so it
// is already tested in the tests for the Create function.
func TestCreateFallback(t *testing.T) {
	VerifyTempfile(t, createUnixFallback)
}
