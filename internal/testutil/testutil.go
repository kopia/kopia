package testutil

import (
	"fmt"
	"os"
	"testing"
)

// TestSkipUnlessCI skips the current test with a provided message, except when running
// in CI environment, in which case it causes hard failure.
func TestSkipUnlessCI(t *testing.T, msg string, args ...interface{}) {
	if len(args) > 0 {
		msg = fmt.Sprintf(msg, args...)
	}

	if os.Getenv("CI") != "" {
		t.Fatal(msg)
	} else {
		t.Skip()
	}
}
