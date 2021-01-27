package testutil

import (
	"fmt"
	"os"
	"runtime"
	"testing"
)

// TestSkipUnlessCI skips the current test with a provided message, except when running
// in CI environment, in which case it causes hard failure.
func TestSkipUnlessCI(t *testing.T, msg string, args ...interface{}) {
	t.Helper()

	if len(args) > 0 {
		msg = fmt.Sprintf(msg, args...)
	}

	if os.Getenv("CI") != "" {
		t.Fatal(msg)
	} else {
		t.Skip(msg)
	}
}

// TestSkipOnCIUnlessLinuxAMD64 skips the current test if running on CI unless the environment is Linux/AMD64.
func TestSkipOnCIUnlessLinuxAMD64(t *testing.T) {
	t.Helper()

	if os.Getenv("CI") != "" && runtime.GOOS+"/"+runtime.GOARCH != "linux/amd64" {
		t.Skip("test not supported in this environment.")
	}
}
