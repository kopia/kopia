// Package testutil contains test utilities.
package testutil

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

// ProviderTest marks the test method so that it only runs in provider-tests suite.
func ProviderTest(t *testing.T) {
	t.Helper()

	if os.Getenv("KOPIA_PROVIDER_TEST") == "" {
		t.Skip("skipping because KOPIA_PROVIDER_TEST is not set")
	}
}

// TestSkipUnlessCI skips the current test with a provided message, except when running
// in CI environment, in which case it causes hard failure.
func TestSkipUnlessCI(tb testing.TB, msg string, args ...interface{}) {
	tb.Helper()

	if len(args) > 0 {
		msg = fmt.Sprintf(msg, args...)
	}

	if os.Getenv("CI") != "" {
		tb.Fatal(msg)
	} else {
		tb.Skip(msg)
	}
}

// TestSkipOnCIUnlessLinuxAMD64 skips the current test if running on CI unless the environment is Linux/AMD64.
func TestSkipOnCIUnlessLinuxAMD64(tb testing.TB) {
	tb.Helper()

	if os.Getenv("CI") != "" && runtime.GOOS+"/"+runtime.GOARCH != "linux/amd64" {
		tb.Skip("test not supported in this environment.")
	}
}

// ShouldReduceTestComplexity returns true if test complexity should be reduced on the current machine.
func ShouldReduceTestComplexity() bool {
	if isRaceDetector {
		return true
	}

	return strings.Contains(runtime.GOARCH, "arm")
}

// ShouldSkipUnicodeFilenames returns true if:
// an environmental variable is unset, set to false, test is running on ARM, or if running race detection.
func ShouldSkipUnicodeFilenames() bool {
	val, enable := os.LookupEnv("ENABLE_UNICODE_FILENAMES")

	if !enable || isRaceDetector || strings.EqualFold(val, "false") {
		return true
	}

	return strings.Contains(runtime.GOARCH, "arm")
}

// ShouldSkipLongFilenames returns true if:
// an environmental variable is unset, set to false, test is running on ARM, or if running race detection.
func ShouldSkipLongFilenames() bool {
	val, enable := os.LookupEnv("ENABLE_LONG_FILENAMES")

	if !enable || isRaceDetector || strings.EqualFold(val, "false") {
		return true
	}

	return strings.Contains(runtime.GOARCH, "arm")
}

// MyTestMain runs tests and verifies some post-run invariants.
func MyTestMain(m *testing.M) {
	v := m.Run()

	os.Exit(v)
}

// MustParseJSONLines parses the lines containing JSON into the provided object.
func MustParseJSONLines(t *testing.T, lines []string, v interface{}) {
	t.Helper()

	allJSON := strings.Join(lines, "\n")
	dec := json.NewDecoder(strings.NewReader(allJSON))
	dec.DisallowUnknownFields()

	if err := dec.Decode(v); err != nil {
		t.Fatalf("failed to parse JSON %v: %v", allJSON, err)
	}
}

// RunAllTestsWithParam uses reflection to run all test methods starting with 'Test' on the provided object.
// nolint:thelper
func RunAllTestsWithParam(t *testing.T, v interface{}) {
	m := reflect.ValueOf(v)
	typ := m.Type()

	for i := 0; i < typ.NumMethod(); i++ {
		i := i
		meth := typ.Method(i)

		if strings.HasPrefix(meth.Name, "Test") {
			t.Run(meth.Name, func(t *testing.T) {
				m.Method(i).Call([]reflect.Value{reflect.ValueOf(t)})
			})
		}
	}
}
