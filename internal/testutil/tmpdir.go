package testutil

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/clock"
)

const (
	maxOutputLinesToLog = 4000
	logsDirPermissions  = 0o750
)

//nolint:gochecknoglobals
var interestingLengths = []int{10, 50, 100, 240, 250, 260, 270}

// GetInterestingTempDirectoryName returns interesting directory name used for testing.
func GetInterestingTempDirectoryName() (string, error) {
	td, err := os.MkdirTemp("", "kopia-test-"+time.Now().UTC().Format("20060102-150405")) //nolint:forbidigo
	if err != nil {
		return "", errors.Wrap(err, "unable to create temp directory")
	}

	//nolint:gosec
	targetLen := interestingLengths[rand.Intn(len(interestingLengths))]

	// make sure the base directory is quite long to trigger very long filenames on Windows.
	if n := len(td); n < targetLen {
		if !ShouldSkipLongFilenames() {
			td = filepath.Join(td, strings.Repeat("f", targetLen-n))
		}

		//nolint:mnd
		if err := os.MkdirAll(td, 0o700); err != nil {
			return "", errors.Wrap(err, "unable to create temp directory")
		}
	}

	return td, nil
}

// TempDirectory returns an interesting temporary directory and cleans it up before test
// completes.
func TempDirectory(tb testing.TB) string {
	tb.Helper()

	d, err := GetInterestingTempDirectoryName()
	if err != nil {
		tb.Fatal(err)
	}

	tb.Cleanup(func() {
		if !tb.Failed() {
			os.RemoveAll(d) //nolint:errcheck
		} else {
			tb.Logf("temporary files left in %v", d)
		}
	})

	return d
}

// TempDirectoryShort returns a short temporary directory and cleans it up before test
// completes.
func TempDirectoryShort(tb testing.TB) string {
	tb.Helper()

	d, err := os.MkdirTemp("", "kopia-test-"+time.Now().UTC().Format("20060102-150405")) //nolint:forbidigo
	if err != nil {
		tb.Fatal(errors.Wrap(err, "unable to create temp directory"))
	}

	tb.Cleanup(func() {
		if !tb.Failed() {
			os.RemoveAll(d) //nolint:errcheck
		} else {
			tb.Logf("temporary files left in %v", d)
		}
	})

	return d
}

// TempLogDirectory returns a temporary directory used for storing logs.
// If KOPIA_LOGS_DIR is provided.
func TempLogDirectory(tb testing.TB) string {
	tb.Helper()

	cleanName := strings.NewReplacer("/", "_", "\\", "_", ":", "_").Replace(tb.Name())

	tb.Helper()

	logsBaseDir := os.Getenv("KOPIA_LOGS_DIR")
	if logsBaseDir == "" {
		logsBaseDir = filepath.Join(os.TempDir(), "kopia-logs")
	}

	logsDir := filepath.Join(logsBaseDir, cleanName+"."+clock.Now().Local().Format("20060102150405"))

	require.NoError(tb, os.MkdirAll(logsDir, logsDirPermissions))

	tb.Cleanup(func() {
		if os.Getenv("KOPIA_KEEP_LOGS") != "" {
			tb.Logf("logs preserved in %v", logsDir)
			return
		}

		if tb.Failed() && os.Getenv("KOPIA_DISABLE_LOG_DUMP_ON_FAILURE") == "" {
			dumpLogs(tb, logsDir)
		}

		os.RemoveAll(logsDir) //nolint:errcheck
	})

	return logsDir
}

func dumpLogs(tb testing.TB, dirname string) {
	tb.Helper()

	entries, err := os.ReadDir(dirname)
	if err != nil {
		tb.Errorf("unable to read %v: %v", dirname, err)

		return
	}

	for _, e := range entries {
		if e.IsDir() {
			dumpLogs(tb, filepath.Join(dirname, e.Name()))
			continue
		}

		dumpLogFile(tb, filepath.Join(dirname, e.Name()))
	}
}

func dumpLogFile(tb testing.TB, fname string) {
	tb.Helper()

	data, err := os.ReadFile(fname) //nolint:gosec
	if err != nil {
		tb.Error(err)
		return
	}

	tb.Logf("LOG FILE: %v %v", fname, trimOutput(string(data)))
}

func trimOutput(s string) string {
	lines := splitLines(s)
	if len(lines) <= maxOutputLinesToLog {
		return s
	}

	lines2 := append([]string(nil), lines[0:(maxOutputLinesToLog/2)]...) //nolint:mnd
	lines2 = append(lines2, fmt.Sprintf("/* %v lines removed */", len(lines)-maxOutputLinesToLog))
	lines2 = append(lines2, lines[len(lines)-(maxOutputLinesToLog/2):]...) //nolint:mnd

	return strings.Join(lines2, "\n")
}

func splitLines(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}

	var result []string
	for l := range strings.SplitSeq(s, "\n") {
		result = append(result, strings.TrimRight(l, "\r"))
	}

	return result
}
