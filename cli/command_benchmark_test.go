package cli_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/tests/testenv"
)

func TestCommandBenchmarkCrypto(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	e.RunAndExpectSuccess(t, "benchmark", "crypto", "--repeat=1", "--block-size=1KB", "--print-options")
}

func TestCommandBenchmarkEncryption(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	e.RunAndExpectSuccess(t, "benchmark", "encryption", "--parallel=3", "--repeat=1", "--block-size=1KB", "--print-options")
}

func TestCommandBenchmarkHashing(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	e.RunAndExpectSuccess(t, "benchmark", "hashing", "--repeat=1", "--block-size=1KB", "--print-options")
}

func TestCommandBenchmarkSplitter(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	e.RunAndExpectSuccess(t, "benchmark", "splitter", "--block-count=1", "--print-options")
}

func TestCommandBenchmarkCompression(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	testFile := filepath.Join(testutil.TempDirectory(t), "testfile.txt")
	os.WriteFile(testFile, bytes.Repeat([]byte{1, 2, 3, 4, 5, 6}, 10000), 0o600)

	e.RunAndExpectSuccess(t, "benchmark", "compression", "--data-file", testFile, "--repeat=2", "--verify-stable", "--print-options")
	e.RunAndExpectSuccess(t, "benchmark", "compression", "--data-file", testFile, "--repeat=2", "--by-size")
}
