package cli_test

import (
	"bytes"
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/tests/testenv"
)

func TestCommandBenchmarkCrypto(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, runner)

	e.RunAndExpectSuccess(t, "benchmark", "crypto", "--repeat=1", "--block-size=1KB", "--print-options")
}

func TestCommandBenchmarkSpliter(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, runner)

	e.RunAndExpectSuccess(t, "benchmark", "splitter", "--block-count=1", "--print-options")
}

func TestCommandBenchmarkCompression(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, runner)

	testFile := filepath.Join(testutil.TempDirectory(t), "testfile.txt")
	ioutil.WriteFile(testFile, bytes.Repeat([]byte{1, 2, 3, 4, 5, 6}, 10000), 0600)

	e.RunAndExpectSuccess(t, "benchmark", "compression", "--data-file", testFile, "--repeat=2", "--verify-stable", "--print-options")
	e.RunAndExpectSuccess(t, "benchmark", "compression", "--data-file", testFile, "--repeat=2", "--by-size")
}
