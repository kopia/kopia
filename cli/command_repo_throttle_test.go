package cli_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/blob/throttling"
	"github.com/kopia/kopia/tests/testenv"
)

func TestRepoThrottle(t *testing.T) {
	t.Parallel()

	env := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewInProcRunner(t))

	env.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", env.RepoDir)
	defer env.RunAndExpectSuccess(t, "repo", "disconnect")

	require.Equal(t, []string{
		"Max Download Speed:            (unlimited)",
		"Max Upload Speed:              (unlimited)",
		"Max Read Requests Per Second:  (unlimited)",
		"Max Write Requests Per Second: (unlimited)",
		"Max List Requests Per Second:  (unlimited)",
		"Max Concurrent Reads:          (unlimited)",
		"Max Concurrent Writes:         (unlimited)",
	}, env.RunAndExpectSuccess(t, "repo", "throttle", "get"))

	env.RunAndExpectSuccess(t, "repo", "throttle", "set",
		"--download-bytes-per-second=1000000000",
		"--upload-bytes-per-second=2000000000",
		"--read-requests-per-second=300",
		"--write-requests-per-second=400",
		"--list-requests-per-second=500",
		"--concurrent-reads=300",
		"--concurrent-writes=400",
	)

	env.RunAndExpectFailure(t, "repo", "throttle", "set", "--download-bytes-per-second=-30")
	env.RunAndExpectFailure(t, "repo", "throttle", "set", "--concurrent-reads=-3")

	require.Equal(t, []string{
		"Max Download Speed:            1 GB/s",
		"Max Upload Speed:              2 GB/s",
		"Max Read Requests Per Second:  300",
		"Max Write Requests Per Second: 400",
		"Max List Requests Per Second:  500",
		"Max Concurrent Reads:          300",
		"Max Concurrent Writes:         400",
	}, env.RunAndExpectSuccess(t, "repo", "throttle", "get"))

	env.RunAndExpectSuccess(t, "repo", "throttle", "set",
		"--upload-bytes-per-second=unlimited",
		"--write-requests-per-second=unlimited",
	)

	require.Equal(t, []string{
		"Max Download Speed:            1 GB/s",
		"Max Upload Speed:              (unlimited)",
		"Max Read Requests Per Second:  300",
		"Max Write Requests Per Second: (unlimited)",
		"Max List Requests Per Second:  500",
		"Max Concurrent Reads:          300",
		"Max Concurrent Writes:         400",
	}, env.RunAndExpectSuccess(t, "repo", "throttle", "get"))

	var limits throttling.Limits

	testutil.MustParseJSONLines(t, env.RunAndExpectSuccess(t, "repo", "throttle", "get", "--json"), &limits)
	require.Equal(t, throttling.Limits{
		ReadsPerSecond:         300,
		WritesPerSecond:        0,
		ListsPerSecond:         500,
		UploadBytesPerSecond:   0,
		DownloadBytesPerSecond: 1e+09,
		ConcurrentReads:        300,
		ConcurrentWrites:       400,
	}, limits)
}
