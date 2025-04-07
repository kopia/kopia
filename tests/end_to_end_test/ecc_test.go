package endtoend_test

import (
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/tests/clitestutil"
	"github.com/kopia/kopia/tests/testenv"
)

func (s *formatSpecificTestSuite) TestNoECC(t *testing.T) {
	t.Parallel()

	const mb = 1024 * 1024

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, s.formatFlags, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--flat", "--path", e.RepoDir)

	dataDir := testutil.TempDirectory(t)

	var data [mb]byte
	for i := range data {
		data[i] = byte(i%254) + 1
	}

	require.NoError(t, os.WriteFile(filepath.Join(dataDir, "some-file1"), data[:], 0o600))

	e.RunAndExpectSuccess(t, "snapshot", "create", dataDir)

	repoSize, err := dirSize(e.RepoDir)
	require.NoError(t, err)

	require.Less(t, repoSize, int64(math.Round(1.1*mb)))
}

func (s *formatSpecificTestSuite) TestECC(t *testing.T) {
	t.Parallel()

	const mb = 1024 * 1024

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, s.formatFlags, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--flat", "--path", e.RepoDir, "--ecc-overhead-percent=50")

	dataDir := testutil.TempDirectory(t)

	var data [mb]byte
	for i := range data {
		data[i] = byte(i%254) + 1
	}

	fileName := "some-file1"

	require.NoError(t, os.WriteFile(filepath.Join(dataDir, fileName), data[:], 0o600))

	e.RunAndExpectSuccess(t, "snapshot", "create", dataDir)

	repoSize, err := dirSize(e.RepoDir)
	require.NoError(t, err)

	// ECC is not supported in version 1
	if s.formatVersion == 1 {
		require.Less(t, repoSize, int64(math.Round(1.1*mb)))
		return
	}

	require.GreaterOrEqual(t, repoSize, int64(math.Round(1.5*mb)))

	err = s.flipOneByteFromEachFile(e)
	require.NoError(t, err)

	si := clitestutil.ListSnapshotsAndExpectSuccess(t, e, dataDir)

	snapID := si[0].Snapshots[0].SnapshotID

	restoreDir := testutil.TempDirectory(t)
	e.RunAndExpectSuccess(t, "restore", snapID, restoreDir)

	restoreData, err := os.ReadFile(filepath.Join(restoreDir, fileName))
	require.NoError(t, err)

	require.Equal(t, data[:], restoreData)
}

func (s *formatSpecificTestSuite) flipOneByteFromEachFile(e *testenv.CLITest) error {
	return filepath.Walk(e.RepoDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if info.Name() == "kopia.repository.f" || info.Name() == ".shards" {
			return nil
		}

		bytes, err := os.ReadFile(path)
		if err != nil {
			return err
		}

		c := rand.Intn(len(bytes))
		if bytes[c] >= 128 {
			bytes[c] = 0
		} else {
			bytes[c] = 255
		}

		err = os.WriteFile(path, bytes, info.Mode())
		if err != nil {
			return err
		}

		return nil
	})
}

func dirSize(path string) (int64, error) {
	var size int64

	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})

	return size, err
}
