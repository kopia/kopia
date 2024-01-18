//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

package pprofdump_test

import (
	"bufio"
	"errors"
	"os"
	"os/exec"
	"path"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/tests/pprofdump/repositorymanipulator"

	"github.com/kopia/kopia/tests/testenv"
	"github.com/kopia/kopia/tests/tools/kopiarunner"
)

func TestPprofDumpRepositoryConnect(t *testing.T) {
	kopiaExe := os.Getenv("KOPIA_EXE")

	// assumption: the test is run on filesystem & not directly on object store
	dataRepoPath := path.Join(*repoPathPrefix, dataSubPath)

	baseDir := t.TempDir()
	require.NotEmpty(t, baseDir)

	bm, err := repositorymanipulator.NewRepositoryManipulator(baseDir, dataRepoPath)
	require.NoError(t, err)

	bm.DataRepoPath = dataRepoPath

	err = bm.SetUpSystemUnderTest()
	require.NoError(t, err)

	cmd := exec.Command(kopiaExe, "maintenance", "run", "--full", "--force", "--safety", "none")

	err = cmd.Start()
	require.NoError(t, err)

	// kill the kopia command before it exits
	time.AfterFunc(10*time.Millisecond, func() {
		err = cmd.Process.Signal(syscall.SIGINT)
		if err != nil {
			t.Fatalf("fatal: %v", err)
		}
	})

	// Create a temporary dir to restore a snapshot
	restoreDir := t.TempDir()
	require.NotEmpty(t, restoreDir)

	// restore a random snapshot
	_, err = bm.RestoreGivenOrRandomSnapshot("", restoreDir)
	require.NoError(t, err)
}

// TestConsistencyWhenKill9AfterModify will test the data consistency while it encountered kill -9 signal.
func TestConsistencyWhenKill9AfterModify(t *testing.T) {
	// assumption: the test is run on filesystem & not directly on object store
	dataRepoPath := path.Join(*repoPathPrefix, dirPath, dataPath)

	baseDir := t.TempDir()
	require.NotEmpty(t, baseDir, "TempDir() did not generate a valid dir")

	bm, err := repositorymanipulator.NewRepositoryManipulator(baseDir, dataRepoPath)
	if err != nil {
		if errors.Is(err, kopiarunner.ErrExeVariableNotSet) {
			t.Skip("Skipping crash consistency tests because KOPIA_EXE is not set")
		}

		t.Skip("Error creating SnapshotTester:", err)
	}

	bm.DataRepoPath = dataRepoPath

	// create a snapshot for initialized data
	_, err = bm.SetUpSystemWithOneSnapshot()
	require.NoError(t, err)

	// add files
	fileSize := 1 * 1024 * 1024
	numFiles := 200

	err = bm.GenerateRandomFiles(fileSize, numFiles)
	require.NoError(t, err)

	newDir := bm.PathToTakeSnapshot

	// connect with repository with the environment configuration, otherwise it will display "ERROR open repository: repository is not connected.kopia connect repo".
	kopiaExe := os.Getenv("KOPIA_EXE")

	cmd := exec.Command(kopiaExe, "repo", "connect", "filesystem", "--path="+dataRepoPath, "--content-cache-size-mb", "500", "--metadata-cache-size-mb", "500", "--no-check-for-updates")
	env := []string{"KOPIA_PASSWORD=" + testenv.TestRepoPassword}
	cmd.Env = append(os.Environ(), env...)

	o, err := cmd.CombinedOutput()
	require.NoError(t, err)
	t.Logf(string(o))

	// create snapshot with StderrPipe
	cmd = exec.Command(kopiaExe, "snap", "create", newDir, "--json", "--parallel=1")

	// kill the kopia command before it exits
	t.Logf("Kill the kopia command before it exits:")
	killOnCondition(t, cmd)

	// Create a temporary dir to restore a snapshot
	restoreDir := t.TempDir()
	require.NotEmpty(t, restoreDir, "TempDir() did not generate a valid dir")

	// try to restore a snapshot without any error messages.
	stdout, err := bm.RestoreGivenOrRandomSnapshot("", restoreDir)
	require.NoError(t, err)

	t.Logf(stdout)
}

func killOnCondition(t *testing.T, cmd *exec.Cmd) {
	t.Helper()

	stderrPipe, err := cmd.StderrPipe()
	require.NoError(t, err)

	// Create a scanner to read from stderrPipe
	scanner := bufio.NewScanner(stderrPipe)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		output := scanner.Text()
		t.Logf(output)

		// Check if the output contains the "hashing" etc.
		if strings.Contains(output, "hashing") && strings.Contains(output, "hashed") && strings.Contains(output, "uploaded") {
			t.Logf("Detaching and terminating target process")
			cmd.Process.Signal(syscall.SIGINT)

			break
		}
	}

	// Start the command
	err = cmd.Start()
	require.NoError(t, err)

	// Wait for the command
	err = cmd.Wait()
	require.NoError(t, err)
}
