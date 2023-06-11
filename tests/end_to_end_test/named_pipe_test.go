package endtoend_test

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kopia/kopia/tests/clitestutil"
	"github.com/kopia/kopia/tests/testenv"
)

const PIPE_PATH = "pipe.txt"
const TESTFILE_PATH = "testfile.txt"

func TestIgnoreNamedPipe(t *testing.T) {
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewExeRunner(t))
	e.Environment["KOPIA_SNAPSHOT_NAMED_PIPES"] = "false"

	// Create a temporary directory for the test
	tmpDir := e.RepoDir

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir, "--override-hostname=foo", "--override-username=foo")

	// Create a named pipe
	pipePath := filepath.Join(tmpDir, PIPE_PATH)
	err := createNamedPipe(pipePath)
	if err != nil {
		t.Fatalf("failed to create named pipe: %v", err)
	}

	// Create a test file next to the pipe
	testFilePath := filepath.Join(tmpDir, TESTFILE_PATH)
	err = createTestFile(testFilePath)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Start streaming data to the pipe asynchronously
	go streamDataToPipe(pipePath)

	// Create a snapshot of the directory
	e.RunAndExpectSuccess(t, "snapshot", "create", tmpDir)

	sources := clitestutil.ListSnapshotsAndExpectSuccess(t, e)
	snapshotId := sources[0].Snapshots[0].SnapshotID

	entries := e.RunAndExpectSuccess(t, "ls", "-r", snapshotId)

	for _, entry := range entries {
		if strings.Contains(entry, PIPE_PATH) {
			t.Fatalf("pipe was snapshoted when it shouldn't have been")
		}
	}
}

func TestBackupNamedPipe(t *testing.T) {
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewExeRunner(t))
	e.Environment["KOPIA_SNAPSHOT_NAMED_PIPES"] = "true"

	// Create a temporary directory for the test
	tmpDir := e.RepoDir

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir, "--override-hostname=foo", "--override-username=foo")

	// Create a named pipe
	pipePath := filepath.Join(tmpDir, PIPE_PATH)
	err := createNamedPipe(pipePath)
	if err != nil {
		t.Fatalf("failed to create named pipe: %v", err)
	}

	// Create a test file next to the pipe
	testFilePath := filepath.Join(tmpDir, TESTFILE_PATH)
	err = createTestFile(testFilePath)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Start streaming data to the pipe asynchronously
	go streamDataToPipe(pipePath)

	// Create a snapshot of the directory
	e.RunAndExpectSuccess(t, "snapshot", "create", tmpDir)

	sources := clitestutil.ListSnapshotsAndExpectSuccess(t, e)
	snapshotId := sources[0].Snapshots[0].ObjectID

	content := e.RunAndExpectSuccess(t, "cat", snapshotId+"/"+PIPE_PATH)
	if strings.Join(content, "\n") != PIPE_CONTENT {
		t.Fatalf("pipe was not read correctly")
	}
}

func createNamedPipe(pipePath string) error {
	err := exec.Command("mkfifo", pipePath).Run()
	if err != nil {
		return fmt.Errorf("failed to create named pipe: %w", err)
	}

	return nil
}

func createTestFile(filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create test file: %w", err)
	}
	defer file.Close()

	// Write some content to the file
	_, err = file.WriteString("Test file content")
	if err != nil {
		return fmt.Errorf("failed to write to test file: %w", err)
	}

	return nil
}

const PIPE_CONTENT = "Data\nFrom\nPipe"

func streamDataToPipe(pipePath string) {
	// Open the named pipe for writing
	pipe, err := os.OpenFile(pipePath, os.O_WRONLY, os.ModeNamedPipe)
	if err != nil {
		fmt.Printf("failed to open named pipe for writing: %v", err)
		return
	}
	defer pipe.Close()

	_, err = pipe.WriteString(PIPE_CONTENT)
	if err != nil {
		if err == io.ErrClosedPipe {
			fmt.Println("Pipe closed")
			return
		}
		fmt.Printf("failed to write to pipe: %v", err)
		return
	}

}
