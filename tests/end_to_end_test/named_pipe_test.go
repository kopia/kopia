//go:build linux
// +build linux

package endtoend_test

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/kopia/kopia/tests/clitestutil"
	"github.com/kopia/kopia/tests/testenv"
)

const (
	pipePath     = "pipe.txt"
	testfilePath = "testfile.txt"
)

func TestIgnoreNamedPipe(t *testing.T) {
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewExeRunner(t))
	e.Environment["KOPIA_SNAPSHOT_NAMED_PIPES"] = "false"

	// Create a temporary directory for the test
	tmpDir := e.RepoDir

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir, "--override-hostname=foo", "--override-username=foo")

	// Create a named pipe
	pipePath := filepath.Join(tmpDir, pipePath)

	err := createNamedPipe(pipePath)
	if err != nil {
		t.Fatalf("failed to create named pipe: %v", err)
	}

	// Create a test file next to the pipe
	testFilePath := filepath.Join(tmpDir, testfilePath)

	err = createTestFile(testFilePath)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	errChan := make(chan error)

	// Start streaming data to the pipe asynchronously
	go streamDataToPipe(pipePath, errChan)

	// Create a snapshot of the directory
	e.RunAndExpectSuccess(t, "snapshot", "create", tmpDir)

	select {
	case err := <-errChan:
		if err != nil {
			t.Fatalf("failed to stream to pipe: %v", err)
		}
	default:
		// no error occured
	}

	sources := clitestutil.ListSnapshotsAndExpectSuccess(t, e)
	snapshotID := sources[0].Snapshots[0].SnapshotID

	entries := e.RunAndExpectSuccess(t, "ls", "-r", snapshotID)

	for _, entry := range entries {
		if strings.Contains(entry, pipePath) {
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
	absolutePipePath := filepath.Join(tmpDir, pipePath)

	err := createNamedPipe(absolutePipePath)
	if err != nil {
		t.Fatalf("failed to create named pipe: %v", err)
	}

	// Create a test file next to the pipe
	testFilePath := filepath.Join(tmpDir, testfilePath)

	err = createTestFile(testFilePath)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	errChan := make(chan error)

	// Start streaming data to the pipe asynchronously
	go streamDataToPipe(absolutePipePath, errChan)

	// Create a snapshot of the directory
	e.RunAndExpectSuccess(t, "snapshot", "create", tmpDir)

	err = <-errChan
	if err != nil {
		t.Fatalf("failed to stream to pipe: %v", err)
	}

	sources := clitestutil.ListSnapshotsAndExpectSuccess(t, e)
	snapshotID := sources[0].Snapshots[0].ObjectID

	content := e.RunAndExpectSuccess(t, "cat", snapshotID+"/"+pipePath)
	if strings.Join(content, "\n") != pipeContent {

		t.Fatalf("pipe was not read correctly")
	}
}

func createNamedPipe(pipePath string) error {
	err := syscall.Mkfifo(pipePath, 0777)
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

const pipeContent = "Data\nFrom\nPipe"

func streamDataToPipe(pipePath string, errChan chan error) {
	// Open the named pipe for writing
	pipe, err := os.OpenFile(pipePath, os.O_WRONLY, os.ModeNamedPipe)
	if err != nil {
		errChan <- fmt.Errorf("failed to open named pipe for writing: %w", err)
		return
	}
	defer pipe.Close()

	_, err = pipe.WriteString(pipeContent)
	if err != nil {
		if errors.Is(err, io.ErrClosedPipe) {
			return
		} else {
			errChan <- fmt.Errorf("failed to write to pipe: %w", err)
		}
	}

	close(errChan)
}
