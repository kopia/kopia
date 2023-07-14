//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

package consistency

import (
	"bufio"
	"bytes"
	"errors"
	"log"
	"os"
	"os/exec"
	"path"
	"strings"
	"sync"
	"testing"

	"github.com/kopia/kopia/tests/tools/kopiarunner"
	"github.com/stretchr/testify/require"
)

func TestConsistencyWhenKill9AfterModify(t *testing.T) {
	// assumption: the test is run on filesystem & not directly on object store
	dataRepoPath := path.Join(*repoPathPrefix, dirPath, dataPath)

	baseDir := t.TempDir()
	if baseDir == "" {
		t.FailNow()
	}

	bm, err := NewSnapshotTester(baseDir, dataRepoPath)
	if err != nil {
		if errors.Is(err, kopiarunner.ErrExeVariableNotSet) {
			t.Skip("Skipping crash consistency tests because KOPIA_EXE is not set")
		} else {
			t.Skip("Error creating SnapshotTester:", err)
		}
	}

	bm.DataRepoPath = dataRepoPath

	// create a snapshot for initialized data
	snapID, err := bm.SetUpSystemUnderTest()
	if err != nil {
		t.FailNow()
	}
	cmpDir := bm.PathToTakeSnapshot

	copyDir := t.TempDir()
	if copyDir == "" {
		t.FailNow()
	}

	err = bm.FileHandler.CopyAllFiles(cmpDir, copyDir)
	require.NoError(t, err)

	// add files
	fileSize := 1 * 1024 * 1024
	numFiles := 30

	err = bm.GenerateRandomFiles(fileSize, numFiles)
	require.NoError(t, err)

	err = bm.FileHandler.CopyAllFiles(bm.PathToTakeSnapshot, copyDir)
	require.NoError(t, err)

	// modify original files
	content := "\nthis is a test for TestConsistencyWhenKill9AfterModify\n"
	err = bm.FileHandler.ModifyDataSetWithContent(copyDir, content)
	require.NoError(t, err)

	// kill the kopia command before it exits
	kopiaExe := os.Getenv("KOPIA_EXE")
	cmd := exec.Command(kopiaExe, "snap", "create", copyDir, "--json")
	killOnCondition(t, cmd)

	// verify snapshot corruption
	err = bm.VerifySnapshot()
	require.NoError(t, err)

	// delete random blob
	// assumption: the repo contains "p" blobs to delete, else the test will fail
	err = bm.DeleteBlob("")
	require.NoError(t, err, "Error deleting kopia blob")

	// Create a temporary dir to restore a snapshot
	restoreDir := t.TempDir()
	if restoreDir == "" {
		t.FailNow()
	}

	// try to restore a snapshot, this should error out
	stdout, err := bm.RestoreGivenOrRandomSnapshot(snapID, restoreDir)
	require.NoError(t, err)

	log.Println(stdout)

	err = bm.FileHandler.CompareDirs(restoreDir, cmpDir)
	require.NoError(t, err)
}

func killOnCondition(t *testing.T, cmd *exec.Cmd) {
	stderrPipe, err := cmd.StderrPipe()
	require.NoError(t, err)

	// excute kill -9 while recieve ` | 1 hashing, 0 hashed (65.5 KB), 0 cached (0 B), uploaded 0 B, estimating...` message
	var wg sync.WaitGroup

	// Add a WaitGroup counter for the first goroutine
	wg.Add(1)

	errOut := bytes.Buffer{}

	go func() {
		defer wg.Done()

		// Create a scanner to read from stderrPipe
		scanner := bufio.NewScanner(stderrPipe)
		for scanner.Scan() {
			output := scanner.Text()
			log.Println(output)
			errOut.Write(scanner.Bytes())
			errOut.WriteByte('\n')

			// Check if the output contains the "hashing" etc.
			if strings.Contains(output, "hashing") && strings.Contains(output, "hashed") && strings.Contains(output, "uploaded") {
				log.Println("Detaching and terminating target process")
				cmd.Process.Kill()
				break
			}
		}
	}()

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		panic(err)
	}

	wg.Add(1)

	o := bytes.Buffer{}

	go func() {
		defer wg.Done()

		// Create a scanner to read from stdoutPipe
		scanner := bufio.NewScanner(stdoutPipe)
		for scanner.Scan() {
			output := scanner.Text()
			log.Println(output)
			o.Write(scanner.Bytes())
			o.WriteByte('\n')
			// Check if the output contains the "copying" text
			if strings.Contains(output, "hashing") && strings.Contains(output, "hashed") && strings.Contains(output, "uploaded") {
				cmd.Process.Kill()
				break
			}
		}
	}()

	// Run the command
	err = cmd.Run()

	// Wait for the goroutines to finish
	wg.Wait()
}
