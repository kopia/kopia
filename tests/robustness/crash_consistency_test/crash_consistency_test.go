//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

package consistency

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"log"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/kopia/kopia/tests/recovery/blobmanipulator"
	"github.com/kopia/kopia/tests/tools/kopiarunner"
	"github.com/stretchr/testify/require"
)

func TestConsistencyWhenKill9AfterModify(t *testing.T) {
	// create, connect repository
	dataRepoPath := path.Join(*repoPathPrefix, dirPath, dataPath)
	baseDir := makeBaseDir(t)
	bm, err := blobmanipulator.NewBlobManipulator(baseDir, dataRepoPath)
	if errors.Is(err, kopiarunner.ErrExeVariableNotSet) {
		t.Skip("Skipping recovery tests because KOPIA_EXE is not set")
	}
	require.NoError(t, err)

	bm.DataRepoPath = dataRepoPath

	// generate 10M * 10 dataset
	fileSize := 1 * 1024 * 1024
	numFiles := 100
	err = bm.GenerateRandomFiles(fileSize, numFiles)
	require.NoError(t, err)

	// Create the first snapshot and expect it to run successfully.
	// This will populate the repository under test with initial data snapshot.
	snapID, _, err := bm.TakeSnapshot(bm.PathToTakeSnapshot)
	require.NoError(t, err)

	dst := getRootDir(t, bm.PathToTakeSnapshot)

	preRestorePath := filepath.Join(dirPath, "restore_pre")
	// try to restore a snapshot named restore_pre
	_, err = bm.RestoreGivenOrRandomSnapshot("", preRestorePath)
	require.NoError(t, err)

	// generate a 10M dataset
	err = bm.GenerateRandomFiles(fileSize, 1)
	require.NoError(t, err)

	src := getRootDir(t, bm.PathToTakeSnapshot)

	// modify the data
	modifyDataSet(t, src, dst)

	log.Println("----kopia creation process ----: ")

	err = bm.KopiaCommandRunner.ConnectRepo("filesystem", "--path="+bm.DataRepoPath)

	// kopia snapshot create for new data
	kopiaExe := os.Getenv("KOPIA_EXE")
	cmd := exec.Command(kopiaExe, "snap", "create", dst, "--json", "--parallel", "1")

	// excute kill -9 while recieve ` | 1 hashing, 0 hashed (65.5 KB), 0 cached (0 B), uploaded 0 B, estimating...` message
	killOnCondition(t, cmd)

	// snapshot verification
	// kopia snapshot verify --verify-files-percent=100
	cmd = exec.Command(kopiaExe, "snapshot", "verify", "--verify-files-percent=100")
	err = cmd.Run()
	require.NoError(t, err)

	newResotrePath := filepath.Join(dirPath, "restore")
	// try to restore a snapshot named restore
	_, err = bm.RestoreGivenOrRandomSnapshot(snapID, newResotrePath)
	require.NoError(t, err)

	compareDirs(t, preRestorePath, newResotrePath)
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

func compareDirs(t *testing.T, source, restoreDir string) {
	t.Helper()

	srcDirs, err := os.ReadDir(source)
	require.NoError(t, err)

	dstDirs, err := os.ReadDir(restoreDir)
	require.NoError(t, err)

	require.Equal(t, len(dstDirs), len(srcDirs))

	checkSet := make(map[string]bool)

	for _, dstDir := range dstDirs {
		checkSet[dstDir.Name()] = true
	}

	for _, srcDir := range srcDirs {
		_, ok := checkSet[srcDir.Name()]
		require.True(t, ok)

		srcFilePath := filepath.Join(source, srcDir.Name())
		dstFilePath := filepath.Join(restoreDir, srcDir.Name())

		cmd := exec.Command("cmp", "-s", srcFilePath, dstFilePath)
		err := cmd.Run()
		if err != nil {
			t.Errorf("Files '%s' and '%s' are different.", srcFilePath, dstFilePath)
		}
	}
}

func makeBaseDir(t *testing.T) string {
	baseDir, err := os.MkdirTemp("", dataPath)
	require.NoError(t, err)

	return baseDir
}

func getRootDir(t *testing.T, source string) string {
	path := source

	for {
		dirEntries, err := os.ReadDir(path)
		require.NoError(t, err)

		if len(dirEntries) == 0 || !dirEntries[0].IsDir() {
			break
		}

		path = filepath.Join(path, dirEntries[0].Name())
	}
	return path
}

func modifyDataSet(t *testing.T, source string, destination string) {
	srcFile, err := os.Open(filepath.Join(source, "file_0"))
	require.NoError(t, err)
	defer srcFile.Close()

	dstDirs, err := os.ReadDir(destination)
	require.NoError(t, err)

	for _, dstFile := range dstDirs {
		dstFilePath := filepath.Join(destination, dstFile.Name())

		dstFile, err := os.OpenFile(dstFilePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
		require.NoError(t, err)

		_, err = io.Copy(dstFile, srcFile)
		require.NoError(t, err)

		dstFile.Close()
	}
}
