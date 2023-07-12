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

func TestConsistencyWhenKill9WithoutModify(t *testing.T) {
	// create, connect repository
	dataRepoPath := path.Join(*repoPathPrefix, dirPath+dataPath)
	baseDir := makeBaseDir()
	bm, err := blobmanipulator.NewBlobManipulator(baseDir, dataRepoPath)
	if err != nil {
		if errors.Is(err, kopiarunner.ErrExeVariableNotSet) {
			log.Println("Skipping recovery tests because KOPIA_EXE is not set")
		} else {
			log.Println("Error creating Blob Manipulator:", err)
		}
		os.Exit(0)
	}

	bm.DataRepoPath = dataRepoPath

	// generate 10M * 10 dataset
	fileSize := 1 * 1024 * 1024
	numFiles := 100
	err = bm.GenerateRandomFiles(fileSize, numFiles)
	if err != nil {
		log.Println("Error in creating snapshot data", err)
		t.FailNow()
	}

	// create snapshot successfully at the first time
	// populate the kopia repo under test with random snapshots
	snapID, _, err := bm.TakeSnapshot(bm.PathToTakeSnapshot)
	if err != nil {
		log.Println("Error in creating snapshot", err)
		t.FailNow()
	}
	dst := getRootDir(bm.PathToTakeSnapshot)

	preResotrePath := filepath.Join(dirPath, "restore_pre")
	// try to restore a snapshot named restore_pre
	stdout, err := bm.RestoreGivenOrRandomSnapshot("", preResotrePath)
	if err != nil {
		log.Println("Error restoring the kopia repository:", stdout, err)
		t.FailNow()
	}

	// generate a 10M dataset
	err = bm.GenerateRandomFiles(fileSize, 1)
	if err != nil {
		log.Println("Error in creating snapshot data", err)
		t.FailNow()
	}

	src := getRootDir(bm.PathToTakeSnapshot)

	// modify the data
	modifyDataSet(src, dst)

	log.Println("----kopia creation process ----: ")

	// kopia snapshot create for new data
	kopiaExe := os.Getenv("KOPIA_EXE")
	cmd := exec.Command(kopiaExe, "snap", "create", dst, "--json", "--parallel", "1")

	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		panic(err)
	}

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

	// snapshot verification
	// kopia snapshot verify --verify-files-percent=100
	cmd = exec.Command(kopiaExe, "snapshot", "verify", "--verify-files-percent=100")
	err = cmd.Run()
	if err != nil {
		t.Errorf("kopia snapshot verify --verify-files-percent=100 failed")
	}

	newResotrePath := filepath.Join(dirPath, "restore")
	// try to restore a snapshot named restore
	stdout, err = bm.RestoreGivenOrRandomSnapshot(snapID, newResotrePath)
	if err != nil {
		log.Println("Error restoring the kopia repository:", stdout, err)
		t.FailNow()
	}

	compareDirs(t, preResotrePath, newResotrePath)
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

func makeBaseDir() string {
	baseDir, err := os.MkdirTemp("", dataPath)
	if err != nil {
		log.Println("Error creating temp dir:", err)
		os.Exit(0)
	}

	return baseDir
}

func getRootDir(source string) string {
	path := source

	for {
		dirEntries, err := os.ReadDir(path)
		if err != nil {
			log.Fatalf("Error open source folder '%s': %s", path, err)
		}

		if len(dirEntries) == 0 || !dirEntries[0].IsDir() {
			break
		}

		path = filepath.Join(path, dirEntries[0].Name())
	}
	return path
}

func modifyDataSet(source, destination string) {
	srcFile, err := os.Open(filepath.Join(source, "file_0"))
	if err != nil {
		log.Fatalf("Error opening source file: %s", err)
	}
	defer srcFile.Close()

	dstDirs, err := os.ReadDir(destination)
	if err != nil {
		log.Fatalf("Error opening destination folder: %s", err)
	}

	for _, dstFile := range dstDirs {
		dstFilePath := filepath.Join(destination, dstFile.Name())

		dstFile, err := os.OpenFile(dstFilePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			log.Fatalf("Error opening destination file '%s': %s", dstFilePath, err)
		}

		_, err = io.Copy(dstFile, srcFile)
		if err != nil {
			dstFile.Close()
			log.Fatalf("Error copying content to '%s': %s", dstFilePath, err)
		}

		dstFile.Close()
	}
}
