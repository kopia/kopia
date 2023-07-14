//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

// Package consistency provides the framework for snapshot fix testing.
package consistency

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"math/rand"
	"strconv"
	"strings"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/tests/robustness"
	"github.com/kopia/kopia/tests/robustness/filehandler"
	"github.com/kopia/kopia/tests/robustness/fiofilewriter"
	"github.com/kopia/kopia/tests/robustness/snapmeta"
	"github.com/kopia/kopia/tests/tools/fio"
	"github.com/kopia/kopia/tests/tools/kopiarunner"
)

var (
	errKopiaRepoNotFound  = errors.New("kopia repository does not exist")
	errCreatingFileWriter = errors.New("could not create file writer")
)

// SnapshotTester provides a way to run a kopia command.
type SnapshotTester struct {
	KopiaCommandRunner *kopiarunner.KopiaSnapshotter
	DirCreater         *snapmeta.KopiaSnapshotter
	fileWriter         *fiofilewriter.FileWriter
	FileHandler        *filehandler.FileHandler

	DataRepoPath       string
	PathToTakeSnapshot string
}

// NewSnapshotTester instantiates a new SnapshotTester and returns its pointer.
func NewSnapshotTester(baseDirPath, dataRepoPath string) (*SnapshotTester, error) {
	ks := getSnapshotter(baseDirPath, dataRepoPath)
	if ks == nil {
		return nil, nil
	}

	runner, err := kopiarunner.NewKopiaSnapshotter(baseDirPath)
	if err != nil {
		return nil, err
	}

	return &SnapshotTester{
		KopiaCommandRunner: runner,
		DirCreater:         ks,
	}, nil
}

func getSnapshotter(baseDirPath, dataRepoPath string) *snapmeta.KopiaSnapshotter {
	ks, err := snapmeta.NewSnapshotter(baseDirPath)
	if err != nil {
		if errors.Is(err, kopiarunner.ErrExeVariableNotSet) {
			log.Println("Skipping recovery tests because KOPIA_EXE is not set")
		} else {
			log.Println("Error creating kopia Snapshotter:", err)
		}

		return nil
	}

	log.Println("Created snapmeta.KopiaSnapshotter")

	if err = ks.ConnectOrCreateRepo(dataRepoPath); err != nil {
		log.Println("Error initializing kopia Snapshotter:", err)
		return nil
	}

	return ks
}

// ConnectOrCreateRepo connects to an existing repository if possible or creates a new one.
func (bm *SnapshotTester) ConnectOrCreateRepo(dataRepoPath string) error {
	if bm.KopiaCommandRunner == nil {
		return errKopiaRepoNotFound
	}

	return bm.DirCreater.ConnectOrCreateRepo(bm.DataRepoPath)
}

// DeleteBlob deletes the provided blob or a random blob, in kopia repo.
func (bm *SnapshotTester) DeleteBlob(blobID string) error {
	if blobID == "" {
		randomBlobID, err := bm.getBlobIDRand()
		blobID = randomBlobID

		if err != nil {
			return err
		}
	}

	log.Printf("Deleting BLOB %s", blobID)

	_, _, err := bm.KopiaCommandRunner.Run("blob", "delete", blobID, "--advanced-commands=enabled")
	if err != nil {
		return err
	}

	return nil
}

func (bm *SnapshotTester) getBlobIDRand() (string, error) {
	var b []blob.Metadata

	// assumption: the repo under test is in filesystem
	err := bm.ConnectOrCreateRepo(bm.DataRepoPath)
	if err != nil {
		return "", err
	}

	blobIDList, _, err := bm.KopiaCommandRunner.Run("blob", "list", "--json")
	if blobIDList == "" {
		return "", robustness.ErrNoOp
	}

	if err != nil {
		return "", err
	}

	err = json.Unmarshal([]byte(blobIDList), &b)
	if err != nil {
		return "", err
	}

	blobToBeDeleted := ""
	// Select the first pack blob in the list
	for _, s := range b {
		temp := string(s.BlobID)
		if strings.HasPrefix(temp, "p") {
			blobToBeDeleted = temp
			break
		}
	}

	return blobToBeDeleted, nil
}

func (bm *SnapshotTester) getFileWriter() bool {
	fw, err := fiofilewriter.New()
	if err != nil {
		if errors.Is(err, fio.ErrEnvNotSet) {
			log.Println("Skipping recovery tests because FIO environment is not set")
		} else {
			log.Println("Error creating fio FileWriter:", err)
		}

		return false
	}

	bm.fileWriter = fw

	return true
}

func (bm *SnapshotTester) writeRandomFiles(ctx context.Context, fileSize, numFiles int) error {
	// create random data
	gotFileWriter := bm.getFileWriter()
	if !gotFileWriter {
		return errCreatingFileWriter
	}

	fileWriteOpts := map[string]string{
		fiofilewriter.MaxDirDepthField:         strconv.Itoa(1),
		fiofilewriter.MaxFileSizeField:         strconv.Itoa(fileSize),
		fiofilewriter.MinFileSizeField:         strconv.Itoa(fileSize),
		fiofilewriter.MaxNumFilesPerWriteField: strconv.Itoa(numFiles),
		fiofilewriter.MinNumFilesPerWriteField: strconv.Itoa(numFiles),
	}

	_, err := bm.fileWriter.WriteRandomFiles(ctx, fileWriteOpts)
	if err != nil {
		return err
	}

	return nil
}

// RestoreGivenOrRandomSnapshot restores a given or a random snapshot from kopia repository into the provided target directory.
func (bm *SnapshotTester) RestoreGivenOrRandomSnapshot(snapID, restoreDir string) (string, error) {
	err := bm.ConnectOrCreateRepo(bm.DataRepoPath)
	if err != nil {
		return "", err
	}

	if snapID == "" {
		// list available snaphsots
		stdout, _, snapshotListErr := bm.KopiaCommandRunner.Run("snapshot", "list", "--json")
		if snapshotListErr != nil {
			return stdout, snapshotListErr
		}

		var s []snapshot.Manifest

		err = json.Unmarshal([]byte(stdout), &s)
		if err != nil {
			return "", err
		}

		// Select a random snapshot to restore
		snapID = string(s[rand.Intn(len(s))].ID)
	}

	_, msg, err := bm.KopiaCommandRunner.Run("snapshot", "restore", snapID, restoreDir)
	if err != nil {
		// return the error message to parse the object ID to be used in snapshot fix command
		return msg, err
	}

	return "", nil
}

// SetUpSystemUnderTest connects or creates a kopia repo, writes random data in source directory,
// creates snapshots of the source directory.
func (bm *SnapshotTester) SetUpSystemUnderTest() (string, error) {
	fileSize := 1 * 1024 * 1024
	numFiles := 50
	ctx := context.Background()

	err := bm.writeRandomFiles(ctx, fileSize, numFiles)
	if err != nil {
		return "", err
	}

	// create snapshot of the data
	bm.PathToTakeSnapshot = bm.FileHandler.GetRootDir(bm.fileWriter.DataDirectory(ctx))

	// create snapshot of the data
	log.Printf("Creating snapshot of directory %s", bm.PathToTakeSnapshot)

	snapshotID, _, err := bm.TakeSnapshot(bm.PathToTakeSnapshot)
	if err != nil {
		return "", err
	}

	log.Printf("Creating snapshot of snapshot ID %s", snapshotID)

	return snapshotID, nil
}

// GenerateRandomFiles connects or creates a Kopia repository that writes random data in source directory.
// Tests can later create snapshots from the source directory.
func (bm *SnapshotTester) GenerateRandomFiles(fileSize int, numFiles int) error {
	ctx := context.Background()

	err := bm.writeRandomFiles(ctx, fileSize, numFiles)
	if err != nil {
		return err
	}

	bm.PathToTakeSnapshot = bm.fileWriter.DataDirectory(ctx)

	return nil
}

// VerifySnapshot implements the Snapshotter interface to verify a kopia snapshot corruption
func (bm *SnapshotTester) VerifySnapshot() error {
	return bm.KopiaCommandRunner.VerifySnapshot("--verify-files-percent=100")
}

// TakeSnapshot creates snapshot of the provided directory.
func (bm *SnapshotTester) TakeSnapshot(dir string) (snapID, stdout string, err error) {
	err = bm.KopiaCommandRunner.ConnectRepo("filesystem", "--path="+bm.DataRepoPath)
	if err != nil {
		return "", "", err
	}

	msg, stdout, err := bm.KopiaCommandRunner.Run("snapshot", "create", dir, "--json")
	if err != nil {
		return msg, stdout, err
	}

	var m snapshot.Manifest

	err = json.Unmarshal([]byte(msg), &m)
	if err != nil {
		return "", "", err
	}

	snapID = string(m.ID)

	return snapID, stdout, nil
}
