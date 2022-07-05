//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

// Package blobmanipulator provides the framework for snapshot fix testing.
package blobmanipulator

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
	"github.com/kopia/kopia/tests/robustness/fiofilewriter"
	"github.com/kopia/kopia/tests/robustness/snapmeta"
	"github.com/kopia/kopia/tests/tools/fio"
	"github.com/kopia/kopia/tests/tools/kopiarunner"
)

// BlobManipulator provides a way to run a kopia command.
type BlobManipulator struct {
	KopiaCommandRunner *kopiarunner.KopiaSnapshotter
	DirCreater         *snapmeta.KopiaSnapshotter
	fileWriter         *fiofilewriter.FileWriter

	DataRepoPath string
}

// NewBlobManipulator instantiates a new BlobManipulator and returns its pointer.
func NewBlobManipulator(baseDirPath string) (*BlobManipulator, error) {
	runner, err := kopiarunner.NewKopiaSnapshotter(baseDirPath)
	if err != nil {
		return nil, err
	}

	return &BlobManipulator{
		KopiaCommandRunner: runner,
	}, nil
}

// ConnectOrCreateRepo connects to an existing repository if possible or creates a new one.
func (bm *BlobManipulator) ConnectOrCreateRepo(dataRepoPath string) error {
	if bm == nil || bm.DirCreater == nil {
		err := errors.New("kopia " + dataRepoPath + "repository does not exist")
		return err
	}

	return bm.DirCreater.ConnectOrCreateRepo(dataRepoPath)
}

// DeleteBlob deletes the provided blob or a random blob, in kopia repo.
func (bm *BlobManipulator) DeleteBlob(blobID string) error {
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

func (bm *BlobManipulator) getBlobIDRand() (string, error) {
	var b []blob.Metadata

	// assumption: the repo under test is in filesystem
	err := bm.KopiaCommandRunner.ConnectRepo("filesystem", "--path="+bm.DataRepoPath)
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

func (bm *BlobManipulator) getFileWriter() bool {
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

// RestoreGivenOrRandomSnapshot restores a given or a random snapshot from kopia repository into the provided target directory.
func (bm *BlobManipulator) RestoreGivenOrRandomSnapshot(snapID, restoreDir string) (string, error) {
	err := bm.KopiaCommandRunner.ConnectRepo("filesystem", "--path="+bm.DataRepoPath)
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
		return msg, err
	}

	return "", nil
}

// SetUpSystemUnderTest connects or creates a kopia repo, writes random data in source directory,
// creates snapshots of the source directory.
func (bm *BlobManipulator) SetUpSystemUnderTest() error {
	err := bm.ConnectOrCreateRepo(bm.DataRepoPath)
	if err != nil {
		return err
	}

	// create random data
	gotFileWriter := bm.getFileWriter()
	if !gotFileWriter {
		err = errors.New("Error creating file writer")
		return err
	}

	fileSize := 100
	numFiles := 100

	fileWriteOpts := map[string]string{
		fiofilewriter.MaxDirDepthField:         strconv.Itoa(1),
		fiofilewriter.MaxFileSizeField:         strconv.Itoa(fileSize),
		fiofilewriter.MinFileSizeField:         strconv.Itoa(fileSize),
		fiofilewriter.MaxNumFilesPerWriteField: strconv.Itoa(numFiles),
		fiofilewriter.MinNumFilesPerWriteField: strconv.Itoa(numFiles),
	}

	ctx := context.Background()

	_, err = bm.fileWriter.WriteRandomFiles(ctx, fileWriteOpts)
	if err != nil {
		return err
	}

	// create snapshot of the data
	snapPath := bm.fileWriter.DataDirectory(ctx)
	log.Printf("Creating snapshot of directory %s", snapPath)

	_, err = bm.TakeSnapshot(snapPath)
	if err != nil {
		return err
	}

	numFiles = 50

	fileWriteOpts = map[string]string{
		fiofilewriter.MaxDirDepthField:         strconv.Itoa(1),
		fiofilewriter.MaxFileSizeField:         strconv.Itoa(fileSize),
		fiofilewriter.MinFileSizeField:         strconv.Itoa(fileSize),
		fiofilewriter.MaxNumFilesPerWriteField: strconv.Itoa(numFiles),
		fiofilewriter.MinNumFilesPerWriteField: strconv.Itoa(numFiles),
	}

	_, err = bm.fileWriter.WriteRandomFiles(ctx, fileWriteOpts)
	if err != nil {
		return err
	}

	fileSize = 40 * 1024 * 1024
	numFiles = 1

	fileWriteOpts = map[string]string{
		fiofilewriter.MaxDirDepthField:         strconv.Itoa(1),
		fiofilewriter.MaxFileSizeField:         strconv.Itoa(fileSize),
		fiofilewriter.MinFileSizeField:         strconv.Itoa(fileSize),
		fiofilewriter.MaxNumFilesPerWriteField: strconv.Itoa(numFiles),
		fiofilewriter.MinNumFilesPerWriteField: strconv.Itoa(numFiles),
	}

	_, err = bm.fileWriter.WriteRandomFiles(ctx, fileWriteOpts)
	if err != nil {
		return err
	}

	// create snapshot of the data
	log.Printf("Creating snapshot of directory %s", snapPath)

	_, err = bm.TakeSnapshot(snapPath)
	if err != nil {
		return err
	}

	return nil
}

// TakeSnapshot creates snapshot of the provided directory.
func (bm *BlobManipulator) TakeSnapshot(dir string) (string, error) {
	err := bm.KopiaCommandRunner.ConnectRepo("filesystem", "--path="+bm.DataRepoPath)
	if err != nil {
		return "", err
	}

	stdout, _, err := bm.KopiaCommandRunner.Run("snapshot", "create", dir)
	if err != nil {
		return stdout, err
	}

	return "", nil
}

// SnapshotFixRemoveFilesByBlobID runs snapshot fix remove-files command with a provided blob id.
func (bm *BlobManipulator) SnapshotFixRemoveFilesByBlobID(blobID string) (string, error) {
	// Get hold of object ID that can be used in the snapshot fix command
	output, msg, err := bm.KopiaCommandRunner.Run("snapshot", "fix", "remove-files", "--object-id="+blobID, "--commit")
	if err != nil {
		log.Println(output, msg)
		return output, err
	}

	return "", nil
}

// SnapshotFixRemoveFilesByFilename runs snapshot fix remove-files command with a provided file name.
func (bm *BlobManipulator) SnapshotFixRemoveFilesByFilename(filename string) (string, error) {
	// Get hold of the filename that can be used to in the snapshot fix command
	stdout, msg, err := bm.KopiaCommandRunner.Run("snapshot", "fix", "remove-files", "--filename="+filename, "--commit")
	if err != nil {
		log.Println(stdout, msg)
		return stdout, err
	}

	return "", nil
}
