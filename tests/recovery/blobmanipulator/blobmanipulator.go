package blobmanipulator

// Package blobmanipulator provides the framework for snapshot fix testing
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

func (bm *BlobManipulator) ConnectOrCreateRepo(dataRepoPath string) error {
	return bm.DirCreater.ConnectOrCreateRepo(dataRepoPath)
}

// Delete provided or a random blob in kopia repo
func (bm *BlobManipulator) DeleteBlob(blobID string) (err error) {

	if blobID == "" {
		blobID, err = bm.getBlobIDRand()
		if err != nil {
			return err
		}
	}
	log.Printf("Deleting BLOB %s", blobID)

	_, _, err = bm.KopiaCommandRunner.Run("blob", "delete", blobID, "--advanced-commands=enabled")
	if err != nil {
		return err
	}

	return nil
}

func (bm *BlobManipulator) getBlobIDRand() (blobToBeDeleted string, err error) {
	var b []blob.Metadata
	// assumption: the repo under test is in filesystem
	err = bm.KopiaCommandRunner.ConnectRepo("filesystem", "--path="+bm.DataRepoPath)
	if err != nil {
		return "", err
	}

	blobIDList, _, _ := bm.KopiaCommandRunner.Run("blob", "list", "--json")
	if len(blobIDList) == 0 {
		return "", robustness.ErrNoOp
	}
	err = json.Unmarshal([]byte(blobIDList), &b)
	if err != nil {
		return "", err
	}

	// Select the first pack blob in the list
	for _, s := range b {
		temp := string(s.BlobID)
		if strings.HasPrefix(temp, "p") {
			blobToBeDeleted = string(s.BlobID)
			break
		}
	}
	return blobToBeDeleted, nil
}

func (bm *BlobManipulator) getFileWriter() bool {
	fw, err := fiofilewriter.New()
	if err != nil {
		if errors.Is(err, fio.ErrEnvNotSet) {
			log.Println("Skipping robustness tests because FIO environment is not set")

		} else {
			log.Println("Error creating fio FileWriter:", err)
		}

		return false
	}

	bm.fileWriter = fw

	return true
}

func (bm *BlobManipulator) RestoreGivenOrRandomSnapshot(snapID string, restoreDir string) (stdout string, err error) {
	err = bm.KopiaCommandRunner.ConnectRepo("filesystem", "--path="+bm.DataRepoPath)
	if err != nil {
		return "", err
	}

	if snapID == "" {
		// list available snaphsots
		stdout, _, err = bm.KopiaCommandRunner.Run("snapshot", "list", "--json")
		if err != nil {
			return stdout, err
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

func (bm *BlobManipulator) SetUpSystemUnderTest() {
	err := bm.ConnectOrCreateRepo(bm.DataRepoPath)
	if err != nil {
		return
	}

	// create random data
	bm.getFileWriter()
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
		return
	}

	// create snapshot of the data
	snapPath := bm.fileWriter.DataDirectory(ctx)
	log.Printf("Creating snapshot of directory %s", snapPath)

	_, err = bm.TakeSnapshot(snapPath)
	if err != nil {
		return
	}

	// delete this, error case only
	_, err = bm.fileWriter.WriteRandomFiles(ctx, fileWriteOpts)
	if err != nil {
		return
	}
	log.Printf("Creating snapshot of directory %s", snapPath)
	_, err = bm.TakeSnapshot(snapPath)
	if err != nil {
		return
	}

	// create snapshot of the data
	log.Printf("Creating snapshot of directory %s", snapPath)

	_, err = bm.TakeSnapshot(snapPath)
	if err != nil {
		return
	}

}

func (bm *BlobManipulator) TakeSnapshot(dir string) (stdout string, err error) {
	err = bm.KopiaCommandRunner.ConnectRepo("filesystem", "--path="+bm.DataRepoPath)
	if err != nil {
		return "", err
	}
	stdout, _, err = bm.KopiaCommandRunner.Run("snapshot", "create", dir)
	if err != nil {
		return stdout, err
	}

	return "", nil
}

func (bm *BlobManipulator) SnapshotFixRemoveFilesByBlobID(blobID string) (stdout string, err error) {
	// Get hold of object ID that can be used in the snapshot fix command
	stdout, msg, err := bm.KopiaCommandRunner.Run("snapshot", "fix", "remove-files", "--object-id="+blobID, "--commit")
	if err != nil {
		log.Println(stdout, msg)
		return stdout, err
	}

	return "", nil
}

func (bm *BlobManipulator) SnapshotFixRemoveFilesByFilename(filename string) (stdout string, err error) {
	// Get hold of the filename that can be used to in the snapshot fix command
	stdout, msg, err := bm.KopiaCommandRunner.Run("snapshot", "fix", "remove-files", "--filename="+filename, "--commit")
	if err != nil {
		log.Println(stdout, msg)
		return stdout, err
	}

	return "", nil
}
