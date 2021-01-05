package diff

import (
	"os"
	"os/exec"
	"time"
)

// ComparerOutput is the interface for displaying results.
type ComparerOutput interface {
	// A directory was added.
	AddDirectory(path string)

	// A directory was removed.
	RemoveDirectory(path string)

	// A directory was replaced by a non-directory.
	ChangeDirNondir(path string)

	// A non-directory was replaced by a directory.
	ChangeNondirDir(path string)

	// A file of certain size was added.
	AddFile(path string, size int64)

	// A file of certain size was removed.
	RemoveFile(path string, size int64)

	// The properties of a file were changed.
	ChangeFile(path string, modTime time.Time, size1 int64, size2 int64)

	// Path does not exist in the source, but exists in the destination.
	NotExistSource(path string)

	// Path does not exist in the destination, but exists in the source.
	NotExistDest(path string)

	// Permission flags of the path differ.
	ModesDiffer(path string, mode1 os.FileMode, mode2 os.FileMode)

	// Size of the file differs.
	SizesDiffer(path string, size1 int64, size2 int64)

	// Modification time of the path differs.
	ModTimeDiffer(path string, modtime1 time.Time, modtime2 time.Time)

	// Owner user id of the path differs.
	OwnerDiffer(path string, userID1 uint32, userID2 uint32)

	// Owner group of the path differs.
	GroupDiffer(path string, groupID1 uint32, groupID2 uint32)

	// Runs a prepared diff command and handles its output.
	RunDiffCommand(path1 string, path2 string, cmd *exec.Cmd)

	// Close the output; no further calls shall be issued.
	Close()
}
