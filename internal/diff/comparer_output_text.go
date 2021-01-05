package diff

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"time"
)

// ComparerOutputText is a basic standard output compare output.
type ComparerOutputText struct {
	out io.Writer
}

// AddDirectory is called when a directory was added.
func (cot *ComparerOutputText) AddDirectory(path string) {
	fmt.Fprintf(cot.out, "added directory %v\n", path)
}

// RemoveDirectory is called when a directory was removed.
func (cot *ComparerOutputText) RemoveDirectory(path string) {
	fmt.Fprintf(cot.out, "removed directory %v\n", path)
}

// ChangeDirNondir is called when a directory was replaced by a non-directory.
func (cot *ComparerOutputText) ChangeDirNondir(path string) {
	fmt.Fprintf(cot.out, "changed %v from directory to non-directory\n", path)
}

// ChangeNondirDir is called when a non-directory was replaced by a directory.
func (cot *ComparerOutputText) ChangeNondirDir(path string) {
	fmt.Fprintf(cot.out, "changed %v from non-directory to a directory\n", path)
}

// ChangeFile is called when the properties of a file were changed.
func (cot *ComparerOutputText) ChangeFile(path string, modTime time.Time, size1, size2 int64) {
	fmt.Fprintf(cot.out, "changed %v at %v (size %v -> %v)\n", path, modTime.String(), size1, size2)
}

// AddFile is called when a file of certain size was added.
func (cot *ComparerOutputText) AddFile(path string, size int64) {
	fmt.Fprintf(cot.out, "added file %v (%v bytes)\n", path, size)
}

// RemoveFile is called when a file of certain size was removed.
func (cot *ComparerOutputText) RemoveFile(path string, size int64) {
	fmt.Fprintf(cot.out, "removed file %v (%v bytes)\n", path, size)
}

// RunDiffCommand runs the given prepared command for the purpose of comparing two files.
func (cot *ComparerOutputText) RunDiffCommand(path1, path2 string, cmd *exec.Cmd) {
	cmd.Stdout = cot.out
	cmd.Stderr = cot.out
	cmd.Run() //nolint:errcheck
}

// NotExistSource is called when the path does not exist in the source, but exists in the destination.
func (cot *ComparerOutputText) NotExistSource(path string) {
	fmt.Fprintf(cot.out, "%v does not exist in source directory\n", path)
}

// NotExistDest is called when the path does not exist in the destination, but exists in the source.
func (cot *ComparerOutputText) NotExistDest(path string) {
	fmt.Fprintf(cot.out, "%v does not exist in destination directory\n", path)
}

// ModesDiffer is called when the permission flags of the path differ.
func (cot *ComparerOutputText) ModesDiffer(path string, mode1, mode2 os.FileMode) {
	fmt.Fprintf(cot.out, "%v modes differ: %v %v\n", path, mode1, mode2)
}

// SizesDiffer is called when the size of the file differs.
func (cot *ComparerOutputText) SizesDiffer(path string, size1, size2 int64) {
	fmt.Fprintf(cot.out, "%v sizes differ: %v %v\n", path, size1, size2)
}

// ModTimeDiffer is called when the modification time of the path differs.
func (cot *ComparerOutputText) ModTimeDiffer(path string, modtime1, modtime2 time.Time) {
	fmt.Fprintf(cot.out, "%v modification times differ: %v %v\n", path, modtime1, modtime2)
}

// OwnerDiffer is called when the owner user id of the path differs.
func (cot *ComparerOutputText) OwnerDiffer(path string, userID1, userID2 uint32) {
	fmt.Fprintf(cot.out, "%v owner users differ: %v %v\n", path, userID1, userID2)
}

// GroupDiffer is called when the owner group of the path differs.
func (cot *ComparerOutputText) GroupDiffer(path string, groupID1, groupID2 uint32) {
	fmt.Fprintf(cot.out, "%v owner groups differ: %v %v\n", path, groupID1, groupID2)
}

// Close the output; no further calls shall be issued.
func (cot *ComparerOutputText) Close() {
	// no particular operations on close
}
