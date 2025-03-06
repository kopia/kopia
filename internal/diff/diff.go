// Package diff implements helpers for comparing two filesystems.
package diff

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/iocopy"
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/repo/object"
)

const dirMode = 0o700

var log = logging.Module("diff")

// EntryTypeStats accumulates specific stats for the snapshots being compared.
type EntryTypeStats struct {
	Added    uint32 `json:"added"`
	Removed  uint32 `json:"removed"`
	Modified uint32 `json:"modified"`

	// aggregate stats
	SameContentButDifferentMetadata uint32 `json:"sameContentButDifferentMetadata"`

	// stats categorized based on metadata
	SameContentButDifferentMode             uint32 `json:"sameContentButDifferentMode"`
	SameContentButDifferentModificationTime uint32 `json:"sameContentButDifferentModificationTime"`
	SameContentButDifferentUserOwner        uint32 `json:"sameContentButDifferentUserOwner"`
	SameContentButDifferentGroupOwner       uint32 `json:"sameContentButDifferentGroupOwner"`
}

// Stats accumulates stats between snapshots being compared.
type Stats struct {
	FileEntries      EntryTypeStats `json:"fileEntries"`
	DirectoryEntries EntryTypeStats `json:"directoryEntries"`
}

// Comparer outputs diff information between two filesystems.
type Comparer struct {
	stats         Stats
	out           io.Writer
	tmpDir        string
	DiffCommand   string
	DiffArguments []string
}

// Compare compares two filesystem entries and emits their diff information.
func (c *Comparer) Compare(ctx context.Context, e1, e2 fs.Entry) (Stats, error) {
	c.stats = Stats{}

	err := c.compareEntry(ctx, e1, e2, ".")
	if err != nil {
		return c.stats, err
	}

	return c.stats, errors.Wrap(err, "error comparing fs entries")
}

// Close removes all temporary files used by the comparer.
func (c *Comparer) Close() error {
	//nolint:wrapcheck
	return os.RemoveAll(c.tmpDir)
}

func maybeOID(e fs.Entry) string {
	if h, ok := e.(object.HasObjectID); ok {
		return h.ObjectID().String()
	}

	return ""
}

func (c *Comparer) compareDirectories(ctx context.Context, dir1, dir2 fs.Directory, parent string) error {
	log(ctx).Debugf("comparing directories %v (%v and %v)", parent, maybeOID(dir1), maybeOID(dir2))

	var entries1, entries2 []fs.Entry

	var err error

	if dir1 != nil {
		entries1, err = fs.GetAllEntries(ctx, dir1)
		if err != nil {
			return errors.Wrapf(err, "unable to read first directory %v", parent)
		}
	}

	if dir2 != nil {
		entries2, err = fs.GetAllEntries(ctx, dir2)
		if err != nil {
			return errors.Wrapf(err, "unable to read second directory %v", parent)
		}
	}

	return c.compareDirectoryEntries(ctx, entries1, entries2, parent)
}

//nolint:gocyclo
func (c *Comparer) compareEntry(ctx context.Context, e1, e2 fs.Entry, path string) error {
	// see if we have the same object IDs, which implies identical objects, thanks to content-addressable-storage
	h1, e1HasObjectID := e1.(object.HasObjectID)
	h2, e2HasObjectID := e2.(object.HasObjectID)

	if e1HasObjectID && e2HasObjectID {
		if h1.ObjectID() == h2.ObjectID() {
			if _, isDir := e1.(fs.Directory); isDir {
				c.compareDirMetadataAndComputeStats(ctx, e1, e2, path)
			} else {
				c.compareFileMetadataAndComputeStats(ctx, e1, e2, path)
			}

			return nil
		}
	}

	if e1 == nil {
		if dir2, isDir2 := e2.(fs.Directory); isDir2 {
			c.output("added directory %v\n", path)

			c.stats.DirectoryEntries.Added++

			return c.compareDirectories(ctx, nil, dir2, path)
		}

		c.output("added file %v (%v bytes)\n", path, e2.Size())

		c.stats.FileEntries.Added++

		if f, ok := e2.(fs.File); ok {
			if err := c.compareFiles(ctx, nil, f, path); err != nil {
				return err
			}
		}

		return nil
	}

	if e2 == nil {
		if dir1, isDir1 := e1.(fs.Directory); isDir1 {
			c.output("removed directory %v\n", path)

			c.stats.DirectoryEntries.Removed++

			return c.compareDirectories(ctx, dir1, nil, path)
		}

		c.output("removed file %v (%v bytes)\n", path, e1.Size())

		c.stats.FileEntries.Removed++

		if f, ok := e1.(fs.File); ok {
			if err := c.compareFiles(ctx, f, nil, path); err != nil {
				return err
			}
		}

		return nil
	}

	c.compareEntryMetadata(e1, e2, path)

	dir1, isDir1 := e1.(fs.Directory)
	dir2, isDir2 := e2.(fs.Directory)

	if isDir1 {
		if !isDir2 {
			// right is a non-directory, left is a directory
			c.output("changed %v from directory to non-directory\n", path)
			return nil
		}

		return c.compareDirectories(ctx, dir1, dir2, path)
	}

	if isDir2 {
		// left is non-directory, right is a directory
		log(ctx).Infof("changed %v from non-directory to a directory", path)
		c.output("changed %v from non-directory to a directory\n", path)

		return nil
	}

	if f1, ok := e1.(fs.File); ok {
		if f2, ok := e2.(fs.File); ok {
			c.output("changed %v at %v (size %v -> %v)\n", path, e2.ModTime().String(), e1.Size(), e2.Size())

			c.stats.FileEntries.Modified++

			if err := c.compareFiles(ctx, f1, f2, path); err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *Comparer) compareDirMetadataAndComputeStats(ctx context.Context, e1, e2 fs.Entry, path string) {
	// check for metadata changes pertaining to directories given that content hasn't changed and gather aggregate statistics
	equal := true

	if m1, m2 := e1.Mode(), e2.Mode(); m1 != m2 {
		equal = false
		c.stats.DirectoryEntries.SameContentButDifferentMode++
	}

	if mt1, mt2 := e1.ModTime(), e2.ModTime(); !mt1.Equal(mt2) {
		equal = false
		c.stats.DirectoryEntries.SameContentButDifferentModificationTime++
	}

	o1, o2 := e1.Owner(), e2.Owner()
	if o1.UserID != o2.UserID {
		equal = false
		c.stats.DirectoryEntries.SameContentButDifferentUserOwner++
	}

	if o1.GroupID != o2.GroupID {
		equal = false
		c.stats.DirectoryEntries.SameContentButDifferentGroupOwner++
	}

	if !equal {
		c.stats.DirectoryEntries.SameContentButDifferentMetadata++

		log(ctx).Debugf("content unchanged but metadata has been modified: %v", path)
	}
}

func (c *Comparer) compareFileMetadataAndComputeStats(ctx context.Context, e1, e2 fs.Entry, path string) {
	// check for metadata changes pertaining to files given that content hasn't changed and gather aggregate statistics
	equal := true
	if m1, m2 := e1.Mode(), e2.Mode(); m1 != m2 {
		equal = false
		c.stats.FileEntries.SameContentButDifferentMode++
	}

	if mt1, mt2 := e1.ModTime(), e2.ModTime(); !mt1.Equal(mt2) {
		equal = false
		c.stats.FileEntries.SameContentButDifferentModificationTime++
	}

	o1, o2 := e1.Owner(), e2.Owner()
	if o1.UserID != o2.UserID {
		equal = false
		c.stats.FileEntries.SameContentButDifferentUserOwner++
	}

	if o1.GroupID != o2.GroupID {
		equal = false
		c.stats.FileEntries.SameContentButDifferentGroupOwner++
	}

	if !equal {
		c.stats.FileEntries.SameContentButDifferentMetadata++

		log(ctx).Debugf("content unchanged but metadata has been modified: %v", path)
	}
}

func (c *Comparer) compareEntryMetadata(e1, e2 fs.Entry, fullpath string) bool {
	if e1 == e2 { // in particular e1 == nil && e2 == nil
		return true
	}

	if e1 == nil {
		c.output("%v does not exist in source directory\n", fullpath)
		return false
	}

	if e2 == nil {
		c.output("%v does not exist in destination directory\n", fullpath)
		return false
	}

	equal := true

	if m1, m2 := e1.Mode(), e2.Mode(); m1 != m2 {
		equal = false

		c.output("%v modes differ: %v %v\n", fullpath, m1, m2)
	}

	if s1, s2 := e1.Size(), e2.Size(); s1 != s2 {
		equal = false

		c.output("%v sizes differ: %v %v\n", fullpath, s1, s2)
	}

	if mt1, mt2 := e1.ModTime(), e2.ModTime(); !mt1.Equal(mt2) {
		equal = false

		c.output("%v modification times differ: %v %v\n", fullpath, mt1, mt2)
	}

	o1, o2 := e1.Owner(), e2.Owner()
	if o1.UserID != o2.UserID {
		equal = false

		c.output("%v owner users differ: %v %v\n", fullpath, o1.UserID, o2.UserID)
	}

	if o1.GroupID != o2.GroupID {
		equal = false

		c.output("%v owner groups differ: %v %v\n", fullpath, o1.GroupID, o2.GroupID)
	}

	_, isDir1 := e1.(fs.Directory)
	_, isDir2 := e2.(fs.Directory)

	if !equal {
		if isDir1 && isDir2 {
			c.stats.DirectoryEntries.Modified++
		} else {
			c.stats.FileEntries.Modified++
		}
	}

	// don't compare filesystem boundaries (e1.Device()), it's pretty useless and is not stored in backups

	return equal
}

func (c *Comparer) compareDirectoryEntries(ctx context.Context, entries1, entries2 []fs.Entry, dirPath string) error {
	e1byname := map[string]fs.Entry{}
	for _, e1 := range entries1 {
		e1byname[e1.Name()] = e1
	}

	for _, e2 := range entries2 {
		entryName := e2.Name()
		if err := c.compareEntry(ctx, e1byname[entryName], e2, dirPath+"/"+entryName); err != nil {
			return errors.Wrapf(err, "error comparing %v", entryName)
		}

		delete(e1byname, entryName)
	}

	// at this point e1byname only has entries present in entries1 but not entries2, those are the deleted ones
	for _, e1 := range entries1 {
		entryName := e1.Name()
		if _, ok := e1byname[entryName]; ok {
			if err := c.compareEntry(ctx, e1, nil, dirPath+"/"+entryName); err != nil {
				return errors.Wrapf(err, "error comparing %v", entryName)
			}
		}
	}

	return nil
}

func (c *Comparer) compareFiles(ctx context.Context, f1, f2 fs.File, fname string) error {
	if c.DiffCommand == "" {
		return nil
	}

	oldName := "/dev/null"
	newName := "/dev/null"

	if f1 != nil {
		oldName = filepath.Join("old", fname)
		oldFile := filepath.Join(c.tmpDir, oldName)

		if err := downloadFile(ctx, f1, oldFile); err != nil {
			return errors.Wrap(err, "error downloading old file")
		}

		defer os.Remove(oldFile) //nolint:errcheck
	}

	if f2 != nil {
		newName = filepath.Join("new", fname)
		newFile := filepath.Join(c.tmpDir, newName)

		if err := downloadFile(ctx, f2, newFile); err != nil {
			return errors.Wrap(err, "error downloading new file")
		}
		defer os.Remove(newFile) //nolint:errcheck
	}

	var args []string
	args = append(args, c.DiffArguments...)
	args = append(args, oldName, newName)

	cmd := exec.CommandContext(ctx, c.DiffCommand, args...) //nolint:gosec
	cmd.Dir = c.tmpDir
	cmd.Stdout = c.out
	cmd.Stderr = c.out
	cmd.Run() //nolint:errcheck

	return nil
}

func downloadFile(ctx context.Context, f fs.File, fname string) error {
	if err := os.MkdirAll(filepath.Dir(fname), dirMode); err != nil {
		return errors.Wrap(err, "error making directory")
	}

	src, err := f.Open(ctx)
	if err != nil {
		return errors.Wrap(err, "error opening object")
	}
	defer src.Close() //nolint:errcheck

	dst, err := os.Create(fname) //nolint:gosec
	if err != nil {
		return errors.Wrap(err, "error creating file to edit")
	}

	defer dst.Close() //nolint:errcheck

	return errors.Wrap(iocopy.JustCopy(dst, src), "error downloading file")
}

// Stats returns aggregated statistics computed during snapshot comparison
// must be invoked after a call to Compare which populates ComparerStats struct.
func (c *Comparer) Stats() Stats {
	return c.stats
}

func (c *Comparer) output(msg string, args ...interface{}) {
	fmt.Fprintf(c.out, msg, args...) //nolint:errcheck
}

// NewComparer creates a comparer for a given repository that will output the results to a given writer.
func NewComparer(out io.Writer) (*Comparer, error) {
	tmp, err := os.MkdirTemp("", "kopia")
	if err != nil {
		return nil, errors.Wrap(err, "error creating temp directory")
	}

	return &Comparer{out: out, tmpDir: tmp}, nil
}
