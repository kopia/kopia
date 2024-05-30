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

// Comparer outputs diff information between two filesystems.
type Comparer struct {
	out    io.Writer
	tmpDir string

	DiffCommand   string
	DiffArguments []string
}

// Compare compares two filesystem entries and emits their diff information.
func (c *Comparer) Compare(ctx context.Context, e1, e2 fs.Entry) error {
	return c.compareEntry(ctx, e1, e2, ".")
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
	if h1, ok := e1.(object.HasObjectID); ok {
		if h2, ok := e2.(object.HasObjectID); ok {
			if h1.ObjectID() == h2.ObjectID() {
				log(ctx).Debugf("unchanged %v", path)
				return nil
			}
		}
	}

	if e1 == nil {
		if dir2, isDir2 := e2.(fs.Directory); isDir2 {
			c.output("added directory %v\n", path)
			return c.compareDirectories(ctx, nil, dir2, path)
		}

		c.output("added file %v (%v bytes)\n", path, e2.Size())

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
			return c.compareDirectories(ctx, dir1, nil, path)
		}

		c.output("removed file %v (%v bytes)\n", path, e1.Size())

		if f, ok := e1.(fs.File); ok {
			if err := c.compareFiles(ctx, f, nil, path); err != nil {
				return err
			}
		}

		return nil
	}

	compareEntry(e1, e2, path, c.out)

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
		return nil
	}

	if f1, ok := e1.(fs.File); ok {
		if f2, ok := e2.(fs.File); ok {
			c.output("changed %v at %v (size %v -> %v)\n", path, e2.ModTime().String(), e1.Size(), e2.Size())

			if err := c.compareFiles(ctx, f1, f2, path); err != nil {
				return err
			}
		}
	}

	return nil
}

func compareEntry(e1, e2 fs.Entry, fullpath string, out io.Writer) bool {
	if e1 == e2 { // in particular e1 == nil && e2 == nil
		return true
	}

	if e1 == nil {
		fmt.Fprintln(out, fullpath, "does not exist in source directory") //nolint:errcheck
		return false
	}

	if e2 == nil {
		fmt.Fprintln(out, fullpath, "does not exist in destination directory") //nolint:errcheck
		return false
	}

	equal := true

	if m1, m2 := e1.Mode(), e2.Mode(); m1 != m2 {
		equal = false

		fmt.Fprintln(out, fullpath, "modes differ: ", m1, m2) //nolint:errcheck
	}

	if s1, s2 := e1.Size(), e2.Size(); s1 != s2 {
		equal = false

		fmt.Fprintln(out, fullpath, "sizes differ: ", s1, s2) //nolint:errcheck
	}

	if mt1, mt2 := e1.ModTime(), e2.ModTime(); !mt1.Equal(mt2) {
		equal = false

		fmt.Fprintln(out, fullpath, "modification times differ: ", mt1, mt2) //nolint:errcheck
	}

	o1, o2 := e1.Owner(), e2.Owner()
	if o1.UserID != o2.UserID {
		equal = false

		fmt.Fprintln(out, fullpath, "owner users differ: ", o1.UserID, o2.UserID) //nolint:errcheck
	}

	if o1.GroupID != o2.GroupID {
		equal = false

		fmt.Fprintln(out, fullpath, "owner groups differ: ", o1.GroupID, o2.GroupID) //nolint:errcheck
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
