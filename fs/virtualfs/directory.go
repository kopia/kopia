package virtualfs

import (
	"context"
	"os"
	"path"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
)

// Directory is a mock in-memory implementation of fs.Directory.
type Directory struct {
	entry

	children fs.Entries
}

var _ fs.Directory = (*Directory)(nil)

// AddDir adds a directory with a given name and permissions.
func (imd *Directory) AddDir(name string, permissions os.FileMode) (*Directory, error) {
	subdir := &Directory{
		entry: entry{
			name: name,
			mode: permissions | os.ModeDir,
		},
	}

	if err := imd.addChild(subdir); err != nil {
		return nil, err
	}

	return subdir, nil
}

// AddAllDirs creates under imd, all the necessary directories in the pathname, similar to os.MkdirAll.
func (imd *Directory) AddAllDirs(pathname string, permissions os.FileMode) (subdir *Directory, err error) {
	p, missing, err := imd.resolveDirs(pathname)
	if err != nil {
		return nil, err
	}

	for _, n := range missing {
		if p, err = p.AddDir(n, permissions); err != nil {
			return nil, errors.Wrapf(err, "unable to add sub directory '%s'", n)
		}
	}

	return p, nil
}

// Child gets the named child of a directory.
func (imd *Directory) Child(ctx context.Context, name string) (fs.Entry, error) {
	return fs.ReadDirAndFindChild(ctx, imd, name)
}

// Readdir gets the contents of a directory.
func (imd *Directory) Readdir(ctx context.Context) (fs.Entries, error) {
	return append(fs.Entries(nil), imd.children...), nil
}

// Remove removes directory entry with the given name.
func (imd *Directory) Remove(name string) {
	newChildren := imd.children[:0]

	for _, e := range imd.children {
		if e.Name() != name {
			newChildren = append(newChildren, e)
		}
	}

	imd.children = newChildren
}

// Subdir finds a subdirectory with the given name.
func (imd *Directory) Subdir(name string) (*Directory, error) {
	curr := imd

	subdir := curr.children.FindByName(name)
	if subdir == nil {
		return nil, errors.Errorf("'%s' not found in '%s'", name, curr.Name())
	}

	if !subdir.IsDir() {
		return nil, errors.Errorf("'%s' is not a directory in '%s'", name, curr.Name())
	}

	return subdir.(*Directory), nil
}

// Summary returns summary of a directory.
func (imd *Directory) Summary() *fs.DirectorySummary {
	return nil
}

// addChild adds the given entry under imd, errors out if the entry is already present.
func (imd *Directory) addChild(e fs.Entry) error {
	if strings.Contains(e.Name(), "/") {
		return errors.New("unable to add child entry: name cannot contain '/'")
	}

	child := imd.children.FindByName(e.Name())
	if child != nil {
		return errors.New("unable to add child entry: already exists")
	}

	imd.children = append(imd.children, e)
	imd.children.Sort()

	return nil
}

// resolveDirs finds the directories in the pathname under imd and returns a list of missing sub directories.
func (imd *Directory) resolveDirs(pathname string) (parent *Directory, missing []string, err error) {
	if pathname == "" {
		return imd, nil, nil
	}

	p := imd

	parts := strings.Split(path.Clean(pathname), "/")
	for i, n := range parts {
		i2 := p.children.FindByName(n)
		if i2 == nil {
			return p, parts[i:], nil
		}

		if !i2.IsDir() {
			return nil, nil, errors.Errorf("'%s' is not a directory in '%s'", n, p.Name())
		}

		p = i2.(*Directory)
	}

	return p, nil, nil
}

// AddFileWithContent adds a virtual file with specified name, permissions and content.
func AddFileWithContent(imd *Directory, filePath string, content []byte, dirPermissions, filePermissions os.FileMode) (*File, error) {
	dir, name := path.Split(filePath)

	p, err := imd.AddAllDirs(dir, dirPermissions)
	if err != nil {
		return nil, err
	}

	f := FileWithContent(name, filePermissions, content)
	if err := p.addChild(f); err != nil {
		return nil, errors.Wrap(err, "unable to add file")
	}

	return f, nil
}

// AddFileWithStdinSource adds a virtual file with the specified name, permissions and stdin source.
func AddFileWithStdinSource(imd *Directory, filePath string, dirPermissions, filePermissions os.FileMode) (*File, error) {
	dir, name := path.Split(filePath)

	p, err := imd.AddAllDirs(dir, dirPermissions)
	if err != nil {
		return nil, err
	}

	source := func() (ReaderSeekerCloser, error) {
		return readCloserWrapper{os.Stdin}, nil
	}

	f := FileWithSource(name, filePermissions, source)
	if err := p.addChild(f); err != nil {
		return nil, errors.Wrap(err, "unable to add file")
	}

	return f, nil
}
