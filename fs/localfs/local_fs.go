package localfs

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/kopialogging"
)

var log = kopialogging.Logger("kopia/localfs")

type sortedEntries fs.Entries

func (e sortedEntries) Len() int      { return len(e) }
func (e sortedEntries) Swap(i, j int) { e[i], e[j] = e[j], e[i] }
func (e sortedEntries) Less(i, j int) bool {
	return e[i].Name() < e[j].Name()
}

type filesystemEntry struct {
	os.FileInfo

	path string
}

func (e filesystemEntry) Owner() fs.OwnerInfo {
	return platformSpecificOwnerInfo(e)
}

func newEntry(md os.FileInfo, path string) filesystemEntry {
	return filesystemEntry{md, path}
}

type filesystemDirectory struct {
	filesystemEntry
}

type filesystemSymlink struct {
	filesystemEntry
}

type filesystemFile struct {
	filesystemEntry
}

func (fsd *filesystemDirectory) Size() int64 {
	// force directory size to always be zero
	return 0
}

func (fsd *filesystemDirectory) Summary() *fs.DirectorySummary {
	return nil
}

func (fsd *filesystemDirectory) Readdir(ctx context.Context) (fs.Entries, error) {
	f, direrr := os.Open(fsd.path)
	if direrr != nil {
		return nil, direrr
	}
	defer f.Close() //nolint:errcheck

	var entries fs.Entries

	for {
		fileInfos, err := f.Readdir(16)
		for _, fi := range fileInfos {
			e, fierr := entryFromFileInfo(fi, filepath.Join(fsd.path, fi.Name()))
			if fierr != nil {
				log.Warningf("unable to create directory entry %q: %v", fi.Name(), fierr)
				continue
			}
			entries = append(entries, e)
		}
		if err == nil {
			continue
		}
		if err == io.EOF {
			break
		}
		return nil, err
	}

	sort.Sort(sortedEntries(entries))

	return entries, nil
}

type fileWithMetadata struct {
	*os.File
}

func (f *fileWithMetadata) Entry() (fs.Entry, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	return &filesystemFile{newEntry(fi, f.Name())}, nil
}

func (fsf *filesystemFile) Open(ctx context.Context) (fs.Reader, error) {
	f, err := os.Open(fsf.path)
	if err != nil {
		return nil, err
	}

	return &fileWithMetadata{f}, nil
}

func (fsl *filesystemSymlink) Readlink(ctx context.Context) (string, error) {
	return os.Readlink(fsl.path)
}

// NewEntry returns fs.Entry for the specified path, the result will be one of supported entry types: fs.File, fs.Directory, fs.Symlink.
func NewEntry(path string) (fs.Entry, error) {
	fi, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}

	return entryFromFileInfo(fi, path)
}

// Directory returns fs.Directory for the specified path.
func Directory(path string) (fs.Directory, error) {
	e, err := NewEntry(path)
	if err != nil {
		return nil, err
	}

	switch e := e.(type) {
	case fs.Directory:
		return e, nil

	default:
		return nil, errors.Errorf("not a directory: %v", path)
	}
}

func entryFromFileInfo(fi os.FileInfo, path string) (fs.Entry, error) {
	switch fi.Mode() & os.ModeType {
	case os.ModeDir:
		return &filesystemDirectory{newEntry(fi, path)}, nil

	case os.ModeSymlink:
		return &filesystemSymlink{newEntry(fi, path)}, nil

	case 0:
		return &filesystemFile{newEntry(fi, path)}, nil

	default:
		return nil, errors.Errorf("unsupported filesystem entry: %v", path)
	}
}

var _ fs.Directory = &filesystemDirectory{}
var _ fs.File = &filesystemFile{}
var _ fs.Symlink = &filesystemSymlink{}
