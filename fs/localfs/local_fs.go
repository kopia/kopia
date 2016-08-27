package localfs

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/kopia/kopia/fs"
)

const (
	directoryReadAhead = 1024
)

type sortedEntries fs.Entries

func (e sortedEntries) Len() int      { return len(e) }
func (e sortedEntries) Swap(i, j int) { e[i], e[j] = e[j], e[i] }
func (e sortedEntries) Less(i, j int) bool {
	return e[i].Metadata().Name < e[j].Metadata().Name
}

type filesystemEntry struct {
	parent   fs.Directory
	metadata *fs.EntryMetadata
	path     string
}

func newEntry(md *fs.EntryMetadata, parent fs.Directory, path string) filesystemEntry {
	return filesystemEntry{parent, md, path}
}

func (e *filesystemEntry) Parent() fs.Directory {
	return e.parent
}

func (e *filesystemEntry) Metadata() *fs.EntryMetadata {
	return e.metadata
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

func (fsd *filesystemDirectory) Readdir() (fs.Entries, error) {
	f, err := os.Open(fsd.path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var entries fs.Entries

	for {
		fileInfos, err := f.Readdir(16)
		for _, fi := range fileInfos {
			e, err := entryFromFileInfo(fi, filepath.Join(fsd.path, fi.Name()), fsd)
			if err != nil {
				return nil, err
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

func (erc *fileWithMetadata) EntryMetadata() (*fs.EntryMetadata, error) {
	fi, err := erc.Stat()
	if err != nil {
		return nil, err
	}

	return entryMetadataFromFileInfo(fi), nil
}

func (fsf *filesystemFile) Open() (fs.Reader, error) {
	f, err := os.Open(fsf.path)
	if err != nil {
		return nil, err
	}

	return &fileWithMetadata{f}, nil
}

func (fsl *filesystemSymlink) Readlink() (string, error) {
	return os.Readlink(fsl.path)
}

// NewEntry returns fs.Entry for the specified path, the result will be one of supported entry types: fs.File, fs.Directory, fs.Symlink.
func NewEntry(path string, parent fs.Directory) (fs.Entry, error) {
	fi, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}

	return entryFromFileInfo(fi, path, parent)
}

// Directory returns fs.Directory for the specified path.
func Directory(path string, parent fs.Directory) (fs.Directory, error) {
	e, err := NewEntry(path, parent)
	if err != nil {
		return nil, err
	}

	switch e := e.(type) {
	case fs.Directory:
		return e, nil

	default:
		return nil, fmt.Errorf("not a directory: %v", path)
	}
}

func entryMetadataFromFileInfo(fi os.FileInfo) *fs.EntryMetadata {
	e := &fs.EntryMetadata{
		Name:        filepath.Base(fi.Name()),
		Type:        entryTypeFromFileMode(fi.Mode() & os.ModeType),
		Permissions: fs.Permissions(fi.Mode() & os.ModePerm),
		ModTime:     fi.ModTime().UTC(),
	}

	if fi.Mode().IsRegular() {
		e.FileSize = fi.Size()
	}

	populatePlatformSpecificEntryDetails(e, fi)
	return e
}

func entryTypeFromFileMode(t os.FileMode) fs.EntryType {
	switch t {
	case 0:
		return fs.EntryTypeFile

	case os.ModeSymlink:
		return fs.EntryTypeSymlink

	case os.ModeDir:
		return fs.EntryTypeDirectory

	default:
		panic("unsupported file mode: " + t.String())
	}
}

func entryFromFileInfo(fi os.FileInfo, path string, parent fs.Directory) (fs.Entry, error) {
	entry := newEntry(entryMetadataFromFileInfo(fi), parent, path)

	switch fi.Mode() & os.ModeType {
	case os.ModeDir:
		return &filesystemDirectory{entry}, nil

	case os.ModeSymlink:
		return &filesystemSymlink{entry}, nil

	case 0:
		return &filesystemFile{entry}, nil

	default:
		return nil, fmt.Errorf("unsupported filesystem entry: %v", path)
	}
}

var _ fs.Directory = &filesystemDirectory{}
var _ fs.File = &filesystemFile{}
var _ fs.Symlink = &filesystemSymlink{}
