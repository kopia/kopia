package localfs

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/kopialogging"
)

var log = kopialogging.Logger("kopia/localfs")

type sortedEntries fs.Entries

func (e sortedEntries) Len() int      { return len(e) }
func (e sortedEntries) Swap(i, j int) { e[i], e[j] = e[j], e[i] }
func (e sortedEntries) Less(i, j int) bool {
	return e[i].Metadata().Name < e[j].Metadata().Name
}

type filesystemEntry struct {
	metadata *fs.EntryMetadata
	path     string
}

func newEntry(md *fs.EntryMetadata, path string) filesystemEntry {
	return filesystemEntry{md, path}
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

func (erc *fileWithMetadata) EntryMetadata() (*fs.EntryMetadata, error) {
	fi, err := erc.Stat()
	if err != nil {
		return nil, err
	}
	return entryMetadataFromFileInfo(fi), nil
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

func entryFromFileInfo(fi os.FileInfo, path string) (fs.Entry, error) {
	switch fi.Mode() & os.ModeType {
	case os.ModeDir:
		return &filesystemDirectory{newEntry(entryMetadataFromFileInfo(fi), path)}, nil

	case os.ModeSymlink:
		return &filesystemSymlink{newEntry(entryMetadataFromFileInfo(fi), path)}, nil

	case 0:
		return &filesystemFile{newEntry(entryMetadataFromFileInfo(fi), path)}, nil

	default:
		return nil, fmt.Errorf("unsupported filesystem entry: %v", path)
	}
}

var _ fs.Directory = &filesystemDirectory{}
var _ fs.File = &filesystemFile{}
var _ fs.Symlink = &filesystemSymlink{}
