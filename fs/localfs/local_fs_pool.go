package localfs

import "github.com/kopia/kopia/internal/freepool"

//nolint:gochecknoglobals
var (
	filesystemFilePool             = freepool.NewStruct(filesystemFile{})
	filesystemDirectoryPool        = freepool.NewStruct(filesystemDirectory{})
	filesystemSymlinkPool          = freepool.NewStruct(filesystemSymlink{})
	filesystemErrorEntryPool       = freepool.NewStruct(filesystemErrorEntry{})
	shallowFilesystemFilePool      = freepool.NewStruct(shallowFilesystemFile{})
	shallowFilesystemDirectoryPool = freepool.NewStruct(shallowFilesystemDirectory{})
)

func newFilesystemFile(e filesystemEntry) *filesystemFile {
	fsf := filesystemFilePool.Take()
	fsf.filesystemEntry = e

	return fsf
}

func (fsf *filesystemFile) Close() {
	filesystemFilePool.Return(fsf)
}

func newFilesystemDirectory(e filesystemEntry) *filesystemDirectory {
	fsd := filesystemDirectoryPool.Take()
	fsd.filesystemEntry = e

	return fsd
}

func (fsd *filesystemDirectory) Close() {
	filesystemDirectoryPool.Return(fsd)
}

func newFilesystemSymlink(e filesystemEntry) *filesystemSymlink {
	fsd := filesystemSymlinkPool.Take()
	fsd.filesystemEntry = e

	return fsd
}

func (fsl *filesystemSymlink) Close() {
	filesystemSymlinkPool.Return(fsl)
}

func newFilesystemErrorEntry(e filesystemEntry, err error) *filesystemErrorEntry {
	fse := filesystemErrorEntryPool.Take()
	fse.filesystemEntry = e
	fse.err = err

	return fse
}

func (e *filesystemErrorEntry) Close() {
	filesystemErrorEntryPool.Return(e)
}

func newShallowFilesystemFile(e filesystemEntry) *shallowFilesystemFile {
	fsf := shallowFilesystemFilePool.Take()
	fsf.filesystemEntry = e

	return fsf
}

func (fsf *shallowFilesystemFile) Close() {
	shallowFilesystemFilePool.Return(fsf)
}

func newShallowFilesystemDirectory(e filesystemEntry) *shallowFilesystemDirectory {
	fsf := shallowFilesystemDirectoryPool.Take()
	fsf.filesystemEntry = e

	return fsf
}

func (fsd *shallowFilesystemDirectory) Close() {
	shallowFilesystemDirectoryPool.Return(fsd)
}
