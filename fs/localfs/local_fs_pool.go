package localfs

import "github.com/kopia/kopia/internal/freepool"

//nolint:gochecknoglobals
var (
	filesystemFilePool = freepool.New(
		func() interface{} { return &filesystemFile{} },
		func(v interface{}) {
			//nolint:forcetypeassert
			*v.(*filesystemFile) = filesystemFile{}
		},
	)
	filesystemDirectoryPool = freepool.New(
		func() interface{} { return &filesystemDirectory{} },
		func(v interface{}) {
			//nolint:forcetypeassert
			*v.(*filesystemDirectory) = filesystemDirectory{}
		},
	)
	filesystemSymlinkPool = freepool.New(
		func() interface{} { return &filesystemSymlink{} },
		func(v interface{}) {
			//nolint:forcetypeassert
			*v.(*filesystemSymlink) = filesystemSymlink{}
		},
	)
	filesystemErrorEntryPool = freepool.New(
		func() interface{} { return &filesystemErrorEntry{} },
		func(v interface{}) {
			//nolint:forcetypeassert
			*v.(*filesystemErrorEntry) = filesystemErrorEntry{}
		},
	)
	shallowFilesystemFilePool = freepool.New(
		func() interface{} { return &shallowFilesystemFile{} },
		func(v interface{}) {
			//nolint:forcetypeassert
			*v.(*shallowFilesystemFile) = shallowFilesystemFile{}
		},
	)
	shallowFilesystemDirectoryPool = freepool.New(
		func() interface{} { return &shallowFilesystemDirectory{} },
		func(v interface{}) {
			//nolint:forcetypeassert
			*v.(*shallowFilesystemDirectory) = shallowFilesystemDirectory{}
		},
	)
)

func newFilesystemFile(e filesystemEntry) *filesystemFile {
	//nolint:forcetypeassert
	fsf := filesystemFilePool.Take().(*filesystemFile)
	fsf.filesystemEntry = e

	return fsf
}

func (fsf *filesystemFile) Close() {
	filesystemFilePool.Return(fsf)
}

func newFilesystemDirectory(e filesystemEntry) *filesystemDirectory {
	//nolint:forcetypeassert
	fsd := filesystemDirectoryPool.Take().(*filesystemDirectory)
	fsd.filesystemEntry = e

	return fsd
}

func (fsd *filesystemDirectory) Close() {
	filesystemDirectoryPool.Return(fsd)
}

func newFilesystemSymlink(e filesystemEntry) *filesystemSymlink {
	//nolint:forcetypeassert
	fsd := filesystemSymlinkPool.Take().(*filesystemSymlink)
	fsd.filesystemEntry = e

	return fsd
}

func (fsl *filesystemSymlink) Close() {
	filesystemSymlinkPool.Return(fsl)
}

func newFilesystemErrorEntry(e filesystemEntry, err error) *filesystemErrorEntry {
	//nolint:forcetypeassert
	fse := filesystemErrorEntryPool.Take().(*filesystemErrorEntry)
	fse.filesystemEntry = e
	fse.err = err

	return fse
}

func (e *filesystemErrorEntry) Close() {
	filesystemErrorEntryPool.Return(e)
}

func newShallowFilesystemFile(e filesystemEntry) *shallowFilesystemFile {
	//nolint:forcetypeassert
	fsf := shallowFilesystemFilePool.Take().(*shallowFilesystemFile)
	fsf.filesystemEntry = e

	return fsf
}

func (fsf *shallowFilesystemFile) Close() {
	shallowFilesystemFilePool.Return(fsf)
}

func newShallowFilesystemDirectory(e filesystemEntry) *shallowFilesystemDirectory {
	//nolint:forcetypeassert
	fsf := shallowFilesystemDirectoryPool.Take().(*shallowFilesystemDirectory)
	fsf.filesystemEntry = e

	return fsf
}

func (fsd *shallowFilesystemDirectory) Close() {
	shallowFilesystemDirectoryPool.Return(fsd)
}
