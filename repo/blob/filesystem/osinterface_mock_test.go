package filesystem

import (
	"io/fs"
	"os"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

var errNonRetriable = errors.New("some non-retriable error")

type mockOS struct {
	readFileRemainingErrors             atomic.Int32
	writeFileRemainingErrors            atomic.Int32
	writeFileCloseRemainingErrors       atomic.Int32
	createNewFileRemainingErrors        atomic.Int32
	mkdirAllRemainingErrors             atomic.Int32
	renameRemainingErrors               atomic.Int32
	removeRemainingRetriableErrors      atomic.Int32
	removeRemainingNonRetriableErrors   atomic.Int32
	chownRemainingErrors                atomic.Int32
	readDirRemainingErrors              atomic.Int32
	readDirRemainingNonRetriableErrors  atomic.Int32
	readDirRemainingFileDeletedDirEntry atomic.Int32
	readDirRemainingFatalDirEntry       atomic.Int32
	statRemainingErrors                 atomic.Int32
	chtimesRemainingErrors              atomic.Int32

	effectiveUID int

	// remaining syscall errnos
	//nolint:unused // Used with platform specific code
	eStaleRemainingErrors atomic.Int32

	osInterface
}

func (osi *mockOS) Open(fname string) (osReadFile, error) {
	rf, err := osi.osInterface.Open(fname)
	if err != nil {
		return nil, err
	}

	if osi.readFileRemainingErrors.Add(-1) >= 0 {
		return readFailureFile{rf}, nil
	}

	return rf, nil
}

func (osi *mockOS) Rename(oldname, newname string) error {
	if osi.renameRemainingErrors.Add(-1) >= 0 {
		return &os.LinkError{Op: "rename", Old: oldname, New: newname, Err: errors.New("underlying problem")}
	}

	return osi.osInterface.Rename(oldname, newname)
}

func (osi *mockOS) IsPathSeparator(c byte) bool { return os.IsPathSeparator(c) }

func (osi *mockOS) ReadDir(dirname string) ([]fs.DirEntry, error) {
	if osi.readDirRemainingErrors.Add(-1) >= 0 {
		return nil, &os.PathError{Op: "readdir", Err: errors.New("underlying problem")}
	}

	if osi.readDirRemainingNonRetriableErrors.Add(-1) >= 0 {
		return nil, errNonRetriable
	}

	ent, err := osi.osInterface.ReadDir(dirname)

	if osi.readDirRemainingFileDeletedDirEntry.Add(-1) >= 0 {
		// add a dir entry which will fail at Info() time
		_, noSuchFileErr := os.Open(uuid.NewString())

		ent = append(ent, &mockDirEntryInfoError{nil, noSuchFileErr})
	}

	if osi.readDirRemainingFatalDirEntry.Add(-1) >= 0 {
		ent = append(ent, &mockDirEntryInfoError{nil, errNonRetriable})
	}

	return ent, err
}

func (osi *mockOS) Remove(fname string) error {
	if osi.removeRemainingRetriableErrors.Add(-1) >= 0 {
		return &os.PathError{Op: "unlink", Err: errors.New("underlying problem")}
	}

	if osi.removeRemainingNonRetriableErrors.Add(-1) >= 0 {
		return errNonRetriable
	}

	return osi.osInterface.Remove(fname)
}

func (osi *mockOS) Chtimes(fname string, atime, mtime time.Time) error {
	if osi.chtimesRemainingErrors.Add(-1) >= 0 {
		return &os.PathError{Op: "chtimes", Err: errors.New("underlying problem")}
	}

	return osi.osInterface.Chtimes(fname, atime, mtime)
}

func (osi *mockOS) Chown(fname string, uid, gid int) error {
	if osi.chownRemainingErrors.Add(-1) >= 0 {
		return &os.PathError{Op: "chown", Err: errors.New("underlying problem")}
	}

	return osi.osInterface.Chown(fname, uid, gid)
}

func (osi *mockOS) CreateNewFile(fname string, perm os.FileMode) (osWriteFile, error) {
	if osi.createNewFileRemainingErrors.Add(-1) >= 0 {
		return nil, &os.PathError{Op: "create", Err: errors.New("underlying problem")}
	}

	wf, err := osi.osInterface.CreateNewFile(fname, perm)
	if err != nil {
		return nil, err
	}

	if osi.writeFileRemainingErrors.Add(-1) >= 0 {
		return writeFailureFile{wf}, nil
	}

	if osi.writeFileCloseRemainingErrors.Add(-1) >= 0 {
		return writeCloseFailureFile{wf}, nil
	}

	return wf, nil
}

func (osi *mockOS) Mkdir(fname string, mode os.FileMode) error {
	if osi.mkdirAllRemainingErrors.Add(-1) >= 0 {
		return &os.PathError{Op: "mkdir", Err: errors.New("underlying problem")}
	}

	return osi.osInterface.Mkdir(fname, mode)
}

func (osi *mockOS) Geteuid() int {
	return osi.effectiveUID
}

type readFailureFile struct {
	osReadFile
}

func (f readFailureFile) Read(b []byte) (int, error) {
	return 0, &os.PathError{Op: "read", Err: errors.New("underlying problem")}
}

type writeFailureFile struct {
	osWriteFile
}

func (f writeFailureFile) Write(b []byte) (int, error) {
	return 0, &os.PathError{Op: "write", Err: errors.New("underlying problem")}
}

type writeCloseFailureFile struct {
	osWriteFile
}

func (f writeCloseFailureFile) Close() error {
	return &os.PathError{Op: "close", Err: errors.New("underlying problem")}
}

type mockDirEntryInfoError struct {
	fs.DirEntry

	err error
}

func (e mockDirEntryInfoError) Info() (fs.FileInfo, error) {
	return nil, e.err
}
