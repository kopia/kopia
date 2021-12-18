package filesystem

import (
	"io/fs"
	"os"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

var errNonRetriable = errors.Errorf("some non-retriable error")

type mockOS struct {
	readFileRemainingErrors             int32
	writeFileRemainingErrors            int32
	writeFileCloseRemainingErrors       int32
	createNewFileRemainingErrors        int32
	mkdirAllRemainingErrors             int32
	renameRemainingErrors               int32
	removeRemainingRetriableErrors      int32
	removeRemainingNonRetriableErrors   int32
	chownRemainingErrors                int32
	readDirRemainingErrors              int32
	readDirRemainingNonRetriableErrors  int32
	readDirRemainingFileDeletedDirEntry int32
	readDirRemainingFatalDirEntry       int32
	statRemainingErrors                 int32
	chtimesRemainingErrors              int32

	effectiveUID int

	osInterface
}

func (osi *mockOS) Open(fname string) (osReadFile, error) {
	rf, err := osi.osInterface.Open(fname)
	if err != nil {
		return nil, err
	}

	if atomic.AddInt32(&osi.readFileRemainingErrors, -1) >= 0 {
		return readFailureFile{rf}, nil
	}

	return rf, nil
}

func (osi *mockOS) Rename(oldname, newname string) error {
	if atomic.AddInt32(&osi.renameRemainingErrors, -1) >= 0 {
		return &os.LinkError{Op: "rename", Old: oldname, New: newname, Err: errors.Errorf("underlying problem")}
	}

	return osi.osInterface.Rename(oldname, newname)
}

func (osi *mockOS) ReadDir(dirname string) ([]fs.DirEntry, error) {
	if atomic.AddInt32(&osi.readDirRemainingErrors, -1) >= 0 {
		return nil, &os.PathError{Op: "readdir", Err: errors.Errorf("underlying problem")}
	}

	if atomic.AddInt32(&osi.readDirRemainingNonRetriableErrors, -1) >= 0 {
		return nil, errNonRetriable
	}

	ent, err := osi.osInterface.ReadDir(dirname)

	if atomic.AddInt32(&osi.readDirRemainingFileDeletedDirEntry, -1) >= 0 {
		// add a dir entry which will fail at Info() time
		_, noSuchFileErr := os.Open(uuid.NewString())

		ent = append(ent, &mockDirEntryInfoError{nil, noSuchFileErr})
	}

	if atomic.AddInt32(&osi.readDirRemainingFatalDirEntry, -1) >= 0 {
		ent = append(ent, &mockDirEntryInfoError{nil, errNonRetriable})
	}

	return ent, err
}

func (osi *mockOS) Remove(fname string) error {
	if atomic.AddInt32(&osi.removeRemainingRetriableErrors, -1) >= 0 {
		return &os.PathError{Op: "unlink", Err: errors.Errorf("underlying problem")}
	}

	if atomic.AddInt32(&osi.removeRemainingNonRetriableErrors, -1) >= 0 {
		return errNonRetriable
	}

	return osi.osInterface.Remove(fname)
}

func (osi *mockOS) Stat(fname string) (fs.FileInfo, error) {
	if atomic.AddInt32(&osi.statRemainingErrors, -1) >= 0 {
		return nil, &os.PathError{Op: "stat", Err: errors.Errorf("underlying problem")}
	}

	return osi.osInterface.Stat(fname)
}

func (osi *mockOS) Chtimes(fname string, atime, mtime time.Time) error {
	if atomic.AddInt32(&osi.chtimesRemainingErrors, -1) >= 0 {
		return &os.PathError{Op: "chtimes", Err: errors.Errorf("underlying problem")}
	}

	return osi.osInterface.Chtimes(fname, atime, mtime)
}

func (osi *mockOS) Chown(fname string, uid, gid int) error {
	if atomic.AddInt32(&osi.chownRemainingErrors, -1) >= 0 {
		return &os.PathError{Op: "chown", Err: errors.Errorf("underlying problem")}
	}

	return osi.osInterface.Chown(fname, uid, gid)
}

func (osi *mockOS) CreateNewFile(fname string, perm os.FileMode) (osWriteFile, error) {
	if atomic.AddInt32(&osi.createNewFileRemainingErrors, -1) >= 0 {
		return nil, &os.PathError{Op: "create", Err: errors.Errorf("underlying problem")}
	}

	wf, err := osi.osInterface.CreateNewFile(fname, perm)
	if err != nil {
		return nil, err
	}

	if atomic.AddInt32(&osi.writeFileRemainingErrors, -1) >= 0 {
		return writeFailureFile{wf}, nil
	}

	if atomic.AddInt32(&osi.writeFileCloseRemainingErrors, -1) >= 0 {
		return writeCloseFailureFile{wf}, nil
	}

	return wf, nil
}

func (osi *mockOS) MkdirAll(fname string, mode os.FileMode) error {
	if atomic.AddInt32(&osi.mkdirAllRemainingErrors, -1) >= 0 {
		return &os.PathError{Op: "mkdir", Err: errors.Errorf("underlying problem")}
	}

	return osi.osInterface.MkdirAll(fname, mode)
}

func (osi *mockOS) Geteuid() int {
	return osi.effectiveUID
}

type readFailureFile struct {
	osReadFile
}

func (f readFailureFile) Read(b []byte) (int, error) {
	return 0, &os.PathError{Op: "read", Err: errors.Errorf("underlying problem")}
}

type writeFailureFile struct {
	osWriteFile
}

func (f writeFailureFile) Write(b []byte) (int, error) {
	return 0, &os.PathError{Op: "write", Err: errors.Errorf("underlying problem")}
}

type writeCloseFailureFile struct {
	osWriteFile
}

func (f writeCloseFailureFile) Close() error {
	return &os.PathError{Op: "close", Err: errors.Errorf("underlying problem")}
}

type mockDirEntryInfoError struct {
	fs.DirEntry

	err error
}

func (e mockDirEntryInfoError) Info() (fs.FileInfo, error) {
	return nil, e.err
}
