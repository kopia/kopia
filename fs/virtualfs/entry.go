package virtualfs

import (
	"os"
	"time"

	"github.com/kopia/kopia/fs"
)

// entry is an in-memory implementation of a directory entry.
type entry struct {
	name    string
	mode    os.FileMode
	size    int64
	modTime time.Time
	owner   fs.OwnerInfo
	device  fs.DeviceInfo
}

var _ fs.Entry = (*entry)(nil)

func (e *entry) Name() string {
	return e.name
}

func (e *entry) IsDir() bool {
	return e.mode.IsDir()
}

func (e *entry) Mode() os.FileMode {
	return e.mode
}

func (e *entry) ModTime() time.Time {
	return e.modTime
}

func (e *entry) Size() int64 {
	return e.size
}

func (e *entry) Sys() interface{} {
	return nil
}

func (e *entry) Owner() fs.OwnerInfo {
	return e.owner
}

func (e *entry) Device() fs.DeviceInfo {
	return e.device
}

func (e *entry) LocalFilesystemPath() string {
	return ""
}
