package upload

import (
	"sync"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/snapshot"
)

// hardLinkKey identifies a file on the source filesystem.
type hardLinkKey struct {
	dev uint64
	ino uint64
}

// hardLinkUpload is the first member of a hardlink group to be uploaded.
// Other members wait for it and reuse its object instead of hashing the
// same content again.
type hardLinkUpload struct {
	done  chan struct{}
	entry *snapshot.DirEntry
	err   error
}

// hardLinkUploadRegistry tracks uploads of hardlink groups within a single
// Upload call. It is safe for concurrent use.
type hardLinkUploadRegistry struct {
	mu      sync.Mutex
	uploads map[hardLinkKey]*hardLinkUpload
}

func newHardLinkUploadRegistry() *hardLinkUploadRegistry {
	return &hardLinkUploadRegistry{uploads: map[hardLinkKey]*hardLinkUpload{}}
}

// hardLinkKeyOf returns the hardlink identity of a file and whether it is
// part of a hardlink group.
func hardLinkKeyOf(f fs.File) (hardLinkKey, bool) {
	hli := f.HardLinkInfo()
	if hli.UniqID == 0 || hli.NLink <= 1 {
		return hardLinkKey{}, false
	}

	return hardLinkKey{dev: f.Device().Dev, ino: hli.UniqID}, true
}

// claim registers the caller as the uploader of key if nobody has yet.
// When owner is true, the caller must invoke finish once the upload completes.
func (r *hardLinkUploadRegistry) claim(key hardLinkKey) (u *hardLinkUpload, owner bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if u, ok := r.uploads[key]; ok {
		return u, false
	}

	u = &hardLinkUpload{done: make(chan struct{})}
	r.uploads[key] = u

	return u, true
}

// release removes a claimed upload that failed so that another member of the
// group may attempt the upload.
func (r *hardLinkUploadRegistry) release(key hardLinkKey, u *hardLinkUpload) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.uploads[key] == u {
		delete(r.uploads, key)
	}
}

// finish publishes the result of the upload and wakes any waiters.
func (u *hardLinkUpload) finish(de *snapshot.DirEntry, err error) {
	u.entry = de
	u.err = err
	close(u.done)
}

// wait blocks until the upload is finished and returns the resulting entry,
// or nil if the upload failed.
func (u *hardLinkUpload) wait() *snapshot.DirEntry {
	<-u.done

	if u.err != nil {
		return nil
	}

	return u.entry
}

// reusableFor reports whether the uploaded entry can stand in for f: the two
// must agree on size, mode and modification time, which guards against inode
// reuse on a source filesystem that is changing during the snapshot.
func (u *hardLinkUpload) reusableFor(f fs.File) bool {
	de := u.entry
	if de == nil {
		return false
	}

	return de.FileSize == f.Size() &&
		de.Permissions == snapshot.Permissions(f.Mode()&fs.ModBits) &&
		de.ModTime == fs.UTCTimestampFromTime(f.ModTime())
}
