package restore

import (
	"sync"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
)

// hardLinkKey identifies a file on the source filesystem.
type hardLinkKey struct {
	dev uint64
	ino uint64
}

// hardLinkAnchor is the first restored member of a hardlink group. Other
// members of the group are linked to it once it has been fully written.
type hardLinkAnchor struct {
	path     string
	objectID object.ID
	done     chan struct{}
	err      error
}

// hardLinkRegistry tracks the first restored path for each hardlink group so
// that subsequent members can be recreated as links instead of copies.
// It is safe for concurrent use.
type hardLinkRegistry struct {
	mu      sync.Mutex
	anchors map[hardLinkKey]*hardLinkAnchor
}

func newHardLinkRegistry() *hardLinkRegistry {
	return &hardLinkRegistry{anchors: map[hardLinkKey]*hardLinkAnchor{}}
}

// hardLinkKeyOf returns the hardlink identity of an entry and whether
// the entry is part of a hardlink group.
func hardLinkKeyOf(e fs.Entry) (hardLinkKey, bool) {
	if _, isDir := e.(fs.Directory); isDir {
		return hardLinkKey{}, false
	}

	hli := e.HardLinkInfo()
	if hli.UniqID == 0 || hli.NLink <= 1 {
		return hardLinkKey{}, false
	}

	return hardLinkKey{dev: e.Device().Dev, ino: hli.UniqID}, true
}

func objectIDOf(e fs.Entry) object.ID {
	if h, ok := e.(object.HasObjectID); ok {
		return h.ObjectID()
	}

	if h, ok := e.(snapshot.HasDirEntry); ok {
		return h.DirEntry().ObjectID
	}

	return object.EmptyID
}

// claim registers path as the anchor for key if none exists yet.
// It returns the anchor and whether the caller became its owner, in which case
// the caller must invoke anchor.finish once the file is written.
func (r *hardLinkRegistry) claim(key hardLinkKey, path string, oid object.ID) (*hardLinkAnchor, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if a, ok := r.anchors[key]; ok {
		return a, false
	}

	a := &hardLinkAnchor{path: path, objectID: oid, done: make(chan struct{})}
	r.anchors[key] = a

	return a, true
}

// release removes a claimed anchor whose write never completed so that another
// member of the group can become the anchor.
func (r *hardLinkRegistry) release(key hardLinkKey, a *hardLinkAnchor) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.anchors[key] == a {
		delete(r.anchors, key)
	}
}

// finish marks the anchor as written (or failed) and wakes any waiters.
func (a *hardLinkAnchor) finish(err error) {
	a.err = err
	close(a.done)
}

// wait blocks until the anchor is finished and returns its error.
func (a *hardLinkAnchor) wait() error {
	<-a.done
	return a.err
}

// linkable reports whether a file with the given object ID may be linked to
// the anchor: it must have identical content, which guards against the source
// filesystem reusing inode numbers for unrelated files.
func (a *hardLinkAnchor) linkable(oid object.ID) bool {
	return a.objectID != object.EmptyID && a.objectID == oid
}
