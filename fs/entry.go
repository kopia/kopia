package fs

import (
	"os"

	"github.com/kopia/kopia/cas"
)

// Entry stores attributes of a single entry in a directory.
type Entry interface {
	os.FileInfo

	UserID() uint32
	GroupID() uint32
	ObjectID() cas.ObjectID
}

func metadataEquals(e1, e2 Entry) bool {
	if (e1 != nil) != (e2 != nil) {
		return false
	}

	if e1.Mode() != e2.Mode() {
		return false
	}

	if e1.ModTime() != e2.ModTime() {
		return false
	}

	if e1.Size() != e2.Size() {
		return false
	}

	if e1.Name() != e2.Name() {
		return false
	}

	if e1.UserID() != e2.UserID() {
		return false
	}

	if e1.GroupID() != e2.GroupID() {
		return false
	}

	return true
}

type entryWithObjectID struct {
	Entry
	oid cas.ObjectID
}

func (e *entryWithObjectID) ObjectID() cas.ObjectID {
	return e.oid
}
