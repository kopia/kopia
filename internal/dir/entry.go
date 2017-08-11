package dir

import (
	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo"
)

// Entry represents a directory entry as stored in JSON stream.
type Entry struct {
	fs.EntryMetadata
	ObjectID repo.ObjectID `json:"obj,omitempty"`
}
