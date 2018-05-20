package dir

import (
	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/object"
)

// Entry represents a directory entry as stored in JSON stream.
type Entry struct {
	fs.EntryMetadata
	ObjectID   object.ID            `json:"obj,omitempty"`
	DirSummary *fs.DirectorySummary `json:"summ,omitempty"`
}
