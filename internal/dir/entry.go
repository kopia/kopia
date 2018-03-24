package dir

import (
	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/object"
)

// Entry represents a directory entry as stored in JSON stream.
type Entry struct {
	fs.EntryMetadata
	ObjectID object.ID `json:"obj,omitempty"`
}

// Summary represents a summary of directory stored in JSON stream.
type Summary struct {
	TotalFileSize  int64 `json:"totalFileSize"`
	TotalFileCount int64 `json:"totalFileCount"`
}
