package dir

import (
	"time"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/object"
)

// Entry represents a directory entry as stored in JSON stream.
type Entry struct {
	fs.EntryMetadata
	ObjectID   object.ID `json:"obj,omitempty"`
	DirSummary *Summary  `json:"summ,omitempty"`
}

// Summary represents a summary of directory stored in JSON stream.
type Summary struct {
	TotalFileSize  int64     `json:"size"`
	TotalFileCount int64     `json:"files"`
	TotalDirCount  int64     `json:"dirs"`
	MaxModTime     time.Time `json:"maxTime"`
}
