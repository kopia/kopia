package block

import (
	"time"

	"github.com/kopia/kopia/repo/blob"
)

// Info is an information about a single block managed by Manager.
type Info struct {
	BlockID          string  `json:"blockID"`
	Length           uint32  `json:"length"`
	TimestampSeconds int64   `json:"time"`
	PackBlobID       blob.ID `json:"packFile,omitempty"`
	PackOffset       uint32  `json:"packOffset,omitempty"`
	Deleted          bool    `json:"deleted"`
	Payload          []byte  `json:"payload"` // set for payloads stored inline
	FormatVersion    byte    `json:"formatVersion"`
}

// Timestamp returns the time when a block was created or deleted.
func (i Info) Timestamp() time.Time {
	return time.Unix(i.TimestampSeconds, 0)
}
