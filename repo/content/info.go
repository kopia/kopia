package content

import (
	"time"

	"github.com/kopia/kopia/repo/blob"
)

// ID is an identifier of content in content-addressable storage.
type ID string

// Prefix returns a one-character prefix of a content ID or an empty string.
func (i ID) Prefix() ID {
	if i.HasPrefix() {
		return i[0:1]
	}

	return ""
}

// HasPrefix determines if the given ID has a non-empty prefix.
func (i ID) HasPrefix() bool {
	return len(i)%2 == 1
}

// Info is an information about a single piece of content managed by Manager.
type Info struct {
	ID               ID      `json:"contentID"`
	Length           uint32  `json:"length"`
	TimestampSeconds int64   `json:"time"`
	PackBlobID       blob.ID `json:"packFile,omitempty"`
	PackOffset       uint32  `json:"packOffset,omitempty"`
	Deleted          bool    `json:"deleted"`
	FormatVersion    byte    `json:"formatVersion"`
}

// Timestamp returns the time when a content was created or deleted.
func (i *Info) Timestamp() time.Time {
	return time.Unix(i.TimestampSeconds, 0)
}
