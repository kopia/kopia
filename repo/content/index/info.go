package index

import (
	"time"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/compression"
)

// Info is an implementation of Info based on a structure.
//
//nolint:recvcheck
type Info struct {
	PackBlobID          blob.ID              `json:"packFile,omitempty"`
	TimestampSeconds    int64                `json:"time"`
	OriginalLength      uint32               `json:"originalLength"`
	PackedLength        uint32               `json:"length"`
	PackOffset          uint32               `json:"packOffset,omitempty"`
	CompressionHeaderID compression.HeaderID `json:"compression,omitempty"`
	ContentID           ID                   `json:"contentID"`
	Deleted             bool                 `json:"deleted"`
	FormatVersion       byte                 `json:"formatVersion"`
	EncryptionKeyID     byte                 `json:"encryptionKeyID,omitempty"`
}

// Timestamp implements the Info interface.
func (i Info) Timestamp() time.Time {
	return time.Unix(i.TimestampSeconds, 0)
}
