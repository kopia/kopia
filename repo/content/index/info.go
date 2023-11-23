package index

import (
	"time"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/compression"
)

// InfoReader is an information about a single piece of content managed by Manager.
//
//nolint:interfacebloat
type InfoReader interface {
	GetContentID() ID
	GetPackBlobID() blob.ID
	GetTimestampSeconds() int64
	Timestamp() time.Time
	GetOriginalLength() uint32
	GetPackedLength() uint32
	GetPackOffset() uint32
	GetDeleted() bool
	GetFormatVersion() byte
	GetCompressionHeaderID() compression.HeaderID
	GetEncryptionKeyID() byte
}

// Info is an implementation of Info based on a structure.
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

// GetContentID implements the Info interface.
func (i Info) GetContentID() ID { return i.ContentID }

// GetPackBlobID implements the Info interface.
func (i Info) GetPackBlobID() blob.ID { return i.PackBlobID }

// GetTimestampSeconds implements the Info interface.
func (i Info) GetTimestampSeconds() int64 { return i.TimestampSeconds }

// GetOriginalLength implements the Info interface.
func (i Info) GetOriginalLength() uint32 { return i.OriginalLength }

// GetPackedLength implements the Info interface.
func (i Info) GetPackedLength() uint32 { return i.PackedLength }

// GetPackOffset implements the Info interface.
func (i Info) GetPackOffset() uint32 { return i.PackOffset }

// GetDeleted implements the Info interface.
func (i Info) GetDeleted() bool { return i.Deleted }

// GetFormatVersion implements the Info interface.
func (i Info) GetFormatVersion() byte { return i.FormatVersion }

// GetCompressionHeaderID implements the Info interface.
func (i Info) GetCompressionHeaderID() compression.HeaderID { return i.CompressionHeaderID }

// GetEncryptionKeyID implements the Info interface.
func (i Info) GetEncryptionKeyID() byte { return i.EncryptionKeyID }

// Timestamp implements the Info interface.
func (i Info) Timestamp() time.Time {
	return time.Unix(i.GetTimestampSeconds(), 0)
}

// ToInfoStruct converts the provided Info to InfoStruct.
func ToInfoStruct(i InfoReader) Info {
	return Info{
		ContentID:           i.GetContentID(),
		PackBlobID:          i.GetPackBlobID(),
		TimestampSeconds:    i.GetTimestampSeconds(),
		OriginalLength:      i.GetOriginalLength(),
		PackedLength:        i.GetPackedLength(),
		PackOffset:          i.GetPackOffset(),
		Deleted:             i.GetDeleted(),
		FormatVersion:       i.GetFormatVersion(),
		CompressionHeaderID: i.GetCompressionHeaderID(),
		EncryptionKeyID:     i.GetEncryptionKeyID(),
	}
}
