package index

import (
	"time"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/compression"
)

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

// InfoCompact is a memory compact version of Info in order to save memory usage
type InfoCompact struct {
	PackBlobID          *blob.ID
	TimestampSeconds    int64
	OriginalLength      uint32
	PackedLength        uint32
	PackOffset          uint32
	CompressionHeaderID compression.HeaderID
	ContentID           ID
	Deleted             bool
	FormatVersion       byte
	EncryptionKeyID     byte
}

// BuilderItem is an generic type for Info and InfoCompact
type BuilderItem interface {
	GetPackBlobID() blob.ID
	GetContentID() ID
	GetTimestampSeconds() int64
	GetOriginalLength() uint32
	GetPackedLength() uint32
	GetPackOffset() uint32
	GetCompressionHeaderID() compression.HeaderID
	IsDeleted() bool
	GetFormatVersion() byte
	GetEncryptionKeyID() byte
	Timestamp() time.Time
}

// Timestamp implements BuilderItem and returns a Time from TimestampSeconds of Info
func (i Info) Timestamp() time.Time {
	return time.Unix(i.TimestampSeconds, 0)
}

// GetPackBlobID implements BuilderItem and returns PackBlobID of Info
func (i Info) GetPackBlobID() blob.ID {
	return i.PackBlobID
}

// GetContentID implements BuilderItem and returns ContentID of Info
func (i Info) GetContentID() ID {
	return i.ContentID
}

// GetTimestampSeconds implements BuilderItem and returns TimestampSeconds of Info
func (i Info) GetTimestampSeconds() int64 {
	return i.TimestampSeconds
}

// GetOriginalLength implements BuilderItem and returns OriginalLength of Info
func (i Info) GetOriginalLength() uint32 {
	return i.OriginalLength
}

// GetPackedLength implements BuilderItem and returns PackedLength of Info
func (i Info) GetPackedLength() uint32 {
	return i.PackedLength
}

// GetPackOffset implements BuilderItem and returns PackOffset of Info
func (i Info) GetPackOffset() uint32 {
	return i.PackOffset
}

// GetCompressionHeaderID implements BuilderItem and returns CompressionHeaderID of Info
func (i Info) GetCompressionHeaderID() compression.HeaderID {
	return i.CompressionHeaderID
}

// IsDeleted implements BuilderItem and returns Deleted of Info
func (i Info) IsDeleted() bool {
	return i.Deleted
}

// GetFormatVersion implements BuilderItem and returns FormatVersion of Info
func (i Info) GetFormatVersion() byte {
	return i.FormatVersion
}

// GetEncryptionKeyID implements BuilderItem and returns EncryptionKeyID of Info
func (i Info) GetEncryptionKeyID() byte {
	return i.EncryptionKeyID
}

// Timestamp implements BuilderItem and returns a Time from TimestampSeconds of InfoCompact
func (i InfoCompact) Timestamp() time.Time {
	return time.Unix(i.TimestampSeconds, 0)
}

// GetPackBlobID implements BuilderItem and returns PackBlobID of InfoCompact
func (i *InfoCompact) GetPackBlobID() blob.ID {
	return *i.PackBlobID
}

// GetContentID implements BuilderItem and returns ContentID of InfoCompact
func (i *InfoCompact) GetContentID() ID {
	return i.ContentID
}

// GetTimestampSeconds implements BuilderItem and returns TimestampSeconds of InfoCompact
func (i *InfoCompact) GetTimestampSeconds() int64 {
	return i.TimestampSeconds
}

// GetOriginalLength implements BuilderItem and returns OriginalLength of InfoCompact
func (i *InfoCompact) GetOriginalLength() uint32 {
	return i.OriginalLength
}

// GetPackedLength implements BuilderItem and returns PackedLength of InfoCompact
func (i *InfoCompact) GetPackedLength() uint32 {
	return i.PackedLength
}

// GetPackOffset implements BuilderItem and returns PackOffset of InfoCompact
func (i InfoCompact) GetPackOffset() uint32 {
	return i.PackOffset
}

// GetCompressionHeaderID implements BuilderItem and returns CompressionHeaderID of InfoCompact
func (i *InfoCompact) GetCompressionHeaderID() compression.HeaderID {
	return i.CompressionHeaderID
}

// IsDeleted implements BuilderItem and returns Deleted of InfoCompact
func (i *InfoCompact) IsDeleted() bool {
	return i.Deleted
}

// GetFormatVersion implements BuilderItem and returns FormatVersion of InfoCompact
func (i *InfoCompact) GetFormatVersion() byte {
	return i.FormatVersion
}

// GetEncryptionKeyID implements BuilderItem and returns EncryptionKeyID of InfoCompact
func (i *InfoCompact) GetEncryptionKeyID() byte {
	return i.EncryptionKeyID
}
