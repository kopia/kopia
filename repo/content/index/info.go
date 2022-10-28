package index

import (
	"fmt"
	"time"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/compression"
)

// Info is an information about a single piece of content managed by Manager.
//
//nolint:interfacebloat
type Info interface {
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

// InfoStruct is an implementation of Info based on a structure.
type InfoStruct struct {
	ContentID           ID                   `json:"contentID"`
	PackBlobID          blob.ID              `json:"packFile,omitempty"`
	TimestampSeconds    int64                `json:"time"`
	OriginalLength      uint32               `json:"originalLength"`
	PackedLength        uint32               `json:"length"`
	PackOffset          uint32               `json:"packOffset,omitempty"`
	Deleted             bool                 `json:"deleted"`
	FormatVersion       byte                 `json:"formatVersion"`
	CompressionHeaderID compression.HeaderID `json:"compression,omitempty"`
	EncryptionKeyID     byte                 `json:"encryptionKeyID,omitempty"`
}

// GetContentID implements the Info interface.
func (i *InfoStruct) GetContentID() ID { return i.ContentID }

// GetPackBlobID implements the Info interface.
func (i *InfoStruct) GetPackBlobID() blob.ID { return i.PackBlobID }

// GetTimestampSeconds implements the Info interface.
func (i *InfoStruct) GetTimestampSeconds() int64 { return i.TimestampSeconds }

// GetOriginalLength implements the Info interface.
func (i *InfoStruct) GetOriginalLength() uint32 { return i.OriginalLength }

// GetPackedLength implements the Info interface.
func (i *InfoStruct) GetPackedLength() uint32 { return i.PackedLength }

// GetPackOffset implements the Info interface.
func (i *InfoStruct) GetPackOffset() uint32 { return i.PackOffset }

// GetDeleted implements the Info interface.
func (i *InfoStruct) GetDeleted() bool { return i.Deleted }

// GetFormatVersion implements the Info interface.
func (i *InfoStruct) GetFormatVersion() byte { return i.FormatVersion }

// GetCompressionHeaderID implements the Info interface.
func (i *InfoStruct) GetCompressionHeaderID() compression.HeaderID { return i.CompressionHeaderID }

// GetEncryptionKeyID implements the Info interface.
func (i *InfoStruct) GetEncryptionKeyID() byte { return i.EncryptionKeyID }

// Timestamp implements the Info interface.
func (i *InfoStruct) Timestamp() time.Time {
	return time.Unix(i.GetTimestampSeconds(), 0)
}

// ToInfoStruct converts the provided Info to *InfoStruct.
func ToInfoStruct(i Info) *InfoStruct {
	if is, ok := i.(*InfoStruct); ok {
		return is
	}

	return &InfoStruct{
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

func DiffInfo(i0, i1 Info) []string {
	var qs []string
	if i0.GetFormatVersion() != i1.GetFormatVersion() {
		qs = append(qs, fmt.Sprintf("mixed FormatVersions: %v %v", i0.GetFormatVersion(), i1.GetFormatVersion()))
	}
	if i0.GetOriginalLength() != i1.GetOriginalLength() {
		qs = append(qs, fmt.Sprintf("mixed OriginalLengths: %v %v", i0.GetOriginalLength(), i1.GetOriginalLength()))
	}
	if i0.GetPackBlobID() != i1.GetPackBlobID() {
		qs = append(qs, fmt.Sprintf("mixed PackBlobIDs: %v %v", i0.GetPackBlobID(), i1.GetPackBlobID()))
	}
	if i0.GetPackedLength() != i1.GetPackedLength() {
		qs = append(qs, fmt.Sprintf("mixed PackedLengths: %v %v", i0.GetPackedLength(), i1.GetPackedLength()))
	}
	if i0.GetPackOffset() != i1.GetPackOffset() {
		qs = append(qs, fmt.Sprintf("mixed PackOffsets: %v %v", i0.GetPackOffset(), i1.GetPackOffset()))
	}
	if i0.GetEncryptionKeyID() != i1.GetEncryptionKeyID() {
		qs = append(qs, fmt.Sprintf("mixed EncryptionKeyIDs: %v %v", i0.GetEncryptionKeyID(), i1.GetEncryptionKeyID()))
	}
	if i0.GetDeleted() != i1.GetDeleted() {
		qs = append(qs, fmt.Sprintf("mixed Deleted flags: %v %v", i0.GetDeleted(), i1.GetDeleted()))
	}
	if i0.GetTimestampSeconds() != i1.GetTimestampSeconds() {
		qs = append(qs, fmt.Sprintf("mixed TimestampSeconds: %v %v", i0.GetTimestampSeconds(), i1.GetTimestampSeconds()))
	}
	return qs
}
