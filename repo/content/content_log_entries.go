package content

import (
	"github.com/kopia/kopia/internal/blobparam"
	"github.com/kopia/kopia/internal/contentlog"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content/index"
)

type flushLogEntry struct {
	LatencyNanos int64
	Error        error
}

func (e flushLogEntry) WriteTo(jw *contentlog.JSONWriter) {
	jw.Int64Field("latencyNanos", e.LatencyNanos)
	jw.ErrorField("error", e.Error)
}

type writeIndexBlobLogEntry struct {
	BlobID             blob.ID
	BlobLength         int
	BlobTimestampNanos int64
	LatencyNanos       int64
	Error              error
}

func (e writeIndexBlobLogEntry) WriteTo(jw *contentlog.JSONWriter) {
	blobparam.BlobID("blobID", e.BlobID).WriteValueTo(jw)
	jw.IntField("blobLength", e.BlobLength)
	jw.Int64Field("blobTimestampNanos", e.BlobTimestampNanos)
	jw.Int64Field("latencyNanos", e.LatencyNanos)
	jw.ErrorField("error", e.Error)
}

type writePackBlobLogEntry struct {
	BlobID       blob.ID
	BlobLength   int
	LatencyNanos int64
}

func (e writePackBlobLogEntry) WriteTo(jw *contentlog.JSONWriter) {
	blobparam.BlobID("blobID", e.BlobID).WriteValueTo(jw)
	jw.IntField("blobLength", e.BlobLength)
	jw.Int64Field("latencyNanos", e.LatencyNanos)
}

type addToPackLogEntry struct {
	PendingPackBlobID     blob.ID
	BlobLength            int
	ContentID             ID
	OriginalContentLength uint32
	PackedContentLength   uint32
	OriginalPackBlobID    blob.ID
	IsDeleted             bool
}

func (e addToPackLogEntry) WriteTo(jw *contentlog.JSONWriter) {
	jw.StringField("m", "add-to-pack")
	blobparam.BlobID("pendingPackBlobID", e.PendingPackBlobID).WriteValueTo(jw)
	jw.IntField("blobLength", e.BlobLength)
	index.ContentIDParam("contentID", e.ContentID).WriteValueTo(jw)
	jw.UInt32Field("originalContentLength", e.OriginalContentLength)
	jw.UInt32Field("packedContentLength", e.PackedContentLength)
	blobparam.BlobID("originalPackBlobID", e.OriginalPackBlobID).WriteValueTo(jw)
	jw.BoolField("isDeleted", e.IsDeleted)
}

type refreshLogEntry struct {
	LatencyNanos int64
	Error        error
}

func (e refreshLogEntry) WriteTo(jw *contentlog.JSONWriter) {
	jw.Int64Field("latencyNanos", e.LatencyNanos)
	jw.ErrorField("error", e.Error)
}
