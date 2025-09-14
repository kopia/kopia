package indexblob

import (
	"github.com/kopia/kopia/internal/contentlog"
	"github.com/kopia/kopia/repo/blob"
)

type writeIndexBlobLogEntry struct {
	BlobID             blob.ID
	BlobLength         int
	BlobTimestampNanos int64
	LatencyNanos       int64
	Error              error
}

func (e writeIndexBlobLogEntry) WriteTo(jw *contentlog.JSONWriter) {
	jw.StringField("blobID", string(e.BlobID))
	jw.IntField("blobLength", e.BlobLength)
	jw.Int64Field("blobTimestampNanos", e.BlobTimestampNanos)
	jw.Int64Field("latencyNanos", e.LatencyNanos)
	jw.ErrorField("error", e.Error)
}
