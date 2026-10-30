// Package blobparam provides parameters for logging blob-specific operations.
package blobparam

import (
	"github.com/kopia/kopia/internal/contentlog"
	"github.com/kopia/kopia/repo/blob"
)

type blobMetadataListParam struct {
	key  string
	list []blob.Metadata
}

func (v blobMetadataListParam) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginListField(v.key)

	for _, bm := range v.list {
		jw.BeginObject()
		jw.StringField("blobID", string(bm.BlobID))
		jw.Int64Field("l", bm.Length)
		jw.TimeField("ts", bm.Timestamp)
		jw.EndObject()
	}

	jw.EndList()
}

// BlobMetadataList creates a parameter for a list of blob metadata.
//
//nolint:revive
func BlobMetadataList(name string, list []blob.Metadata) blobMetadataListParam {
	return blobMetadataListParam{key: name, list: list}
}

type blobIDParam struct {
	key string
	val blob.ID
}

func (v blobIDParam) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.StringField(v.key, string(v.val))
}

// BlobID creates a parameter for a blob ID.
//
//nolint:revive
func BlobID(name string, id blob.ID) blobIDParam {
	return blobIDParam{key: name, val: id}
}

type blobIDListParam struct {
	key  string
	list []blob.ID
}

func (v blobIDListParam) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginListField(v.key)

	for _, blobID := range v.list {
		jw.StringElement(string(blobID))
	}

	jw.EndList()
}

// BlobIDList creates a parameter for a list of blob IDs.
//
//nolint:revive
func BlobIDList(name string, list []blob.ID) blobIDListParam {
	return blobIDListParam{key: name, list: list}
}

type blobMetadataParam struct {
	key string
	val blob.Metadata
}

func (v blobMetadataParam) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginObjectField(v.key)
	jw.StringField("blobID", string(v.val.BlobID))
	jw.Int64Field("l", v.val.Length)
	jw.TimeField("ts", v.val.Timestamp)
	jw.EndObject()
}

// BlobMetadata creates a parameter for a blob metadata.
//
//nolint:revive
func BlobMetadata(name string, bm blob.Metadata) blobMetadataParam {
	return blobMetadataParam{key: name, val: bm}
}
