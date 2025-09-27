// Package contentparam provides parameters for logging content-related operations.
package contentparam

import (
	"github.com/kopia/kopia/internal/contentlog"
	"github.com/kopia/kopia/repo/content/index"
)

const maxLoggedContentIDLength = 5

type contentIDParam struct {
	Key   string
	Value index.ID
}

func (e contentIDParam) WriteValueTo(jw *contentlog.JSONWriter) {
	var buf [128]byte

	jw.RawJSONField(e.Key, e.Value.AppendToJSON(buf[:0], maxLoggedContentIDLength))
}

// ContentID is a parameter that writes a content ID to the JSON writer.
//
//nolint:revive
func ContentID(key string, value index.ID) contentIDParam {
	return contentIDParam{Key: key, Value: value}
}
