package manifest

import "time"

// EntryMetadata contains metadata about manifest item. Each manifest item has one or more labels
// Including required "type" label.
type EntryMetadata struct {
	ID      ID                `json:"id"`
	Length  int               `json:"length"`
	Labels  map[string]string `json:"labels"`
	ModTime time.Time         `json:"mtime"`
}
