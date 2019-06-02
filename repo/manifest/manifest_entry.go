package manifest

import "time"

// EntryMetadata contains metadata about manifest item. Each manifest item has one or more labels
// Including required "type" label.
type EntryMetadata struct {
	ID      ID
	Length  int
	Labels  map[string]string
	ModTime time.Time
}
