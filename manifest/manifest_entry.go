package manifest

import "time"

// EntryMetadata contains metadata about manifest item. Each manifest item has one or more labels
// Including required "type" label.
type EntryMetadata struct {
	ID      string
	Length  int
	Labels  map[string]string
	ModTime time.Time
}

// EntryIDs returns the list of IDs for the provided list of EntryMetadata.
func EntryIDs(entries []*EntryMetadata) []string {
	var ids []string
	for _, e := range entries {
		ids = append(ids, e.ID)
	}
	return ids
}
