package manifest

import (
	"sort"
	"time"
)

// EntryMetadata contains metadata about manifest item. Each manifest item has one or more labels
// Including required "type" label.
type EntryMetadata struct {
	ID      ID                `json:"id"`
	Length  int               `json:"length"`
	Labels  map[string]string `json:"labels"`
	ModTime time.Time         `json:"mtime"`
}

// DedupeEntryMetadataByLabel deduplicates EntryMetadata in a provided slice by picking
// the latest one for a given set of label.
func DedupeEntryMetadataByLabel(entries []*EntryMetadata, label string) []*EntryMetadata {
	var result []*EntryMetadata

	m := map[string]*EntryMetadata{}

	for _, e := range entries {
		u := e.Labels[label]
		if isLaterThan(e, m[u]) {
			m[u] = e
		}
	}

	for _, v := range m {
		result = append(result, v)
	}

	sort.Slice(result, func(i, j int) bool {
		if l, r := result[i].ModTime, result[j].ModTime; !l.Equal(r) {
			return l.Before(r)
		}

		if l, r := result[i].ID, result[j].ID; l != r {
			return l < r
		}

		return false
	})

	return result
}

// PickLatestID picks the ID of latest EntryMetadata in a given slice.
func PickLatestID(entries []*EntryMetadata) ID {
	var latest *EntryMetadata

	for _, e := range entries {
		if isLaterThan(e, latest) {
			latest = e
		}
	}

	if latest == nil {
		return ""
	}

	return latest.ID
}

// isLaterThan returns true if a is later than b by examining modification time.
// in case modification times are the same, arbitrary ordering based on IDs is used.
func isLaterThan(a, b *EntryMetadata) bool {
	if b == nil {
		return true
	}

	if a == nil {
		return false
	}

	if a.ModTime.After(b.ModTime) {
		return true
	}

	return a.ID > b.ID
}
