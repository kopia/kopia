package snapshot

import (
	"sort"
	"time"

	"github.com/kopia/kopia/object"
)

// Manifest represents information about a single point-in-time filesystem snapshot.
type Manifest struct {
	ID     string     `json:"-"`
	Source SourceInfo `json:"source"`

	Description string    `json:"description"`
	StartTime   time.Time `json:"startTime"`
	EndTime     time.Time `json:"endTime"`

	RootObjectID        object.ID `json:"root"`
	HashCacheID         object.ID `json:"hashCache"`
	HashCacheCutoffTime time.Time `json:"hashCacheCutoff"`
	Stats               Stats     `json:"stats"`
	IncompleteReason    string    `json:"incomplete,omitempty"`

	RetentionReasons []string `json:"-"`
}

// GroupBySource returns a slice of slices, such that each result item contains manifests from a single source.
func GroupBySource(manifests []*Manifest) [][]*Manifest {
	resultMap := map[SourceInfo][]*Manifest{}
	for _, m := range manifests {
		resultMap[m.Source] = append(resultMap[m.Source], m)
	}

	var result [][]*Manifest
	for _, v := range resultMap {
		result = append(result, v)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i][0].Source.String() < result[j][0].Source.String()
	})

	return result
}

// SortByTime returns a slice of manifests sorted by start time.
func SortByTime(manifests []*Manifest, reverse bool) []*Manifest {
	result := append([]*Manifest(nil), manifests...)
	sort.Slice(result, func(i, j int) bool {
		return result[i].StartTime.After(result[j].StartTime) == reverse
	})

	return result
}
