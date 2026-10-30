package epoch

import (
	"github.com/kopia/kopia/repo/blob"
)

// RangeMetadata represents a range of indexes for [min,max] epoch range. Both min and max are inclusive.
type RangeMetadata struct {
	MinEpoch int             `json:"min"`
	MaxEpoch int             `json:"max"`
	Blobs    []blob.Metadata `json:"blobs"`
}

func findLongestRangeCheckpoint(ranges []*RangeMetadata) []*RangeMetadata {
	byMin := map[int][]*RangeMetadata{}

	for _, r := range ranges {
		byMin[r.MinEpoch] = append(byMin[r.MinEpoch], r)
	}

	return findLongestRangeCheckpointStartingAt(0, byMin, make(map[int][]*RangeMetadata))
}

func findLongestRangeCheckpointStartingAt(startEpoch int, byMin, memo map[int][]*RangeMetadata) []*RangeMetadata {
	l, ok := memo[startEpoch]
	if ok {
		return l
	}

	var (
		longest         = 0
		longestMetadata []*RangeMetadata
	)

	for _, cp := range byMin[startEpoch] {
		combined := append([]*RangeMetadata{cp}, findLongestRangeCheckpointStartingAt(cp.MaxEpoch+1, byMin, memo)...)

		if m := combined[len(combined)-1].MaxEpoch; (m > longest) || (m == longest && len(combined) < len(longestMetadata)) {
			longest = m
			longestMetadata = combined
		}
	}

	memo[startEpoch] = longestMetadata

	return longestMetadata
}
