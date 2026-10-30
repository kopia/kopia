package index

// IDRange represents a range of IDs.
type IDRange struct {
	StartID IDPrefix // inclusive
	EndID   IDPrefix // exclusive
}

// Contains determines whether given ID is in the range.
func (r IDRange) Contains(id ID) bool {
	return id.comparePrefix(r.StartID) >= 0 && id.comparePrefix(r.EndID) < 0
}

const maxIDCharacterPlus1 = "\x7B"

// PrefixRange returns ID range that contains all IDs with a given prefix.
func PrefixRange(prefix IDPrefix) IDRange {
	return IDRange{prefix, prefix + maxIDCharacterPlus1}
}

// AllIDs is an IDRange that contains all valid IDs.
//
//nolint:gochecknoglobals
var AllIDs = IDRange{"", maxIDCharacterPlus1}

// AllPrefixedIDs is an IDRange that contains all valid IDs prefixed IDs ('g' .. 'z').
//
//nolint:gochecknoglobals
var AllPrefixedIDs = IDRange{"g", maxIDCharacterPlus1}

// AllNonPrefixedIDs is an IDRange that contains all valid IDs non-prefixed IDs ('0' .. 'f').
//
//nolint:gochecknoglobals
var AllNonPrefixedIDs = IDRange{"0", "g"}
