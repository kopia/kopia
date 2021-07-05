// Package completeset manages complete set of blob metadata.
package completeset

import (
	"strconv"
	"strings"

	"github.com/kopia/kopia/repo/blob"
)

// FindFirst looks for a first complete set of blobs IDs following a naming convention:
//    '<any>-s<set>-c<count>'
// where:
//   'prefix' is arbitrary string not containing a dash ('-')
//   'set' is a random string shared by all indexes in the same set
//   'count' is a number that specifies how many items must be in the set to make it complete.
//
// The algorithm returns IDs of blobs that form the first complete set.
func FindFirst(bms []blob.Metadata) []blob.Metadata {
	sets := map[string][]blob.Metadata{}

	for _, bm := range bms {
		id := bm.BlobID
		parts := strings.Split(string(id), "-")

		if len(parts) < 3 || !strings.HasPrefix(parts[1], "s") || !strings.HasPrefix(parts[2], "c") {
			// malformed ID, ignore
			continue
		}

		count, err := strconv.Atoi(parts[2][1:])
		if err != nil {
			// malformed ID, ignore
			continue
		}

		setID := parts[1]
		sets[setID] = append(sets[setID], bm)

		if len(sets[setID]) == count {
			return sets[setID]
		}
	}

	return nil
}
