package epoch

import (
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/kopia/kopia/repo/blob"
)

// epochNumberFromBlobID extracts the epoch number from a string formatted as
// <prefix><epochNumber>_<remainder>.
func epochNumberFromBlobID(blobID blob.ID) (int, bool) {
	s := string(blobID)

	if p := strings.IndexByte(s, '_'); p >= 0 {
		s = s[0:p]
	}

	for len(s) > 0 && !unicode.IsDigit(rune(s[0])) {
		s = s[1:]
	}

	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, false
	}

	return n, true
}

func blobsWrittenBefore(bms []blob.Metadata, maxTime time.Time) []blob.Metadata {
	var result []blob.Metadata

	for _, bm := range bms {
		if !maxTime.IsZero() && bm.Timestamp.After(maxTime) {
			continue
		}

		result = append(result, bm)
	}

	return result
}

func groupByEpochNumber(bms []blob.Metadata) map[int][]blob.Metadata {
	result := map[int][]blob.Metadata{}

	for _, bm := range bms {
		if n, ok := epochNumberFromBlobID(bm.BlobID); ok {
			result[n] = append(result[n], bm)
		}
	}

	return result
}
