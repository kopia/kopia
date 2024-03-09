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

	for s != "" && !unicode.IsDigit(rune(s[0])) {
		s = s[1:]
	}

	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, false
	}

	return n, true
}

// epochNumberFromBlobID extracts the range epoch numbers from a string formatted as
// <prefix><epochNumber>_<epochNumber2>_<remainder>.
func epochRangeFromBlobID(blobID blob.ID) (min, max int, ok bool) {
	parts := strings.Split(string(blobID), "_")

	//nolint:gomnd
	if len(parts) < 3 {
		return 0, 0, false
	}

	first := parts[0]
	second := parts[1]

	for first != "" && !unicode.IsDigit(rune(first[0])) {
		first = first[1:]
	}

	n1, err1 := strconv.Atoi(first)
	n2, err2 := strconv.Atoi(second)

	return n1, n2, err1 == nil && err2 == nil
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

func groupByEpochRanges(bms []blob.Metadata) map[int]map[int][]blob.Metadata {
	result := map[int]map[int][]blob.Metadata{}

	for _, bm := range bms {
		if n1, n2, ok := epochRangeFromBlobID(bm.BlobID); ok {
			if result[n1] == nil {
				result[n1] = make(map[int][]blob.Metadata)
			}

			result[n1][n2] = append(result[n1][n2], bm)
		}
	}

	return result
}

func deletionWatermarkFromBlobID(blobID blob.ID) (time.Time, bool) {
	str := strings.TrimPrefix(string(blobID), string(DeletionWatermarkBlobPrefix))

	unixSeconds, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		return time.Time{}, false
	}

	return time.Unix(unixSeconds, 0), true
}
