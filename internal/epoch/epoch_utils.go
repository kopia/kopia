package epoch

import (
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/pkg/errors"

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

// epochRangeFromBlobID extracts the range epoch numbers from a string formatted as
// <prefix><epochNumber>_<epochNumber2>_<remainder>.
func epochRangeFromBlobID(blobID blob.ID) (minEpoch, maxEpoch int, ok bool) {
	parts := strings.Split(string(blobID), "_")

	//nolint:mnd
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

// closedIntRange represents a discrete closed-closed [lo, hi] range for ints.
// That is, the range includes both lo and hi.
type closedIntRange struct {
	lo, hi int
}

func (r closedIntRange) length() uint {
	// any range where lo > hi is empty. The canonical empty representation
	// is {lo:0, hi: -1}
	if r.lo > r.hi {
		return 0
	}

	return uint(r.hi - r.lo + 1) //nolint:gosec
}

func (r closedIntRange) isEmpty() bool {
	return r.length() == 0
}

// constants from the standard math package.
const (
	//nolint:mnd
	intSize = 32 << (^uint(0) >> 63) // 32 or 64

	maxInt = 1<<(intSize-1) - 1
	minInt = -1 << (intSize - 1)
)

func getFirstContiguousKeyRange[E any](m map[int]E) closedIntRange {
	if len(m) == 0 {
		return closedIntRange{lo: 0, hi: -1}
	}

	keys := make([]int, 0, len(m))

	for k := range m {
		keys = append(keys, k)
	}

	slices.Sort(keys)

	lo := keys[0]
	if hi := keys[len(keys)-1]; hi-lo+1 == len(m) {
		// the difference between the largest and smallest key is the same as
		// the length of the key set, then the range is contiguous
		return closedIntRange{lo: lo, hi: hi}
	}

	hi := lo
	for _, v := range keys[1:] {
		if v != hi+1 {
			break
		}

		hi = v
	}

	return closedIntRange{lo: lo, hi: hi}
}

func getCompactedEpochRange(cs CurrentSnapshot) closedIntRange {
	return getFirstContiguousKeyRange(cs.SingleEpochCompactionSets)
}

var errInvalidCompactedRange = errors.New("invalid compacted epoch range")

func getRangeCompactedRange(cs CurrentSnapshot) closedIntRange {
	rangeSetsLen := len(cs.LongestRangeCheckpointSets)

	if rangeSetsLen == 0 {
		return closedIntRange{lo: 0, hi: -1}
	}

	return closedIntRange{
		lo: cs.LongestRangeCheckpointSets[0].MinEpoch,
		hi: cs.LongestRangeCheckpointSets[rangeSetsLen-1].MaxEpoch,
	}
}

func oldestUncompactedEpoch(cs CurrentSnapshot) (int, error) {
	rangeCompacted := getRangeCompactedRange(cs)

	var oldestUncompacted int

	if !rangeCompacted.isEmpty() {
		if rangeCompacted.lo != 0 {
			// range compactions are expected to cover the 0 epoch
			return -1, errors.Wrapf(errInvalidCompactedRange, "Epoch 0 not included in range compaction, lowest epoch in range compactions: %v", rangeCompacted.lo)
		}

		oldestUncompacted = rangeCompacted.hi + 1
	}

	singleCompacted := getCompactedEpochRange(cs)

	if singleCompacted.isEmpty() || oldestUncompacted < singleCompacted.lo {
		return oldestUncompacted, nil
	}

	// singleCompacted is not empty
	if oldestUncompacted > singleCompacted.hi {
		return oldestUncompacted, nil
	}

	return singleCompacted.hi + 1, nil
}
