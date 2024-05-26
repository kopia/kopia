package epoch

import (
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

	return uint(r.hi - r.lo + 1)
}

func (r closedIntRange) isEmpty() bool {
	return r.length() == 0
}

var errNonContiguousRange = errors.New("non-contiguous range")

// constants from the standard math package.
const (
	//nolint:mnd
	intSize = 32 << (^uint(0) >> 63) // 32 or 64

	maxInt = 1<<(intSize-1) - 1
	minInt = -1 << (intSize - 1)
)

// Returns a range for the keys in m. It returns an empty range when m is empty.
func getKeyRange[E any](m map[int]E) closedIntRange {
	if len(m) == 0 {
		return closedIntRange{lo: 0, hi: -1}
	}

	lo, hi := maxInt, minInt
	for k := range m {
		if k < lo {
			lo = k
		}

		if k > hi {
			hi = k
		}
	}

	return closedIntRange{lo: lo, hi: hi}
}

// Returns a contiguous range for the keys in m.
// When the range is not continuous an error is returned.
func getContiguousKeyRange[E any](m map[int]E) (closedIntRange, error) {
	r := getKeyRange(m)

	// r.hi and r.lo are from unique map keys, so for the range to be continuous
	// then the range length must be exactly the same as the size of the map.
	// For example, if lo==2, hi==4, and len(m) == 3, the range must be
	// contiguous => {2, 3, 4}
	if r.length() != uint(len(m)) {
		return closedIntRange{-1, -2}, errors.Wrapf(errNonContiguousRange, "[lo: %d, hi: %d], length: %d", r.lo, r.hi, len(m))
	}

	return r, nil
}

func getCompactedEpochRange(cs CurrentSnapshot) (closedIntRange, error) {
	return getContiguousKeyRange(cs.SingleEpochCompactionSets)
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

	singleCompacted, err := getCompactedEpochRange(cs)
	if err != nil {
		return -1, errors.Wrap(err, "could not get latest single-compacted epoch")
	}

	if singleCompacted.isEmpty() || oldestUncompacted < singleCompacted.lo {
		return oldestUncompacted, nil
	}

	// singleCompacted is not empty
	if oldestUncompacted > singleCompacted.hi {
		return oldestUncompacted, nil
	}

	return singleCompacted.hi + 1, nil
}
