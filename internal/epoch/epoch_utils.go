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

type intRange struct {
	lo, hi int
}

func (r intRange) length() uint {
	return uint(r.hi - r.lo)
}

func (r intRange) isEmpty() bool {
	return r.length() == 0
}

var errNonContiguousRange = errors.New("non-contiguous range")

// constants from the standard math package.
const (
	//nolint:gomnd
	intSize = 32 << (^uint(0) >> 63) // 32 or 64

	maxInt = 1<<(intSize-1) - 1
	minInt = -1 << (intSize - 1)
)

// Returns a continuous close-open epoch range for the keys, that is [lo, hi).
// A range of the form [v,v) means the range is empty.
// When the range is not continuous an error is returned.
func getKeyRange[E any](m map[int]E) (intRange, error) {
	var count uint

	lo, hi := maxInt, minInt

	for k := range m {
		if k < lo {
			lo = k
		}

		if k > hi {
			hi = k
		}

		count++
	}

	if count == 0 {
		return intRange{}, nil
	}

	// hi and lo are from unique map keys, so for the range to be continuous
	// the difference between hi and lo cannot be larger than count -1.
	// For example, if lo==2 & hi==4, then count must be 3 => {2, 3, 4}.
	if uint(hi-lo) > count-1 {
		return intRange{}, errors.Wrapf(errNonContiguousRange, "[lo: %d, hi: %d], length: %d", lo, hi, count)
	}

	return intRange{lo: lo, hi: hi + 1}, nil
}
