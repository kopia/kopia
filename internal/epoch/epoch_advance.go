package epoch

import (
	"time"

	"github.com/kopia/kopia/repo/blob"
)

// shouldAdvance determines if the current epoch should be advanced based on set of blobs in it.
//
// Epoch will be advanced if it's been more than 'minEpochDuration' between earliest and
// most recent write AND at least one of the criteria has been met:
//
// - number of blobs in the epoch exceeds 'countThreshold'
// - total size of blobs in the epoch exceeds 'totalSizeBytesThreshold'.
func shouldAdvance(bms []blob.Metadata, minEpochDuration time.Duration, countThreshold int, totalSizeBytesThreshold int64) bool {
	if len(bms) == 0 {
		return false
	}

	var (
		minTime   = bms[0].Timestamp
		maxTime   = bms[0].Timestamp
		totalSize = int64(0)
	)

	for _, bm := range bms {
		if bm.Timestamp.Before(minTime) {
			minTime = bm.Timestamp
		}

		if bm.Timestamp.After(maxTime) {
			maxTime = bm.Timestamp
		}

		totalSize += bm.Length
	}

	// not enough time between first and last write in an epoch.
	if maxTime.Sub(minTime) < minEpochDuration {
		return false
	}

	if len(bms) >= countThreshold {
		return true
	}

	if totalSize >= totalSizeBytesThreshold {
		return true
	}

	return false
}
