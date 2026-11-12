package epoch

import "github.com/kopia/kopia/repo/blob"

// RangeCheckpointBlobPrefix returns the blob ID prefix of a range checkpoint covering epochs [epoch1, epoch2].
func RangeCheckpointBlobPrefix(epoch1, epoch2 int) blob.ID {
	return rangeCheckpointBlobPrefix(epoch1, epoch2)
}
