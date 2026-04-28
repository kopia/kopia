package storj

import (
	"errors"
	"io"

	"storj.io/uplink"

	"github.com/kopia/kopia/repo/blob"
)

// uplink returns the errors as presented here:
// https://github.com/storj/uplink/blob/f9227df540b169562fb7df4fb5571091635b891a/common.go#L48
// but `kopia` compares against different error types,
// e.g. like here: https://github.com/kopia/kopia/blob/9237b29023b462b6d18ba411c6ec208385377db3/internal/blobtesting/asserts.go#L111
// This means our blob interface needs to take care to return the expected/corresponding error type

// defined in storj.io/uplink:
// ErrTooManyRequests is returned when user has sent too many requests in a given amount of time.
// ErrBandwidthLimitExceeded is returned when project will exceeded bandwidth limit.
// ErrStorageLimitExceeded is returned when project will exceeded storage limit.
// ErrSegmentsLimitExceeded is returned when project will exceeded segments limit.
// ErrPermissionDenied is returned when the request is denied due to invalid permissions.
// ErrBucketNameInvalid is returned when the bucket name is invalid.
// ErrBucketAlreadyExists is returned when the bucket already exists during creation.
// ErrBucketNotEmpty is returned when the bucket is not empty during deletion.
// ErrBucketNotFound is returned when the bucket is not found.

// for now we only convert errors that have/need a specific kopia type, all others are passed as-is
func convertKnownError(uplinkErr error) (kopiaErr error) {
	switch {
	case errors.Is(uplinkErr, io.EOF):
		return uplinkErr
	case errors.Is(uplinkErr, uplink.ErrPermissionDenied):
		return errors.Join(blob.ErrInvalidCredentials, uplinkErr) // this is not really the same, but for now a best match
	case errors.Is(uplinkErr, uplink.ErrBucketNotFound) || errors.Is(uplinkErr, uplink.ErrObjectNotFound):
		return errors.Join(blob.ErrBlobNotFound, uplinkErr) // FIXME: (how) does kopia distinguish between a bucket, dir and file object?
	default:
		return uplinkErr
	}
}

// func errwrapf(format string, err error, args ...interface{}) error {
// 	var all []interface{}
// 	all = append(all, err)
// 	all = append(all, args...)
// 	return packageError.Wrap(fmt.Errorf(format, all...))
// }
