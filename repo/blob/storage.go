package blob

import (
	"context"
	"encoding/json"
	"io"
	"sync"
	"time"

	"github.com/pkg/errors"
)

// ErrSetTimeUnsupported is returned by implementations of Storage that don't support SetTime.
var ErrSetTimeUnsupported = errors.Errorf("SetTime is not supported")

// ErrInvalidRange is returned when the requested blob offset or length is invalid.
var ErrInvalidRange = errors.Errorf("invalid blob offset or length")

// Bytes encapsulates a sequence of bytes, possibly stored in a non-contiguous buffers,
// which can be written sequentially or treated as a io.Reader.
type Bytes interface {
	io.WriterTo

	Length() int
	Reader() io.Reader
}

// Reader defines read access API to blob storage.
type Reader interface {
	// GetBlob returns full or partial contents of a blob with given ID.
	// If length>0, the the function retrieves a range of bytes [offset,offset+length)
	// If length<0, the entire blob must be fetched.
	// Returns ErrInvalidRange if the fetched blob length is invalid.
	GetBlob(ctx context.Context, blobID ID, offset, length int64) ([]byte, error)

	// GetMetadata returns Metadata about single blob.
	GetMetadata(ctx context.Context, blobID ID) (Metadata, error)

	// ListBlobs invokes the provided callback for each blob in the storage.
	// Iteration continues until the callback returns an error or until all matching blobs have been reported.
	ListBlobs(ctx context.Context, blobIDPrefix ID, cb func(bm Metadata) error) error

	// ConnectionInfo returns JSON-serializable data structure containing information required to
	// connect to storage.
	ConnectionInfo() ConnectionInfo

	// Name of the storage used for quick identification by humans.
	DisplayName() string
}

// Storage encapsulates API for connecting to blob storage.
//
// The underlying storage system must provide:
//
// * high durability, availability and bit-rot protection
// * read-after-write - blob written using PubBlob() must be immediately readable using GetBlob() and ListBlobs()
// * atomicity - it mustn't be possible to observe partial results of PubBlob() via either GetBlob() or ListBlobs()
// * timestamps that don't go back in time (small clock skew up to minutes is allowed)
// * reasonably low latency for retrievals
//
// The required semantics are provided by existing commercial cloud storage products (Google Cloud, AWS, Azure).
type Storage interface {
	Reader

	// PutBlob uploads the blob with given data to the repository or replaces existing blob with the provided
	// id with contents gathered from the specified list of slices.
	PutBlob(ctx context.Context, blobID ID, data Bytes) error

	// SetTime changes last modification time of a given blob, if supported, returns ErrSetTimeUnsupported otherwise.
	SetTime(ctx context.Context, blobID ID, t time.Time) error

	// DeleteBlob removes the blob from storage. Future Get() operations will fail with ErrNotFound.
	DeleteBlob(ctx context.Context, blobID ID) error

	// Close releases all resources associated with storage.
	Close(ctx context.Context) error
}

// ID is a string that represents blob identifier.
type ID string

// Metadata represents metadata about a single BLOB in a storage.
type Metadata struct {
	BlobID    ID        `json:"id"`
	Length    int64     `json:"length"`
	Timestamp time.Time `json:"timestamp"`
}

func (m *Metadata) String() string {
	b, _ := json.Marshal(m)
	return string(b)
}

// ErrBlobNotFound is returned when a BLOB cannot be found in storage.
var ErrBlobNotFound = errors.New("BLOB not found")

// ListAllBlobs returns Metadata for all blobs in a given storage that have the provided name prefix.
func ListAllBlobs(ctx context.Context, st Storage, prefix ID) ([]Metadata, error) {
	var result []Metadata

	err := st.ListBlobs(ctx, prefix, func(bm Metadata) error {
		result = append(result, bm)
		return nil
	})

	return result, errors.Wrap(err, "error listing all blobs")
}

// IterateAllPrefixesInParallel invokes the provided callback and returns the first error returned by the callback or nil.
func IterateAllPrefixesInParallel(ctx context.Context, parallelism int, st Storage, prefixes []ID, callback func(Metadata) error) error {
	if len(prefixes) == 1 {
		return st.ListBlobs(ctx, prefixes[0], callback)
	}

	if parallelism <= 0 {
		parallelism = 1
	}

	var wg sync.WaitGroup

	semaphore := make(chan struct{}, parallelism)
	errch := make(chan error, len(prefixes))

	for _, prefix := range prefixes {
		wg.Add(1)

		prefix := prefix

		// acquire semaphore
		semaphore <- struct{}{}

		go func() {
			defer wg.Done()
			defer func() {
				<-semaphore // release semaphore
			}()

			if err := st.ListBlobs(ctx, prefix, callback); err != nil {
				errch <- err
			}
		}()
	}

	wg.Wait()
	close(errch)

	// return first error or nil
	return <-errch
}

// EnsureLengthExactly validates that length of the given slice is exactly the provided value.
// and returns ErrInvalidRange if the length is of the slice if not.
// As a special case length < 0 disables validation.
func EnsureLengthExactly(b []byte, length int64) ([]byte, error) {
	if length < 0 {
		return b, nil
	}

	if len(b) != int(length) {
		return nil, errors.Wrapf(ErrInvalidRange, "invalid length %v, expected %v", len(b), length)
	}

	return b, nil
}

// EnsureLengthAndTruncate validates that length of the given slice is at least the provided value
// and returns ErrInvalidRange if the length is of the slice if not.
// As a special case length < 0 disables validation.
func EnsureLengthAndTruncate(b []byte, length int64) ([]byte, error) {
	if length < 0 {
		return b, nil
	}

	if len(b) < int(length) {
		return nil, errors.Wrapf(ErrInvalidRange, "invalid length %v, expected at least %v", len(b), length)
	}

	return b[0:length], nil
}
