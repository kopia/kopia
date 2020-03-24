package blob

import (
	"context"
	"io"
	"log"
	"sync"
	"time"

	"github.com/pkg/errors"
)

// Bytes encapsulates a sequence of bytes, possibly stored in a non-contiguous buffers,
// which can be written sequentially or treated as a io.Reader.
type Bytes interface {
	io.WriterTo

	Length() int
	Reader() io.Reader
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
	// PutBlob uploads the blob with given data to the repository or replaces existing blob with the provided
	// id with contents gathered from the specified list of slices.
	PutBlob(ctx context.Context, blobID ID, data Bytes) error

	// DeleteBlob removes the blob from storage. Future Get() operations will fail with ErrNotFound.
	DeleteBlob(ctx context.Context, blobID ID) error

	// GetBlob returns full or partial contents of a blob with given ID.
	// If length>0, the the function retrieves a range of bytes [offset,offset+length)
	// If length<0, the entire blob must be fetched.
	GetBlob(ctx context.Context, blobID ID, offset, length int64) ([]byte, error)

	// ListBlobs invokes the provided callback for each blob in the storage.
	// Iteration continues until the callback returns an error or until all matching blobs have been reported.
	ListBlobs(ctx context.Context, blobIDPrefix ID, cb func(bm Metadata) error) error

	// ConnectionInfo returns JSON-serializable data structure containing information required to
	// connect to storage.
	ConnectionInfo() ConnectionInfo

	// Close releases all resources associated with storage.
	Close(ctx context.Context) error
}

// ID is a string that represents blob identifier.
type ID string

// Metadata represents metadata about a single BLOB in a storage.
type Metadata struct {
	BlobID    ID
	Length    int64
	Timestamp time.Time
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

	return result, err
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

// ListAllBlobsConsistent lists all blobs with given name prefix in the provided storage until the results are
// consistent. The results are consistent if the list result fetched twice is identical. This guarantees that while
// the first scan was in progress, no new blob was added or removed.
// maxAttempts specifies maximum number of list attempts (must be >= 2)
func ListAllBlobsConsistent(ctx context.Context, st Storage, prefix ID, maxAttempts int) ([]Metadata, error) {
	var previous []Metadata

	for i := 0; i < maxAttempts; i++ {
		result, err := ListAllBlobs(ctx, st, prefix)
		if err != nil {
			return nil, err
		}

		if i > 0 && sameBlobs(result, previous) {
			return result, nil
		}

		previous = result
	}

	return nil, errors.Errorf("unable to achieve consistent snapshot despite %v attempts", maxAttempts)
}

// sameBlobs returns true if b1 & b2 contain the same blobs (ignoring order).
func sameBlobs(b1, b2 []Metadata) bool {
	if len(b1) != len(b2) {
		log.Printf("a")

		return false
	}

	m := map[ID]Metadata{}

	for _, b := range b1 {
		m[b.BlobID] = normalizeMetadata(b)
	}

	for _, b := range b2 {
		if r := m[b.BlobID]; r != normalizeMetadata(b) {
			return false
		}
	}

	return true
}

func normalizeMetadata(m Metadata) Metadata {
	return Metadata{m.BlobID, m.Length, normalizeTimestamp(m.Timestamp)}
}

func normalizeTimestamp(t time.Time) time.Time {
	return time.Unix(0, t.UnixNano())
}
