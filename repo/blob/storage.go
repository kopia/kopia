package blob

import (
	"context"
	"encoding/json"
	"io"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/kopia/kopia/repo/logging"
)

var log = logging.Module("blob")

// ErrSetTimeUnsupported is returned by implementations of Storage that don't support SetTime.
var ErrSetTimeUnsupported = errors.New("SetTime is not supported")

// ErrInvalidRange is returned when the requested blob offset or length is invalid.
var ErrInvalidRange = errors.New("invalid blob offset or length")

// InvalidCredentialsErrStr is the error string returned by the provider
// when a token has expired.
const InvalidCredentialsErrStr = "The provided token has expired"

// ErrInvalidCredentials is returned when the token used for
// authenticating with a storage provider has expired.
var ErrInvalidCredentials = errors.Errorf(InvalidCredentialsErrStr)

// ErrBlobAlreadyExists is returned when attempting to put a blob that already exists.
var ErrBlobAlreadyExists = errors.New("blob already exists")

// ErrUnsupportedPutBlobOption is returned when a PutBlob option that is not supported
// by an implementation of Storage is specified in a PutBlob call.
var ErrUnsupportedPutBlobOption = errors.New("unsupported put-blob option")

// ErrNotAVolume is returned when attempting to use a Volume method against a storage
// implementation that does not support the intended functionality.
var ErrNotAVolume = errors.New("unsupported method, storage is not a volume")

// ErrUnsupportedObjectLock is returned when attempting to use an Object Lock specific
// function on a storage implementation that does not have the intended functionality.
var ErrUnsupportedObjectLock = errors.New("object locking unsupported")

// Bytes encapsulates a sequence of bytes, possibly stored in a non-contiguous buffers,
// which can be written sequentially or treated as a io.Reader.
type Bytes interface {
	io.WriterTo

	Length() int
	Reader() io.ReadSeekCloser
}

// OutputBuffer is implemented by *gather.WriteBuffer.
type OutputBuffer interface {
	io.Writer

	Reset()
	Length() int
}

// Capacity describes the storage capacity and usage of a Volume.
type Capacity struct {
	// Size of volume in bytes.
	SizeB uint64 `json:"capacity,omitempty"`
	// Available (writeable) space in bytes.
	FreeB uint64 `json:"available"`
}

// Volume defines disk/volume access API to blob storage.
type Volume interface {
	// GetCapacity returns the capacity of a given volume.
	GetCapacity(ctx context.Context) (Capacity, error)
}

// Reader defines read access API to blob storage.
type Reader interface {
	// GetBlob returns full or partial contents of a blob with given ID.
	// If length>0, the function retrieves a range of bytes [offset,offset+length)
	// If length<0, the entire blob must be fetched.
	// Returns ErrInvalidRange if the fetched blob length is invalid.
	GetBlob(ctx context.Context, blobID ID, offset, length int64, output OutputBuffer) error

	// GetMetadata returns Metadata about single blob.
	GetMetadata(ctx context.Context, blobID ID) (Metadata, error)

	// ListBlobs invokes the provided callback for each blob in the storage.
	// Iteration continues until the callback returns an error or until all matching blobs have been reported.
	ListBlobs(ctx context.Context, blobIDPrefix ID, cb func(bm Metadata) error) error

	// ConnectionInfo returns JSON-serializable data structure containing information required to
	// connect to storage.
	ConnectionInfo() ConnectionInfo

	// DisplayName Name of the storage used for quick identification by humans.
	DisplayName() string
}

// RetentionMode - object retention mode.
type RetentionMode string

const (
	// Governance - governance mode.
	Governance RetentionMode = "GOVERNANCE"

	// Compliance - compliance mode.
	Compliance RetentionMode = "COMPLIANCE"

	// Locked - Locked policy mode for Azure.
	Locked RetentionMode = RetentionMode(blob.ImmutabilityPolicyModeLocked)
)

func (r RetentionMode) String() string {
	return string(r)
}

// IsValid - check whether this retention mode is valid or not.
func (r RetentionMode) IsValid() bool {
	return r == Governance || r == Compliance
}

// PutOptions represents put-options for a single BLOB in a storage.
type PutOptions struct {
	RetentionMode   RetentionMode
	RetentionPeriod time.Duration

	// if true, PutBlob will fail with ErrBlobAlreadyExists if a blob with the same ID exists.
	DoNotRecreate bool

	// if not empty, set the provided timestamp on the blob instead of server-assigned,
	// if unsupported by the server return ErrSetTimeUnsupported
	SetModTime time.Time
	GetModTime *time.Time // if != nil, populate the value pointed at with the actual modification time
}

// ExtendOptions represents retention options for extending object locks.
type ExtendOptions struct {
	RetentionMode   RetentionMode
	RetentionPeriod time.Duration
}

// DefaultProviderImplementation provides a default implementation for
// common functions that are mostly provider independent and have a sensible
// default.
//
// Storage providers should embed this struct and override functions that they
// have different return values for.
type DefaultProviderImplementation struct{}

// ExtendBlobRetention provides a common implementation for unsupported blob retention storage.
func (s DefaultProviderImplementation) ExtendBlobRetention(context.Context, ID, ExtendOptions) error {
	return ErrUnsupportedObjectLock
}

// IsReadOnly complies with the Storage interface.
func (s DefaultProviderImplementation) IsReadOnly() bool {
	return false
}

// Close complies with the Storage interface.
func (s DefaultProviderImplementation) Close(context.Context) error {
	return nil
}

// FlushCaches complies with the Storage interface.
func (s DefaultProviderImplementation) FlushCaches(context.Context) error {
	return nil
}

// GetCapacity complies with the Storage interface.
func (s DefaultProviderImplementation) GetCapacity(context.Context) (Capacity, error) {
	return Capacity{}, ErrNotAVolume
}

// HasRetentionOptions returns true when blob-retention settings have been
// specified, otherwise returns false.
func (o PutOptions) HasRetentionOptions() bool {
	return o.RetentionPeriod != 0 || o.RetentionMode != ""
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
	Volume
	Reader

	// PutBlob uploads the blob with given data to the repository or replaces existing blob with the provided
	// id with contents gathered from the specified list of slices.
	PutBlob(ctx context.Context, blobID ID, data Bytes, opts PutOptions) error

	// DeleteBlob removes the blob from storage. Future Get() operations will fail with ErrNotFound.
	DeleteBlob(ctx context.Context, blobID ID) error

	// Close releases all resources associated with storage.
	Close(ctx context.Context) error

	// FlushCaches flushes any local caches associated with storage.
	FlushCaches(ctx context.Context) error

	// ExtendBlobRetention extends the retention time of a blob (when blob retention is enabled)
	ExtendBlobRetention(ctx context.Context, blobID ID, opts ExtendOptions) error

	// IsReadOnly returns whether this Storage is in read-only mode. When in
	// read-only mode all mutation operations will fail.
	IsReadOnly() bool
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
	b, err := json.Marshal(m)
	if err != nil {
		return "<invalid>"
	}

	return string(b)
}

// ErrBlobNotFound is returned when a BLOB cannot be found in storage.
var ErrBlobNotFound = errors.New("BLOB not found")

// ListAllBlobs returns Metadata for all blobs in a given storage that have the provided name prefix.
func ListAllBlobs(ctx context.Context, st Reader, prefix ID) ([]Metadata, error) {
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
		//nolint:wrapcheck
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
func EnsureLengthExactly(gotLength int, length int64) error {
	if length < 0 {
		return nil
	}

	if gotLength != int(length) {
		return errors.Wrapf(ErrInvalidRange, "invalid length %v, expected %v", gotLength, length)
	}

	return nil
}

// IDsFromMetadata returns IDs for blobs in Metadata slice.
func IDsFromMetadata(mds []Metadata) []ID {
	ids := make([]ID, len(mds))

	for i, md := range mds {
		ids[i] = md.BlobID
	}

	return ids
}

// TotalLength returns minimum timestamp for blobs in Metadata slice.
func TotalLength(mds []Metadata) int64 {
	var total int64

	for _, md := range mds {
		total += md.Length
	}

	return total
}

// MinTimestamp returns minimum timestamp for blobs in Metadata slice.
func MinTimestamp(mds []Metadata) time.Time {
	minTime := time.Time{}

	for _, md := range mds {
		if minTime.IsZero() || md.Timestamp.Before(minTime) {
			minTime = md.Timestamp
		}
	}

	return minTime
}

// MaxTimestamp returns maximum timestamp for blobs in Metadata slice.
func MaxTimestamp(mds []Metadata) time.Time {
	maxTime := time.Time{}

	for _, md := range mds {
		if md.Timestamp.After(maxTime) {
			maxTime = md.Timestamp
		}
	}

	return maxTime
}

// DeleteMultiple deletes multiple blobs in parallel.
func DeleteMultiple(ctx context.Context, st Storage, ids []ID, parallelism int) error {
	eg, ctx := errgroup.WithContext(ctx)
	sem := make(chan struct{}, parallelism)

	for _, id := range ids {
		// acquire semaphore
		sem <- struct{}{}

		eg.Go(func() error {
			defer func() {
				<-sem // release semaphore
			}()

			return errors.Wrapf(st.DeleteBlob(ctx, id), "deleting %v", id)
		})
	}

	return errors.Wrap(eg.Wait(), "error deleting blobs")
}

// PutBlobAndGetMetadata invokes PutBlob and returns the resulting Metadata.
func PutBlobAndGetMetadata(ctx context.Context, st Storage, blobID ID, data Bytes, opts PutOptions) (Metadata, error) {
	// ensure GetModTime is set, or reuse existing one.
	if opts.GetModTime == nil {
		opts.GetModTime = new(time.Time)
	}

	err := st.PutBlob(ctx, blobID, data, opts)

	return Metadata{
		BlobID:    blobID,
		Length:    int64(data.Length()),
		Timestamp: *opts.GetModTime,
	}, err //nolint:wrapcheck
}

// ReadBlobMap reads the map of all the blobs indexed by ID.
func ReadBlobMap(ctx context.Context, br Reader) (map[ID]Metadata, error) {
	blobMap := map[ID]Metadata{}

	log(ctx).Info("Listing blobs...")

	if err := br.ListBlobs(ctx, "", func(bm Metadata) error {
		blobMap[bm.BlobID] = bm
		if len(blobMap)%10000 == 0 {
			log(ctx).Infof("  %v blobs...", len(blobMap))
		}
		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "unable to list blobs")
	}

	log(ctx).Infof("Listed %v blobs.", len(blobMap))

	return blobMap, nil
}
