package blobtesting

import (
	"bytes"
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/repo/blob"
)

var ErrBlobLocked = errors.New("cannot alter object before retention period expires") // +checklocksignore

type entry struct {
	value          []byte
	mtime          time.Time
	retentionTime  time.Time
	retentionMode  blob.RetentionMode
	isDeleteMarker bool
}

type versionedEntries map[blob.ID][]*entry

// objectLockingMap is an in-memory versioned object store which maintains
// historical versions of each blob on every put. Deletes use a delete-marker
// overlay mechanism and lists will avoid entries if their latest object is a
// marker. This struct manages the retention time of each blob through the
// PutBlob options.
type objectLockingMap struct {
	blob.DefaultProviderImplementation
	// +checklocks:mutex
	data    versionedEntries
	timeNow func() time.Time // +checklocksignore
	mutex   sync.RWMutex
}

// +checklocksread:s.mutex
func (s *objectLockingMap) getLatestByID(id blob.ID) (*entry, error) {
	versions, ok := s.data[id]
	if !ok {
		return nil, blob.ErrBlobNotFound
	}

	// get the latest version and if it is a delete marker then simulate
	// not-found
	e := versions[len(versions)-1]
	if e.isDeleteMarker {
		return nil, blob.ErrBlobNotFound
	}

	return e, nil
}

// +checklocksread:s.mutex
func (s *objectLockingMap) getLatestForMutationLocked(id blob.ID) (*entry, error) {
	e, err := s.getLatestByID(id)
	if err != nil {
		return nil, blob.ErrBlobNotFound
	}

	if !e.retentionTime.IsZero() && e.retentionTime.After(s.timeNow()) {
		return nil, errors.WithStack(ErrBlobLocked)
	}

	return e, nil
}

// GetBlob works the same as map-storage GetBlob except that if the latest
// version is a delete-marker then it will return ErrBlobNotFound.
func (s *objectLockingMap) GetBlob(ctx context.Context, id blob.ID, offset, length int64, output blob.OutputBuffer) error {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	e, err := s.getLatestByID(id)
	if err != nil {
		return err
	}

	output.Reset()

	data := e.value

	if length < 0 {
		if _, err := output.Write(data); err != nil {
			return errors.Wrap(err, "error writing data to output")
		}

		return nil
	}

	if int(offset) > len(data) || offset < 0 {
		return errors.Wrapf(blob.ErrInvalidRange, "invalid offset: %v", offset)
	}

	data = data[offset:]
	if int(length) > len(data) {
		return errors.Wrapf(blob.ErrInvalidRange, "invalid length: %v", length)
	}

	if _, err := output.Write(data[0:length]); err != nil {
		return errors.Wrap(err, "error writing data to output")
	}

	return nil
}

// GetMetadata works the same as map-storage GetMetadata except that if the latest
// version is a delete-marker then it will return ErrBlobNotFound.
func (s *objectLockingMap) GetMetadata(ctx context.Context, id blob.ID) (blob.Metadata, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	e, err := s.getLatestByID(id)
	if err != nil {
		return blob.Metadata{}, err
	}

	return blob.Metadata{
		BlobID:    id,
		Length:    int64(len(e.value)),
		Timestamp: e.mtime,
	}, nil
}

func (s *objectLockingMap) GetRetention(ctx context.Context, id blob.ID) (blob.RetentionMode, time.Time, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	e, err := s.getLatestByID(id)
	if err != nil {
		return "", time.Time{}, errors.Wrap(err, "getting blob")
	}

	return e.retentionMode, e.retentionTime, nil
}

// PutBlob works the same as map-storage PutBlob except that if the latest
// version is a delete-marker then it will return ErrBlobNotFound. The
// PutOptions retention parameters will be respected when storing the object.
func (s *objectLockingMap) PutBlob(ctx context.Context, id blob.ID, data blob.Bytes, opts blob.PutOptions) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	var b bytes.Buffer
	if _, err := data.WriteTo(&b); err != nil {
		return err
	}

	e := &entry{
		value: b.Bytes(),
	}

	if opts.SetModTime.IsZero() {
		e.mtime = s.timeNow()
	} else {
		e.mtime = opts.SetModTime
	}

	if opts.HasRetentionOptions() {
		e.retentionTime = e.mtime.Add(opts.RetentionPeriod)
		e.retentionMode = opts.RetentionMode
	}

	s.data[id] = append(s.data[id], e)

	if opts.GetModTime != nil {
		*opts.GetModTime = e.mtime
	}

	return nil
}

// DeleteBlob will insert a delete marker after the last version of the object.
// If the object does not exist then this becomes a no-op.
func (s *objectLockingMap) DeleteBlob(ctx context.Context, id blob.ID) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// prevent adding a delete marker when latest is already a marker or
	// an entry for the blob does not exist
	if _, err := s.getLatestByID(id); err != nil {
		// no error if already deleted
		if errors.Is(err, blob.ErrBlobNotFound) {
			return nil
		}

		return err
	}

	s.data[id] = append(s.data[id], &entry{
		mtime:          s.timeNow(),
		isDeleteMarker: true,
	})

	return nil
}

// ExtendBlobRetention will alter the retention time on a blob if it exists.
func (s *objectLockingMap) ExtendBlobRetention(ctx context.Context, id blob.ID, opts blob.ExtendOptions) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	e, err := s.getLatestByID(id)
	if err != nil {
		return blob.ErrBlobNotFound
	}

	// Update the retention time from now to the given retention period in the
	// future. Note that we do not bump the existing time on the element `e.mtime`
	// by the given delta because we'd like to align with the S3 storage's current
	// time and the S3 storage code extends retention periods based off the
	// current time, not object mod time.
	if !e.retentionTime.IsZero() {
		e.retentionTime = s.timeNow().Add(opts.RetentionPeriod)
	}

	return nil
}

// ListBlobs will return the list of all the objects except the ones which have
// a delete-marker as their latest version.
func (s *objectLockingMap) ListBlobs(ctx context.Context, prefix blob.ID, callback func(blob.Metadata) error) error {
	s.mutex.RLock()

	keys := []blob.ID{}

	for k := range s.data {
		if strings.HasPrefix(string(k), string(prefix)) {
			keys = append(keys, k)
		}
	}

	s.mutex.RUnlock()

	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	for _, k := range keys {
		m, err := s.GetMetadata(ctx, k)
		if err != nil {
			if errors.Is(err, blob.ErrBlobNotFound) {
				continue
			}

			return err
		}

		if err := callback(m); err != nil {
			return err
		}
	}

	return nil
}

// TouchBlob updates the mtime of the latest version of the object. If it is a
// delete-marker or if it does not exist then this becomes a no-op. If the
// latest version has retention parameters set then they are respected.
// Mutations are no allowed unless retention period expires.
func (s *objectLockingMap) TouchBlob(ctx context.Context, id blob.ID, threshold time.Duration) (time.Time, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	e, err := s.getLatestForMutationLocked(id)
	if err != nil {
		// no error if delete-marker or not-exists, prevent changing mtime
		// of delete-markers
		if errors.Is(err, blob.ErrBlobNotFound) {
			return time.Time{}, nil
		}

		return time.Time{}, err
	}

	n := s.timeNow()
	if n.Sub(e.mtime) >= threshold {
		e.mtime = n
	}

	return e.mtime, nil
}

// ConnectionInfo is a no-op.
func (s *objectLockingMap) ConnectionInfo() blob.ConnectionInfo {
	// unsupported
	return blob.ConnectionInfo{}
}

// DisplayName gets the identifier of this storage for display purposes.
func (s *objectLockingMap) DisplayName() string {
	return "VersionedMap"
}

// NewVersionedMapStorage returns an implementation of Storage backed by the
// contents of an internal in-memory map used primarily for testing.
func NewVersionedMapStorage(timeNow func() time.Time) RetentionStorage {
	if timeNow == nil {
		timeNow = clock.Now
	}

	return &objectLockingMap{data: make(versionedEntries), timeNow: timeNow}
}
