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

type entry struct {
	value          []byte
	mtime          time.Time
	retentionTime  time.Time
	isDeleteMarker bool
}

type versionedEntries map[blob.ID][]*entry

type objectLockingMap struct {
	data    versionedEntries
	timeNow func() time.Time
	mutex   sync.RWMutex
}

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

func (s *objectLockingMap) getLatestForMutationLocked(id blob.ID) (*entry, error) {
	e, err := s.getLatestByID(id)
	if err != nil {
		return nil, blob.ErrBlobNotFound
	}

	if !e.retentionTime.IsZero() && e.retentionTime.After(s.timeNow()) {
		return nil, errors.New("cannot alter object before retention period expires")
	}

	return e, nil
}

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
	}

	s.data[id] = append(s.data[id], e)

	if opts.GetModTime != nil {
		*opts.GetModTime = e.mtime
	}

	return nil
}

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

func (s *objectLockingMap) Close(ctx context.Context) error {
	return nil
}

func (s *objectLockingMap) TouchBlob(ctx context.Context, id blob.ID, threshold time.Duration) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	e, err := s.getLatestForMutationLocked(id)
	if err != nil {
		// no error if delete-marker or not-exists, prevent changing mtime
		// of delete-markers
		if errors.Is(err, blob.ErrBlobNotFound) {
			return nil
		}

		return err
	}

	n := s.timeNow()
	if n.Sub(e.mtime) >= threshold {
		e.mtime = n
	}

	return nil
}

func (s *objectLockingMap) ConnectionInfo() blob.ConnectionInfo {
	// unsupported
	return blob.ConnectionInfo{}
}

func (s *objectLockingMap) DisplayName() string {
	return "VersionedMap"
}

func (s *objectLockingMap) FlushCaches(ctx context.Context) error {
	return nil
}

// NewVersionedMapStorage returns an implementation of Storage backed by the
// contents of an internal in-memory map used primarily for testing.
func NewVersionedMapStorage(timeNow func() time.Time) blob.Storage {
	if timeNow == nil {
		timeNow = clock.Now
	}

	return &objectLockingMap{data: make(versionedEntries), timeNow: timeNow}
}
