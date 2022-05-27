package overlay

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
	value          []byte // TODO: thsi can be a link to local storage
	mtime          time.Time
	retentionTime  time.Time
	isDeleteMarker bool
}

type overlayData map[blob.ID][]*entry

// overlayObjLockingMap is an in-memory versioned object store which maintains
// historical versions of each blob on every put. Deletes use a delete-marker
// overlay mechanism and lists will avoid entries if their latest object is a
// marker. This struct manages the retention time of each blob throug hte
// PutBlob options.
type overlayObjLockingMap struct {
	blob.Storage

	// +checklocks:mutex
	data    overlayData
	timeNow func() time.Time // +checklocksignore
	mutex   sync.RWMutex
}

// +checklocksread:s.mutex
func (s *overlayObjLockingMap) getLatestByID(id blob.ID) (*entry, error) {
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
func (s *overlayObjLockingMap) getLatestForMutationLocked(id blob.ID) (*entry, error) {
	e, err := s.getLatestByID(id)
	if err != nil {
		return nil, blob.ErrBlobNotFound
	}

	if !e.retentionTime.IsZero() && e.retentionTime.After(s.timeNow()) {
		return nil, errors.New("cannot alter object before retention period expires")
	}

	return e, nil
}

// GetBlob works the same as map-storage GetBlob except that if the latest
// version is a delete-marker then it will return ErrBlobNotFound.
func (s *overlayObjLockingMap) GetBlob(ctx context.Context, id blob.ID, offset, length int64, output blob.OutputBuffer) error {
	s.mutex.RLock()

	if _, ok := s.data[id]; !ok {
		s.mutex.RUnlock()
		return s.Storage.GetBlob(ctx, id, offset, length, output)
	}

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
func (s *overlayObjLockingMap) GetMetadata(ctx context.Context, id blob.ID) (blob.Metadata, error) {
	s.mutex.RLock()

	if _, ok := s.data[id]; !ok {
		s.mutex.RUnlock()
		return s.Storage.GetMetadata(ctx, id)
	}

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

// PutBlob works the same as map-storage PutBlob except that if the latest
// version is a delete-marker then it will return ErrBlobNotFound. The
// PutOptions retention parameters will be respected when storing the object.
func (s *overlayObjLockingMap) PutBlob(ctx context.Context, id blob.ID, data blob.Bytes, opts blob.PutOptions) error {
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

// DeleteBlob will insert a delete marker after the last version of the object.
// If the object does not exist then this becomes a no-op.
func (s *overlayObjLockingMap) DeleteBlob(ctx context.Context, id blob.ID) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// prevent adding a delete marker when latest is already a marker or
	// an entry for the blob does not exist
	if versions, _ := s.data[id]; len(versions) > 0 {
		// get the latest version and if it is a delete marker then simulate
		// not-found
		e := versions[len(versions)-1]
		if e.isDeleteMarker {
			return blob.ErrBlobNotFound
		}
	}

	s.data[id] = append(s.data[id], &entry{
		mtime:          s.timeNow(),
		isDeleteMarker: true,
	})

	return nil
}

// ListBlobs will return the list of all the objects except the ones which have
// a delete-marker as their latest version.
func (s *overlayObjLockingMap) ListBlobs(ctx context.Context, prefix blob.ID, callback func(blob.Metadata) error) error {
	s.mutex.RLock()

	overlayKeys := []blob.ID{}

	for k := range s.data {
		if strings.HasPrefix(string(k), string(prefix)) {
			overlayKeys = append(overlayKeys, k)
		}
	}

	s.mutex.RUnlock()

	sort.Slice(overlayKeys, func(i, j int) bool {
		return overlayKeys[i] < overlayKeys[j]
	})

	next := 0
	return s.Storage.ListBlobs(ctx, prefix, func(bm blob.Metadata) error {
		for next < len(overlayKeys) {
			if bm.BlobID < overlayKeys[next] {
				break
			}

			overlayBM, err := s.GetMetadata(ctx, overlayKeys[next])
			if err != nil {
				if errors.Is(err, blob.ErrBlobNotFound) {
					next++
					continue
				}

				return err
			}

			err = callback(overlayBM)
			next++

			// if we have already processed the current blob from theunderlying
			// storage then skip everything else
			if err != nil || bm.BlobID == overlayKeys[next-1] {
				return err
			}
		}

		return callback(bm)
	})
}

func (s *overlayObjLockingMap) ConnectionInfo() blob.ConnectionInfo {
	return s.Storage.ConnectionInfo()
}

// DisplayName gets the identifier of this storage for display purposes.
func (s *overlayObjLockingMap) DisplayName() string {
	return "OverlayObjLockingMap"
}

// NewOverlayObjLockingStorage returns an implementation of Storage backed by the
// contents of an internal in-memory map used primarily for testing.
func NewOverlayObjLockingStorage(wrapped blob.Storage, timeNow func() time.Time) blob.Storage {
	if timeNow == nil {
		timeNow = clock.Now
	}

	return &overlayObjLockingMap{Storage: wrapped, data: make(overlayData), timeNow: timeNow}
}
