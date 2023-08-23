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

// DataMap is a map of blob ID to their contents.
type DataMap map[blob.ID][]byte

type mapStorage struct {
	blob.DefaultProviderImplementation
	// +checklocks:mutex
	data DataMap
	// +checklocks:mutex
	keyTime map[blob.ID]time.Time
	// +checklocks:mutex
	timeNow func() time.Time
	mutex   sync.RWMutex
}

func (s *mapStorage) GetBlob(ctx context.Context, id blob.ID, offset, length int64, output blob.OutputBuffer) error {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	output.Reset()

	data, ok := s.data[id]
	if !ok {
		return blob.ErrBlobNotFound
	}

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

func (s *mapStorage) GetMetadata(ctx context.Context, id blob.ID) (blob.Metadata, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	data, ok := s.data[id]
	if ok {
		return blob.Metadata{
			BlobID:    id,
			Length:    int64(len(data)),
			Timestamp: s.keyTime[id],
		}, nil
	}

	return blob.Metadata{}, blob.ErrBlobNotFound
}

func (s *mapStorage) PutBlob(ctx context.Context, id blob.ID, data blob.Bytes, opts blob.PutOptions) error {
	switch {
	case opts.HasRetentionOptions():
		return errors.Wrap(blob.ErrUnsupportedPutBlobOption, "blob-retention")
	case opts.DoNotRecreate:
		return errors.Wrap(blob.ErrUnsupportedPutBlobOption, "do-not-recreate")
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if !opts.SetModTime.IsZero() {
		s.keyTime[id] = opts.SetModTime
	} else {
		s.keyTime[id] = s.timeNow()
	}

	var b bytes.Buffer

	data.WriteTo(&b)

	s.data[id] = b.Bytes()

	if opts.GetModTime != nil {
		*opts.GetModTime = s.keyTime[id]
	}

	return nil
}

func (s *mapStorage) DeleteBlob(ctx context.Context, id blob.ID) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	delete(s.data, id)
	delete(s.keyTime, id)

	return nil
}

func (s *mapStorage) ListBlobs(ctx context.Context, prefix blob.ID, callback func(blob.Metadata) error) error {
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
		s.mutex.RLock()
		v, ok := s.data[k]
		ts := s.keyTime[k]
		s.mutex.RUnlock()

		if !ok {
			continue
		}

		if err := callback(blob.Metadata{
			BlobID:    k,
			Length:    int64(len(v)),
			Timestamp: ts,
		}); err != nil {
			return err
		}
	}

	return nil
}

func (s *mapStorage) TouchBlob(ctx context.Context, blobID blob.ID, threshold time.Duration) (time.Time, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if v, ok := s.keyTime[blobID]; ok {
		n := s.timeNow()
		if n.Sub(v) >= threshold {
			s.keyTime[blobID] = n
		}
	}

	return s.keyTime[blobID], nil
}

func (s *mapStorage) ConnectionInfo() blob.ConnectionInfo {
	// unsupported
	return blob.ConnectionInfo{}
}

func (s *mapStorage) DisplayName() string {
	return "Map"
}

// NewMapStorage returns an implementation of Storage backed by the contents of given map.
// Used primarily for testing.
func NewMapStorage(data DataMap, keyTime map[blob.ID]time.Time, timeNow func() time.Time) blob.Storage {
	if keyTime == nil {
		keyTime = make(map[blob.ID]time.Time)
	}

	if timeNow == nil {
		timeNow = clock.Now
	}

	return &mapStorage{data: data, keyTime: keyTime, timeNow: timeNow}
}
