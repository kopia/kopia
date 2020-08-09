package blobtesting

import (
	"context"
	"sync"
	"time"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/logging"
)

var log = logging.GetContextLoggerFunc("faulty-storage")

// Fault describes the behavior of a single fault.
type Fault struct {
	Repeat      int           // how many times to repeat this fault
	Sleep       time.Duration // sleep before returning
	ErrCallback func() error
	WaitFor     chan struct{} // waits until the given channel is closed before returning
	Err         error         // error to return (can be nil in combination with Sleep and WaitFor)
}

// FaultyStorage implements fault injection for Storage.
type FaultyStorage struct {
	Base   blob.Storage
	Faults map[string][]*Fault

	mu sync.Mutex
}

// GetBlob implements blob.Storage
func (s *FaultyStorage) GetBlob(ctx context.Context, id blob.ID, offset, length int64) ([]byte, error) {
	if err := s.getNextFault(ctx, "GetBlob", id, offset, length); err != nil {
		return nil, err
	}

	return s.Base.GetBlob(ctx, id, offset, length)
}

// GetMetadata implements blob.Storage
func (s *FaultyStorage) GetMetadata(ctx context.Context, id blob.ID) (blob.Metadata, error) {
	if err := s.getNextFault(ctx, "GetMetadata", id); err != nil {
		return blob.Metadata{}, err
	}

	return s.Base.GetMetadata(ctx, id)
}

// PutBlob implements blob.Storage
func (s *FaultyStorage) PutBlob(ctx context.Context, id blob.ID, data blob.Bytes) error {
	if err := s.getNextFault(ctx, "PutBlob", id); err != nil {
		return err
	}

	return s.Base.PutBlob(ctx, id, data)
}

// DeleteBlob implements blob.Storage
func (s *FaultyStorage) DeleteBlob(ctx context.Context, id blob.ID) error {
	if err := s.getNextFault(ctx, "DeleteBlob", id); err != nil {
		return err
	}

	return s.Base.DeleteBlob(ctx, id)
}

// ListBlobs implements blob.Storage
func (s *FaultyStorage) ListBlobs(ctx context.Context, prefix blob.ID, callback func(blob.Metadata) error) error {
	if err := s.getNextFault(ctx, "ListBlobs", prefix); err != nil {
		return err
	}

	return s.Base.ListBlobs(ctx, prefix, func(bm blob.Metadata) error {
		if err := s.getNextFault(ctx, "ListBlobsItem", prefix); err != nil {
			return err
		}
		return callback(bm)
	})
}

// Close implements blob.Storage
func (s *FaultyStorage) Close(ctx context.Context) error {
	if err := s.getNextFault(ctx, "Close"); err != nil {
		return err
	}

	return s.Base.Close(ctx)
}

// ConnectionInfo implements blob.Storage
func (s *FaultyStorage) ConnectionInfo() blob.ConnectionInfo {
	return s.Base.ConnectionInfo()
}

// DisplayName implements blob.Storage
func (s *FaultyStorage) DisplayName() string {
	return s.Base.DisplayName()
}

func (s *FaultyStorage) getNextFault(ctx context.Context, method string, args ...interface{}) error {
	s.mu.Lock()

	faults := s.Faults[method]
	if len(faults) == 0 {
		s.mu.Unlock()
		log(ctx).Debugf("no faults for %v %v", method, args)

		return nil
	}

	f := faults[0]
	if f.Repeat > 0 {
		f.Repeat--
		log(ctx).Debugf("will repeat %v more times the fault for %v %v", f.Repeat, method, args)
	} else {
		s.Faults[method] = faults[1:]
	}

	s.mu.Unlock()

	if f.WaitFor != nil {
		log(ctx).Debugf("waiting for channel to be closed in %v %v", method, args)
		<-f.WaitFor
	}

	if f.Sleep > 0 {
		log(ctx).Debugf("sleeping for %v in %v %v", f.Sleep, method, args)
		time.Sleep(f.Sleep)
	}

	if f.ErrCallback != nil {
		err := f.ErrCallback()
		log(ctx).Debugf("returning %v for %v %v", err, method, args)

		return err
	}

	log(ctx).Debugf("returning %v for %v %v", f.Err, method, args)

	return f.Err
}

var _ blob.Storage = (*FaultyStorage)(nil)
