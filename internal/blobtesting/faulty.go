package blobtesting

import (
	"context"
	"sync"
	"time"

	"github.com/kopia/kopia/internal/repologging"
	"github.com/kopia/kopia/repo/blob"
)

var log = repologging.Logger("faulty-storage")

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
	if err := s.getNextFault("GetBlob", id, offset, length); err != nil {
		return nil, err
	}
	return s.Base.GetBlob(ctx, id, offset, length)
}

// PutBlob implements blob.Storage
func (s *FaultyStorage) PutBlob(ctx context.Context, id blob.ID, data []byte) error {
	if err := s.getNextFault("PutBlob", id, len(data)); err != nil {
		return err
	}
	return s.Base.PutBlob(ctx, id, data)
}

// DeleteBlob implements blob.Storage
func (s *FaultyStorage) DeleteBlob(ctx context.Context, id blob.ID) error {
	if err := s.getNextFault("DeleteBlob", id); err != nil {
		return err
	}
	return s.Base.DeleteBlob(ctx, id)
}

// ListBlobs implements blob.Storage
func (s *FaultyStorage) ListBlobs(ctx context.Context, prefix blob.ID, callback func(blob.Metadata) error) error {
	if err := s.getNextFault("ListBlobs", prefix); err != nil {
		return err
	}

	return s.Base.ListBlobs(ctx, prefix, func(bm blob.Metadata) error {
		if err := s.getNextFault("ListBlobsItem", prefix); err != nil {
			return err
		}
		return callback(bm)
	})
}

// Close implements blob.Storage
func (s *FaultyStorage) Close(ctx context.Context) error {
	if err := s.getNextFault("Close"); err != nil {
		return err
	}
	return s.Base.Close(ctx)
}

// ConnectionInfo implements blob.Storage
func (s *FaultyStorage) ConnectionInfo() blob.ConnectionInfo {
	return s.Base.ConnectionInfo()
}

func (s *FaultyStorage) getNextFault(method string, args ...interface{}) error {
	s.mu.Lock()
	faults := s.Faults[method]
	if len(faults) == 0 {
		s.mu.Unlock()
		log.Debugf("no faults for %v %v", method, args)
		return nil
	}

	f := faults[0]
	if f.Repeat > 0 {
		f.Repeat--
		log.Debugf("will repeat %v more times the fault for %v %v", f.Repeat, method, args)
	} else {
		s.Faults[method] = faults[1:]
	}
	s.mu.Unlock()
	if f.WaitFor != nil {
		log.Debugf("waiting for channel to be closed in %v %v", method, args)
		<-f.WaitFor
	}
	if f.Sleep > 0 {
		log.Debugf("sleeping for %v in %v %v", f.Sleep, method, args)
		time.Sleep(f.Sleep)
	}
	if f.ErrCallback != nil {
		err := f.ErrCallback()
		log.Debugf("returning %v for %v %v", err, method, args)
		return err
	}
	log.Debugf("returning %v for %v %v", f.Err, method, args)
	return f.Err
}

var _ blob.Storage = (*FaultyStorage)(nil)
