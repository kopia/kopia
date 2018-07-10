package storagetesting

import (
	"context"
	"sync"
	"time"

	"github.com/kopia/kopia/internal/kopialogging"
	"github.com/kopia/kopia/storage"
)

var log = kopialogging.Logger("faulty-storage")

type Fault struct {
	Repeat  int
	Sleep   time.Duration
	WaitFor chan struct{} // waits until the given channel is closed before returning
	Err     error
}

type FaultyStorage struct {
	Base   storage.Storage
	Faults map[string][]*Fault

	mu sync.Mutex
}

func (s *FaultyStorage) GetBlock(ctx context.Context, id string, offset, length int64) ([]byte, error) {
	if err := s.getNextFault("GetBlock", id, offset, length); err != nil {
		return nil, err
	}
	return s.Base.GetBlock(ctx, id, offset, length)
}

func (s *FaultyStorage) PutBlock(ctx context.Context, id string, data []byte) error {
	if err := s.getNextFault("PutBlock", id, len(data)); err != nil {
		return err
	}
	return s.Base.PutBlock(ctx, id, data)
}

func (s *FaultyStorage) DeleteBlock(ctx context.Context, id string) error {
	if err := s.getNextFault("DeleteBlock", id); err != nil {
		return err
	}
	return s.Base.DeleteBlock(ctx, id)
}

func (s *FaultyStorage) ListBlocks(ctx context.Context, prefix string, callback func(storage.BlockMetadata) error) error {
	if err := s.getNextFault("ListBlocks", prefix); err != nil {
		return err
	}

	return s.Base.ListBlocks(ctx, prefix, func(bm storage.BlockMetadata) error {
		if err := s.getNextFault("ListBlocksItem", prefix); err != nil {
			return err
		}
		return callback(bm)
	})
}

func (s *FaultyStorage) Close(ctx context.Context) error {
	if err := s.getNextFault("Close"); err != nil {
		return err
	}
	return s.Base.Close(ctx)
}

func (s *FaultyStorage) ConnectionInfo() storage.ConnectionInfo {
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
	}
	log.Debugf("returning %v for %v %v", f.Err, method, args)
	return f.Err
}

var _ storage.Storage = (*FaultyStorage)(nil)
