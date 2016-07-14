package logging

import (
	"io"
	"log"

	"github.com/kopia/kopia/storage"
)

type loggingStorage struct {
	storage.Storage
}

func (s *loggingStorage) BlockExists(id string) (bool, error) {
	result, err := s.Storage.BlockExists(id)
	log.Printf("BlockExists(%#v)=%#v,%#v", id, result, err)
	return result, err
}

func (s *loggingStorage) GetBlock(id string) ([]byte, error) {
	result, err := s.Storage.GetBlock(id)
	if len(result) < 20 {
		log.Printf("GetBlock(%#v)=(%#v, %#v)", id, result, err)
	} else {
		log.Printf("GetBlock(%#v)=({%#v bytes}, %#v)", id, len(result), err)
	}
	return result, err
}

func (s *loggingStorage) PutBlock(id string, data storage.ReaderWithLength, options storage.PutOptions) error {
	err := s.Storage.PutBlock(id, data, options)
	log.Printf("PutBlock(%#v, options=%v, len=%v)=%#v", id, options, data.Len(), err)
	return err
}

func (s *loggingStorage) DeleteBlock(id string) error {
	err := s.Storage.DeleteBlock(id)
	log.Printf("DeleteBlock(%#v)=%#v", id, err)
	return err
}

func (s *loggingStorage) ListBlocks(prefix string) chan (storage.BlockMetadata) {
	log.Printf("ListBlocks(%#v)", prefix)
	return s.Storage.ListBlocks(prefix)
}

func (s *loggingStorage) Flush() error {
	if s, ok := s.Storage.(storage.Flusher); ok {
		log.Printf("Flush()")
		return s.Flush()
	}

	return nil
}

func (s *loggingStorage) Close() error {
	if c, ok := s.Storage.(io.Closer); ok {
		log.Printf("Close()")
		return c.Close()
	}

	return nil
}

// NewWrapper returns a Storage wrapper that logs all storage commands.
func NewWrapper(wrapped storage.Storage) storage.Storage {
	return &loggingStorage{wrapped}
}
