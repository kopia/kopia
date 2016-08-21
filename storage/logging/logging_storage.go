// Package logging implements wrapper around Storage that logs all activity.
package logging

import (
	"log"

	"github.com/kopia/kopia/storage"
)

// Printfer supports emitting formatted strings to arbitrary outputs.
// The formatting language must be compatible with fmt.Sprintf().
type Printfer interface {
	Printf(fmt string, args ...interface{})
}

type loggingStorage struct {
	base   storage.Storage
	printf func(string, ...interface{})
}

func (s *loggingStorage) BlockExists(id string) (bool, error) {
	result, err := s.base.BlockExists(id)
	s.printf("BlockExists(%#v)=%#v,%#v", id, result, err)
	return result, err
}

func (s *loggingStorage) GetBlock(id string) ([]byte, error) {
	result, err := s.base.GetBlock(id)
	if len(result) < 20 {
		s.printf("GetBlock(%#v)=(%#v, %#v)", id, result, err)
	} else {
		s.printf("GetBlock(%#v)=({%#v bytes}, %#v)", id, len(result), err)
	}
	return result, err
}

func (s *loggingStorage) PutBlock(id string, data storage.ReaderWithLength, options storage.PutOptions) error {
	l := data.Len()
	err := s.base.PutBlock(id, data, options)
	s.printf("PutBlock(%#v, options=%v, len=%v)=%#v", id, options, l, err)
	return err
}

func (s *loggingStorage) DeleteBlock(id string) error {
	err := s.base.DeleteBlock(id)
	s.printf("DeleteBlock(%#v)=%#v", id, err)
	return err
}

func (s *loggingStorage) ListBlocks(prefix string) chan (storage.BlockMetadata) {
	s.printf("ListBlocks(%#v)", prefix)
	return s.base.ListBlocks(prefix)
}

func (s *loggingStorage) Flush() error {
	s.printf("Flush()")
	return s.base.Flush()
}

func (s *loggingStorage) Close() error {
	s.printf("Close()")
	return s.base.Close()
}

// NewWrapper returns a Storage wrapper that logs all storage commands.
func NewWrapper(wrapped storage.Storage) storage.Storage {
	return &loggingStorage{base: wrapped, printf: log.Printf}
}

// NewWrapperWithLogger returns a Storage wrapper that logs all storage commands to the specified logger.
func NewWrapperWithLogger(wrapped storage.Storage, printfer Printfer) storage.Storage {
	return &loggingStorage{base: wrapped, printf: printfer.Printf}
}
