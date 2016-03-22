package storage

import (
	"io"
	"log"
)

type loggingRepository struct {
	Repository
}

func (s *loggingRepository) BlockExists(id BlockID) (bool, error) {
	result, err := s.Repository.BlockExists(id)
	log.Printf("BlockExists(%#v)=%#v,%#v", id, result, err)
	return result, err
}

func (s *loggingRepository) GetBlock(id BlockID) ([]byte, error) {
	result, err := s.Repository.GetBlock(id)
	if len(result) < 20 {
		log.Printf("GetBlock(%#v)=(%#v, %#v)", id, result, err)
	} else {
		log.Printf("GetBlock(%#v)=({%#v bytes}, %#v)", id, len(result), err)
	}
	return result, err
}

func (s *loggingRepository) PutBlock(id BlockID, data io.ReadCloser, options PutOptions) error {
	err := s.Repository.PutBlock(id, data, options)
	log.Printf("PutBlock(%#v, %#v)=%#v", id, options, err)
	return err
}

func (s *loggingRepository) DeleteBlock(id BlockID) error {
	err := s.Repository.DeleteBlock(id)
	log.Printf("DeleteBlock(%#v)=%#v", id, err)
	return err
}

func (s *loggingRepository) ListBlocks(prefix BlockID) chan (BlockMetadata) {
	log.Printf("ListBlocks(%#v)", prefix)
	return s.Repository.ListBlocks(prefix)
}

func (s *loggingRepository) Flush() error {
	log.Printf("Flush()")
	return s.Repository.Flush()
}

// NewLoggingWrapper returns a Repository wrapper that logs all repository commands.
func NewLoggingWrapper(wrapped Repository) Repository {
	return &loggingRepository{wrapped}
}
