package block

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/kopia/kopia/internal/packindex"
	"github.com/kopia/kopia/storage"
	"github.com/rs/zerolog/log"
	"golang.org/x/exp/mmap"
)

const simpleIndexSuffix = ".sndx"

type simpleCommittedBlockIndex struct {
	dirname      string
	indexesMutex sync.Mutex
	indexBlocks  map[PhysicalBlockID]bool
	merged       packindex.Merged
}

func (b *simpleCommittedBlockIndex) getBlock(blockID string) (Info, error) {
	info, err := b.merged.GetInfo(blockID)
	if info != nil {
		return *info, nil
	}
	if err == nil {
		return Info{}, storage.ErrBlockNotFound
	}
	return Info{}, err
}

func (b *simpleCommittedBlockIndex) hasIndexBlockID(indexBlockID PhysicalBlockID) (bool, error) {
	return b.indexBlocks[indexBlockID], nil
}

func (b *simpleCommittedBlockIndex) commit(indexBlockID PhysicalBlockID, builder packindex.Builder) error {
	fullPath := filepath.Join(b.dirname, string(indexBlockID+simpleIndexSuffix))

	w, ferr := os.Create(fullPath)
	if ferr != nil {
		return ferr
	}

	if err := builder.Build(w); err != nil {
		w.Close() //nolint:errcheck
		return fmt.Errorf("unable to build pack: %v", err)
	}

	if err := w.Close(); err != nil {
		return fmt.Errorf("close error: %v", err)
	}

	var ndx packindex.Index
	var err error
	if ndx, err = b.openIndex(fullPath); err != nil {
		return fmt.Errorf("unable to open pack: %v", err)
	}

	b.indexesMutex.Lock()
	b.indexBlocks[indexBlockID] = true
	b.merged = append(b.merged, ndx)
	b.indexesMutex.Unlock()

	return nil
}

func (b *simpleCommittedBlockIndex) load(indexBlockID PhysicalBlockID, data []byte) (int, error) {
	fullPath := filepath.Join(b.dirname, string(indexBlockID+simpleIndexSuffix))

	if err := ioutil.WriteFile(fullPath, data, 0600); err != nil {
		return 0, err
	}

	ndx, err := b.openIndex(fullPath)
	if err != nil {
		return 0, fmt.Errorf("unable to open pack index %q: %v", fullPath, err)
	}

	b.indexesMutex.Lock()
	b.indexBlocks[indexBlockID] = true
	b.merged = append(b.merged, ndx)
	b.indexesMutex.Unlock()

	return 0, nil
}

func (b *simpleCommittedBlockIndex) listBlocks(prefix string, cb func(i Info) error) error {
	return b.merged.Iterate(prefix, cb)
}

func (b *simpleCommittedBlockIndex) loadIndexes() error {
	b.indexesMutex.Lock()
	defer b.indexesMutex.Unlock()

	entries, err := ioutil.ReadDir(b.dirname)
	if err != nil {
		return err
	}

	newIndexes := map[PhysicalBlockID]bool{}
	var newMerged packindex.Merged
	defer func() {
		newMerged.Close() //nolint:errcheck
	}()
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), simpleIndexSuffix) {
			continue
		}

		fullpath := filepath.Join(b.dirname, e.Name())
		ndx, err := b.openIndex(fullpath)
		if err != nil {
			return fmt.Errorf("unable to open pack index %q: %v", fullpath, err)
		}

		log.Printf("opened %v with %v entries", fullpath, ndx.EntryCount())

		// ndx.Iterate("", func(i Info) error {
		// 	log.Info().Msgf("i: %v blk:%v off:%v len:%v", i.BlockID, i.PackBlockID, i.PackOffset, i.Length)
		// 	return nil
		// })

		newIndexes[PhysicalBlockID(strings.TrimSuffix(e.Name(), simpleIndexSuffix))] = true
		newMerged = append(newMerged, ndx)
	}
	b.indexBlocks = newIndexes
	b.merged = newMerged
	newMerged = nil
	return nil
}

func (b *simpleCommittedBlockIndex) openIndex(fullpath string) (packindex.Index, error) {
	f, err := mmap.Open(fullpath)
	//f, err := os.Open(fullpath)
	if err != nil {
		return nil, err
	}

	return packindex.Open(f)
}

func newSimpleCommittedBlockIndex(dirname string) (committedBlockIndex, error) {
	_ = os.MkdirAll(dirname, 0700)

	s := &simpleCommittedBlockIndex{dirname: dirname}
	if err := s.loadIndexes(); err != nil {
		return nil, err
	}
	return s, nil
}
