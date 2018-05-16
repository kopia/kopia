package block

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/kopia/kopia/internal/packindex"
	"github.com/kopia/kopia/storage"
	"github.com/rs/zerolog/log"
	"golang.org/x/exp/mmap"
)

const simpleIndexSuffix = ".sndx"

type simpleCommittedBlockIndex struct {
	dirname string

	mu          sync.Mutex
	indexBlocks map[PhysicalBlockID]bool
	merged      packindex.Merged
}

func (b *simpleCommittedBlockIndex) getBlock(blockID string) (Info, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

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
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.indexBlocks[indexBlockID], nil
}

func (b *simpleCommittedBlockIndex) addBlock(indexBlockID PhysicalBlockID, data []byte, use bool) error {
	fullPath := filepath.Join(b.dirname, string(indexBlockID+simpleIndexSuffix))

	if err := ioutil.WriteFile(fullPath, data, 0600); err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	b.indexBlocks[indexBlockID] = true

	if !use {
		return nil
	}

	ndx, err := b.openIndex(fullPath)
	if err != nil {
		return fmt.Errorf("unable to open pack index %q: %v", fullPath, err)
	}

	b.merged = append(b.merged, ndx)

	return nil
}

func (b *simpleCommittedBlockIndex) listBlocks(prefix string, cb func(i Info) error) error {
	b.mu.Lock()
	m := b.merged
	b.mu.Unlock()

	return m.Iterate(prefix, cb)
}

func (b *simpleCommittedBlockIndex) openIndex(fullpath string) (packindex.Index, error) {
	f, err := mmap.Open(fullpath)
	//f, err := os.Open(fullpath)
	if err != nil {
		return nil, err
	}

	return packindex.Open(f)
}

func (b *simpleCommittedBlockIndex) use(packBlockIDs []PhysicalBlockID) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	newIndexes := map[PhysicalBlockID]bool{}
	var newMerged packindex.Merged
	defer func() {
		newMerged.Close() //nolint:errcheck
	}()
	for _, e := range packBlockIDs {
		fullpath := filepath.Join(b.dirname, string(e)+simpleIndexSuffix)
		ndx, err := b.openIndex(fullpath)
		if err != nil {
			return fmt.Errorf("unable to open pack index %q: %v", fullpath, err)
		}

		log.Printf("opened %v with %v entries", fullpath, ndx.EntryCount())
		newIndexes[e] = true
		newMerged = append(newMerged, ndx)
	}
	b.indexBlocks = newIndexes
	b.merged = newMerged
	newMerged = nil
	return nil
}

func newSimpleCommittedBlockIndex(dirname string) (committedBlockIndex, error) {
	_ = os.MkdirAll(dirname, 0700)

	s := &simpleCommittedBlockIndex{
		dirname:     dirname,
		indexBlocks: map[PhysicalBlockID]bool{},
	}
	return s, nil
}
