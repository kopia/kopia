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

	mu     sync.Mutex
	merged packindex.Merged
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

	_, err := os.Stat(b.indexBlockPath(indexBlockID))
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}

	return false, err
}

func (b *simpleCommittedBlockIndex) indexBlockPath(indexBlockID PhysicalBlockID) string {
	return filepath.Join(b.dirname, string(indexBlockID+simpleIndexSuffix))
}

func (b *simpleCommittedBlockIndex) addBlockToCache(indexBlockID PhysicalBlockID, data []byte) error {
	exists, err := b.hasIndexBlockID(indexBlockID)
	if err != nil {
		return err
	}

	if exists {
		return nil
	}

	// write to a temp file to avoid race where two processes are writing at the same time.
	tf, err := ioutil.TempFile(b.dirname, "index")
	if err != nil {
		return fmt.Errorf("can't create tmp file: %v", err)
	}
	defer os.Remove(tf.Name()) //nolint:errcheck

	if _, err := tf.Write(data); err != nil {
		return fmt.Errorf("can't write to temp file: %v", err)
	}
	if err := tf.Close(); err != nil {
		return fmt.Errorf("can't close tmp file")
	}

	// rename() is atomic, so one process will succeed, but the other will fail
	if err := os.Rename(tf.Name(), b.indexBlockPath(indexBlockID)); err != nil {
		// verify that the block exists
		exists, err := b.hasIndexBlockID(indexBlockID)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("unsuccessful index write of block %q", indexBlockID)
		}
	}

	return nil
}

func (b *simpleCommittedBlockIndex) addBlock(indexBlockID PhysicalBlockID, data []byte, use bool) error {
	if err := b.addBlockToCache(indexBlockID, data); err != nil {
		return err
	}

	if !use {
		return nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	ndx, err := b.openIndex(b.indexBlockPath(indexBlockID))
	if err != nil {
		return fmt.Errorf("unable to open pack index %q: %v", indexBlockID, err)
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
	if err != nil {
		return nil, err
	}

	return packindex.Open(f)
}

func (b *simpleCommittedBlockIndex) use(packBlockIDs []PhysicalBlockID) error {
	b.mu.Lock()
	defer b.mu.Unlock()

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
		newMerged = append(newMerged, ndx)
	}
	b.merged = newMerged
	newMerged = nil
	return nil
}

func newSimpleCommittedBlockIndex(dirname string) (committedBlockIndex, error) {
	_ = os.MkdirAll(dirname, 0700)

	s := &simpleCommittedBlockIndex{
		dirname: dirname,
	}
	return s, nil
}
