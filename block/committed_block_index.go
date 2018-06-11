package block

import (
	"path/filepath"

	"github.com/kopia/kopia/internal/packindex"
)

type committedBlockIndex interface {
	getBlock(blockID string) (Info, error)
	listBlocks(prefix string, cb func(i Info) error) error

	addBlock(indexBlockID string, indexData []byte, use bool) error
	hasIndexBlockID(indexBlockID string) (bool, error)

	use(indexBlockIDs []string) (bool, error)
}

func newCommittedBlockIndex(caching CachingOptions) (committedBlockIndex, error) {
	if caching.CacheDirectory != "" {
		return newSimpleCommittedBlockIndex(filepath.Join(caching.CacheDirectory, "indexes"))
	}

	return &inMemoryCommittedBlockIndex{
		cachedPhysicalBlocks: make(map[string]packindex.Index),
		usedPhysicalBlocks:   make(map[string]packindex.Index),
	}, nil
}
