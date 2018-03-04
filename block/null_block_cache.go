package block

import (
	"github.com/kopia/kopia/storage"
)

type nullBlockCache struct {
	st storage.Storage
}

func (c nullBlockCache) getBlock(virtualBlockID string, blockID string, offset, length int64) ([]byte, error) {
	return c.st.GetBlock(blockID, offset, length)
}

func (c nullBlockCache) putBlock(blockID string, data []byte) error {
	return c.st.PutBlock(blockID, data)
}

func (c nullBlockCache) listIndexBlocks(full bool) ([]Info, error) {
	return listIndexBlocksFromStorage(c.st, full)
}

func (c nullBlockCache) close() error {
	return nil
}
