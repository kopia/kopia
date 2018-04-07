package block

import (
	"bytes"
	"context"

	"github.com/kopia/kopia/storage"
)

type nullBlockCache struct {
	st storage.Storage
}

func (c nullBlockCache) getBlock(ctx context.Context, virtualBlockID string, blockID PhysicalBlockID, offset, length int64) ([]byte, error) {
	return c.st.GetBlock(ctx, string(blockID), offset, length)
}

func (c nullBlockCache) putBlock(ctx context.Context, blockID PhysicalBlockID, data []byte) error {
	return c.st.PutBlock(ctx, string(blockID), bytes.NewReader(data))
}

func (c nullBlockCache) listIndexBlocks(ctx context.Context, full bool) ([]IndexInfo, error) {
	return listIndexBlocksFromStorage(ctx, c.st, full)
}

func (c nullBlockCache) close() error {
	return nil
}
