package block

import (
	"bytes"
	"context"

	"github.com/kopia/kopia/storage"
)

type nullBlockCache struct {
	st storage.Storage
}

func (c nullBlockCache) getBlock(ctx context.Context, cacheKey string, blockID string, offset, length int64) ([]byte, error) {
	return c.st.GetBlock(ctx, blockID, offset, length)
}

func (c nullBlockCache) putBlock(ctx context.Context, blockID string, data []byte) error {
	return c.st.PutBlock(ctx, blockID, bytes.NewReader(data))
}

func (c nullBlockCache) listIndexBlocks(ctx context.Context) ([]IndexInfo, error) {
	return listIndexBlocksFromStorage(ctx, c.st)
}

func (c nullBlockCache) deleteListCache(ctx context.Context) {
}

func (c nullBlockCache) deleteBlock(ctx context.Context, blockID string) error {
	return c.st.DeleteBlock(ctx, blockID)
}

func (c nullBlockCache) close() error {
	return nil
}
