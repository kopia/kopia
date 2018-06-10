package block

import (
	"context"

	"github.com/kopia/kopia/storage"
)

type nullBlockCache struct {
	st storage.Storage
}

func (c nullBlockCache) getContentBlock(ctx context.Context, cacheKey string, blockID string, offset, length int64) ([]byte, error) {
	return c.st.GetBlock(ctx, blockID, offset, length)
}

func (c nullBlockCache) listIndexBlocks(ctx context.Context) ([]IndexInfo, error) {
	return listIndexBlocksFromStorage(ctx, c.st)
}

func (c nullBlockCache) deleteListCache(ctx context.Context) {
}

func (c nullBlockCache) close() error {
	return nil
}
