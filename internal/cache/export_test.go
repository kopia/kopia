package cache

import (
	"context"

	"github.com/kopia/kopia/internal/gather"
)

// FreeSpaceCacheTTL exposes the internal free-space query rate-limit for tests.
const FreeSpaceCacheTTL = freeSpaceCacheTTL

// TestingGetFull fetches the contents of a full blob. Returns false if not found.
func (c *PersistentCache) TestingGetFull(ctx context.Context, key string, output *gather.WriteBuffer) bool {
	return c.getFull(ctx, key, output)
}
