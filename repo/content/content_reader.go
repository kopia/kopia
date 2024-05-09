package content

import (
	"context"

	"github.com/kopia/kopia/internal/epoch"
	"github.com/kopia/kopia/repo/format"
)

// Reader defines content read API.
type Reader interface {
	// returns true if the repository supports content compression.
	// this may be slightly stale if the repository recently
	// got upgraded, in which case it will return false which is safe.
	SupportsContentCompression() bool
	ContentFormat() format.Provider
	GetContent(ctx context.Context, id ID) ([]byte, error)
	ContentInfo(ctx context.Context, id ID) (Info, error)
	IterateContents(ctx context.Context, opts IterateOptions, callback IterateCallback) error
	IteratePacks(ctx context.Context, opts IteratePackOptions, callback IteratePacksCallback) error
	ListActiveSessions(ctx context.Context) (map[SessionID]*SessionInfo, error)
	EpochManager(ctx context.Context) (*epoch.Manager, bool, error)
}
