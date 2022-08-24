package content

import (
	"context"

	"github.com/kopia/kopia/internal/epoch"
	"github.com/kopia/kopia/repo/format"
)

// Reader defines content read API.
type Reader interface {
	SupportsContentCompression() (bool, error)
	ContentFormat() format.Provider
	GetContent(ctx context.Context, id ID) ([]byte, error)
	ContentInfo(ctx context.Context, id ID) (Info, error)
	IterateContents(ctx context.Context, opts IterateOptions, callback IterateCallback) error
	IteratePacks(ctx context.Context, opts IteratePackOptions, callback IteratePacksCallback) error
	ListActiveSessions(ctx context.Context) (map[SessionID]*SessionInfo, error)
	EpochManager() (*epoch.Manager, bool, error)
}
