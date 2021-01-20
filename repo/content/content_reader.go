package content

import (
	"context"
)

// Reader defines content read API.
type Reader interface {
	ContentFormat() FormattingOptions
	GetContent(ctx context.Context, id ID) ([]byte, error)
	ContentInfo(ctx context.Context, id ID) (Info, error)
	IterateContents(ctx context.Context, opts IterateOptions, callback IterateCallback) error
	IteratePacks(ctx context.Context, opts IteratePackOptions, callback IteratePacksCallback) error
	ListActiveSessions(ctx context.Context) (map[SessionID]*SessionInfo, error)
}
