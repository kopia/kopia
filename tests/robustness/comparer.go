package robustness

import (
	"context"
	"io"
)

// Comparer describes an interface that gathers state data on a provided
// path, and compares that data to the state on another path.
type Comparer interface {
	Gather(ctx context.Context, path string) ([]byte, error)
	Compare(ctx context.Context, path string, data []byte, reportOut io.Writer) error
}
