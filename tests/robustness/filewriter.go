//go:build darwin || (linux && amd64)

package robustness

import "context"

// FileWriter is an interface used for filesystem related actions.
type FileWriter interface {
	// DataDirectory returns the absolute path of the data directory configured.
	DataDirectory(ctx context.Context) string

	// DeleteDirectoryContents deletes some of the content of a random directory,
	// based on its input option values (none of which are required).
	// The method returns the effective option values used and the error if any.
	// ErrNoOp is returned if no directory is found.
	DeleteDirectoryContents(ctx context.Context, opts map[string]string) (map[string]string, error)

	// DeleteEverything deletes all content.
	DeleteEverything(ctx context.Context) error

	// DeleteRandomSubdirectory deletes a random directory, based
	// on its input option values (none of which are required).
	// The method returns the effective option values used and the error if any.
	// ErrNoOp is returned if no directory is found.
	DeleteRandomSubdirectory(ctx context.Context, opts map[string]string) (map[string]string, error)

	// WriteRandomFiles writes a number of files in a random directory, based
	// on its input option values (none of which are required).
	// The method returns the effective option values used and the error if any.
	WriteRandomFiles(ctx context.Context, opts map[string]string) (map[string]string, error)
}
