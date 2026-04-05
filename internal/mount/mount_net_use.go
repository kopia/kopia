//go:build windows && !winfsp

package mount

import (
	"context"

	"github.com/kopia/kopia/fs"
)

// Directory mounts a given directory under a provided drive letter using WebDAV + net use.
func Directory(ctx context.Context, entry fs.Directory, driveLetter string, _ Options) (Controller, error) {
	return directoryWebDAVNetUse(ctx, entry, driveLetter)
}
