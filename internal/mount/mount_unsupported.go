//go:build freebsd || openbsd
// +build freebsd openbsd

package mount

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
)

// Directory returns an error due to mounting being unsupported on current operating system.
//
//nolint:revive
func Directory(ctx context.Context, entry fs.Directory, mountPoint string, mountOptions Options) (Controller, error) {
	return nil, errors.New("mounting is not supported")
}
