package mount

import (
	"context"
	"os/exec"

	"github.com/pkg/errors"
)

func mountWebDavHelper(ctx context.Context, webDavURL, path string) error {
	// webDavUrl is defined by the webdav controller, it is not directly user specified
	// path is either:
	// - specified by the user in the CLI case, in which case they could run 'mount' directly
	// - specified by the server as a temporary directory in TMPDIR, and does not use a value from the "mount request"
	mount := exec.CommandContext(ctx, "/sbin/mount", "-t", "webdav", "-r", webDavURL, path) //nolint:gosec // G204: see comment above
	if err := mount.Run(); err != nil {
		return errors.Errorf("webdav mount %q on %q failed: %v", webDavURL, path, err)
	}

	return nil
}

func unmountWebDavHelper(ctx context.Context, path string) error {
	unmount := exec.CommandContext(ctx, "/usr/sbin/diskutil", "unmount", path) //nolint:gosec // G204: path comes from the mount controller field
	if err := unmount.Run(); err != nil {
		return errors.Errorf("unmount %q failed: %v", path, err)
	}

	return nil
}
