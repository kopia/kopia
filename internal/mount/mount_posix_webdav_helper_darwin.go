package mount

import (
	"context"
	"os/exec"

	"github.com/pkg/errors"
)

func mountWebDavHelper(ctx context.Context, url, path string) error {
	mount := exec.CommandContext(ctx, "/sbin/mount", "-t", "webdav", "-r", url, path)
	if err := mount.Run(); err != nil {
		return errors.Errorf("webdav mount %q on %q failed: %v", url, path, err)
	}

	return nil
}

func unmountWebDavHelper(ctx context.Context, path string) error {
	unmount := exec.CommandContext(ctx, "/usr/sbin/diskutil", "unmount", path)
	if err := unmount.Run(); err != nil {
		return errors.Errorf("unmount %q failed: %v", path, err)
	}

	return nil
}
