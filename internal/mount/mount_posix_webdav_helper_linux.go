package mount

import (
	"context"
	"os/exec"
)

func mountWebDavHelper(ctx context.Context, webDavURL, path string) error {
	// webDavUrl is defined by the webdav controller, it is not directly user specified
	// path is either:
	// - specified by the user in the CLI case, in which case they could run 'mount' directly
	// - specified by the server as a temporary directory in TMPDIR, and does not use a value from the "mount request"
	mount := exec.CommandContext(ctx, "/usr/bin/mount", "-t", "davfs", "-r", webDavURL, path) //nolint:gosec // G204: see comment above
	if err := mount.Run(); err != nil {
		log(ctx).Errorf("mount command failed: %v. Cowardly refusing to run with root permissions. Try \"sudo /usr/bin/mount -t davfs -r %s %s\"\n", err, webDavURL, path)
	}

	return nil
}

func unmountWebDavHelper(ctx context.Context, path string) error {
	unmount := exec.CommandContext(ctx, "/usr/bin/umount", path) //nolint:gosec // G204: path comes from the mount controller field
	if err := unmount.Run(); err != nil {
		log(ctx).Errorf("umount command failed: %v. Cowardly refusing to run with root permissions. Try \"sudo /usr/bin/umount %s\"\n", err, path)
	}

	return nil
}
