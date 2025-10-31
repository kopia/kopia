package mount

import (
	"context"
	"os/exec"
)

func mountWebDavHelper(ctx context.Context, url, path string) error {
	mount := exec.CommandContext(ctx, "/usr/bin/mount", "-t", "davfs", "-r", url, path)
	if err := mount.Run(); err != nil {
		log(ctx).Errorf("mount command failed: %v. Cowardly refusing to run with root permissions. Try \"sudo /usr/bin/mount -t davfs -r %s %s\"\n", err, url, path)
	}

	return nil
}

func unmountWebDevHelper(ctx context.Context, path string) error {
	unmount := exec.CommandContext(ctx, "/usr/bin/umount", path)
	if err := unmount.Run(); err != nil {
		log(ctx).Errorf("umount command failed: %v. Cowardly refusing to run with root permissions. Try \"sudo /usr/bin/umount %s\"\n", err, path)
	}

	return nil
}
