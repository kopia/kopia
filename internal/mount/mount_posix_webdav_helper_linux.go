package mount

import (
	"context"
	"os/exec"
)

func mountWebDavHelper(ctx context.Context, url, path string) error {
	mount := exec.Command("/usr/bin/mount", "-t", "davfs", "-r", url, path) //nolint:gosec
	if err := mount.Run(); err != nil {
		log(ctx).Warningf("mount command failed: %v. Cowardly refusing to run with root permissions. Try \"sudo /usr/bin/mount -t davfs -r %s %s\"\n", err, url, path)
	}

	return nil
}

func unmountWebDevHelper(ctx context.Context, path string) error {
	unmount := exec.Command("/usr/bin/umount", path) //nolint:gosec
	if err := unmount.Run(); err != nil {
		log(ctx).Warningf("umount command failed: %v. Cowardly refusing to run with root permissions. Try \"sudo /usr/bin/umount %s\"\n", err, path)
	}

	return nil
}
