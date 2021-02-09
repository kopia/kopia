package mount

import (
	"fmt"
	"os/exec"
)

func mountWebDavHelper(url, path string) error {
	mount := exec.Command("/usr/bin/mount", "-t", "davfs", "-r", url, path)
	if err := mount.Run(); err != nil {
		fmt.Printf("Cowardly refusing to run with root permissions. Maybe try \"sudo /usr/bin/mount -t davfs -r %s %s\"\n", url, path)
	}
	return nil
}

func unmountWebDevHelper(path string) error {
	unmount := exec.Command("/usr/bin/umount",  path)
	if err := unmount.Run(); err != nil {
		fmt.Printf("Cowardly refusing to run with root permissions. Maybe try \"sudo /usr/bin/umount %s\"\n",  path)
	}
	return nil
}
