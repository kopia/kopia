//go:build !linux

package kopiarunner

import "os/exec"

func setpdeath(c *exec.Cmd) *exec.Cmd {
	return c
}
