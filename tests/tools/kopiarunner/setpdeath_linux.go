package kopiarunner

import (
	"os/exec"
	"syscall"
)

func setpdeath(c *exec.Cmd) *exec.Cmd {
	if c == nil {
		return nil
	}

	if c.SysProcAttr == nil {
		c.SysProcAttr = &syscall.SysProcAttr{}
	}

	c.SysProcAttr.Pdeathsig = syscall.SIGTERM

	return c
}
