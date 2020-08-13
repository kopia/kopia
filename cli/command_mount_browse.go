package cli

import (
	"context"
	"os"
	"os/exec"

	"github.com/pkg/errors"
	"github.com/skratchdot/open-golang/open"
)

var mountBrowser = mountCommand.Flag("browse", "Browse mounted filesystem using the provided method").Default("OS").Enum("NONE", "WEB", "OS")

var mountBrowsers = map[string]func(ctx context.Context, mountPoint, addr string) error{
	"NONE": nil,
	"WEB":  openInWebBrowser,
	"OS":   openInOSBrowser,
}

func browseMount(ctx context.Context, mountPoint, addr string) error {
	b := mountBrowsers[*mountBrowser]
	if b == nil {
		waitForCtrlC()
		return nil
	}

	return b(ctx, mountPoint, addr)
}

// nolint:unparam
func openInWebBrowser(ctx context.Context, mountPoint, addr string) error {
	startWebBrowser(ctx, addr)
	waitForCtrlC()

	return nil
}

func openInOSBrowser(ctx context.Context, mountPoint, addr string) error {
	if isWindows() {
		return netUSE(ctx, mountPoint, addr)
	}

	startWebBrowser(ctx, addr)
	waitForCtrlC()

	return nil
}

func netUSE(ctx context.Context, mountPoint, addr string) error {
	c := exec.Command("net", "use", mountPoint, addr) // nolint:gosec
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	c.Stdin = os.Stdin

	if err := c.Run(); err != nil {
		return errors.Wrap(err, "unable to mount")
	}

	startWebBrowser(ctx, "x:\\")
	waitForCtrlC()

	c = exec.Command("net", "use", mountPoint, "/d") // nolint:gosec
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	c.Stdin = os.Stdin

	if err := c.Run(); err != nil {
		return errors.Wrap(err, "unable to unmount")
	}

	return nil
}

func startWebBrowser(ctx context.Context, url string) {
	if err := open.Start(url); err != nil {
		log(ctx).Warningf("unable to start web browser: %v", err)
	}
}
