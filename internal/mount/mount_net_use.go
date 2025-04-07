//go:build windows
// +build windows

package mount

import (
	"bufio"
	"context"
	"os/exec"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
)

// Directory mounts a given directory under a provided drive letter.
func Directory(ctx context.Context, entry fs.Directory, driveLetter string, _ Options) (Controller, error) {
	if !isValidWindowsDriveOrAsterisk(driveLetter) {
		return nil, errors.New("must be a valid drive letter or asterisk")
	}

	c, err := DirectoryWebDAV(ctx, entry)
	if err != nil {
		return nil, err
	}

	actualDriveLetter, err := netUseMount(ctx, driveLetter, c.MountPath())
	if err != nil {
		if uerr := c.Unmount(ctx); uerr != nil {
			log(ctx).Errorf("unable to unmount webdav server: %v", uerr)
		}

		return nil, errors.Wrap(err, "unable to mount webdav server as drive letter")
	}

	return netuseController{c, actualDriveLetter}, nil
}

func netUse(ctx context.Context, args ...string) (string, error) {
	nu := exec.CommandContext(ctx, "net", append([]string{"use"}, args...)...) //nolint:gosec
	log(ctx).Debugf("running %v %v", nu.Path, nu.Args)

	out, err := nu.Output()
	log(ctx).Debugf("net use finished with %v %v", string(out), err)

	return string(out), errors.Wrap(err, "error running 'net use'")
}

func netUseMount(ctx context.Context, driveLetter, webdavURL string) (string, error) {
	out, err := netUse(ctx, driveLetter, webdavURL)
	if err != nil {
		return "", errors.Wrapf(err, "unable to run 'net use' (%v), see https://kopia.io/docs/mounting/#windows for more information", out)
	}

	if driveLetter != "*" {
		return driveLetter, nil
	}

	// on success the Net use will print the drive letter
	// because the output is localized, we look for any word that looks like a drive letter followed by
	// colon.
	s := bufio.NewScanner(strings.NewReader(out))
	for s.Scan() {
		for _, word := range strings.Split(s.Text(), " ") {
			if isWindowsDrive(word) {
				return word, nil
			}
		}
	}

	return "", errors.Errorf("unable to find windows drive letter name in successful 'net use' output (%v), this is a bug", out)
}

func netUseUnmount(ctx context.Context, driveLetter string) error {
	_, err := netUse(ctx, driveLetter, "/delete", "/y")
	return err
}

func isWindowsDrive(s string) bool {
	if len(s) != 2 { //nolint:mnd
		return false
	}

	if d := strings.ToUpper(s)[0]; d < 'A' || d > 'Z' {
		return false
	}

	return s[1] == ':'
}

func isValidWindowsDriveOrAsterisk(s string) bool {
	if s == "*" {
		return true
	}

	return isWindowsDrive(s)
}

type netuseController struct {
	inner       Controller
	driveLetter string
}

func (c netuseController) Unmount(ctx context.Context) error {
	if err := netUseUnmount(ctx, c.driveLetter); err != nil {
		return errors.Wrap(err, "unable to delete drive with 'net use'")
	}

	//nolint:wrapcheck
	return c.inner.Unmount(ctx)
}

func (c netuseController) MountPath() string {
	return c.driveLetter
}

func (c netuseController) Done() <-chan struct{} {
	return c.inner.Done()
}
