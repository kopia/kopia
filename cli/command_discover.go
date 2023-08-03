package cli

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/snapshot/snapshotfs"
	"github.com/pkg/errors"
)

type commandDiscover struct {
	directoryPath string

	out textOutput
}

func (c *commandDiscover) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("discover", " This tool introduces an all-inclusive scanning solution, efficiently scrutinizing directories and delivering comprehensive reports on file system sizes and file counts within the backup’s sources.").Alias("directory")
	cmd.Arg("path", "directory path").Required().StringVar(&c.directoryPath)
	cmd.Action(svc.noRepositoryAction(c.run))

	c.out.setup(svc)
}

func (c *commandDiscover) scanSingleSource(ctx context.Context) error {
	source := c.directoryPath
	log(ctx).Infof("Snapshotting %v ...", source)

	var err error
	var finalErrors []string

	fsEntry, err := c.getContentToDiscover(ctx, source)
	if err != nil {
		finalErrors = append(finalErrors, fmt.Sprintf("failed to prepare source: %s", err))
	}

	s := c.setupScanner()

	err = s.Scan(ctx, fsEntry)
	if err != nil {
		// fail-fast uploads will fail here without recording a manifest, other uploads will
		// possibly fail later.
		return errors.Wrap(err, "upload error")
	}
	return nil
}

func (c *commandDiscover) run(ctx context.Context) error {
	return c.scanSingleSource(ctx)
}

func (c *commandDiscover) getContentToDiscover(ctx context.Context, dir string) (fsEntry fs.Entry, err error) {
	var absDir string

	absDir, err = filepath.Abs(dir)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid source %v", dir)
	}

	fsEntry, err = getLocalFSEntry(ctx, absDir)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get local filesystem entry")
	}
	return fsEntry, nil
}

func (c *commandDiscover) setupScanner() *snapshotfs.Scanner {
	u := snapshotfs.NewScanner()

	return u
}
