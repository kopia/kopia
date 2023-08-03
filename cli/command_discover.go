package cli

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/snapshotfs"
	"github.com/pkg/errors"
)

type commandDiscover struct {
	directoryPath string

	out textOutput
}

func (c *commandDiscover) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("discovery", " This tool introduces an all-inclusive scanning solution, efficiently scrutinizing directories and delivering comprehensive reports on file system sizes and file counts within the backup’s sources.").Alias("directory")
	cmd.Arg("path", "directory path").Required().StringVar(&c.directoryPath)
	cmd.Action(svc.repositoryWriterAction(c.run))

	c.out.setup(svc)
}

func (c *commandDiscover) scanSingleSource(ctx context.Context, rep repo.RepositoryWriter) error {
	source := c.directoryPath
	log(ctx).Infof("Snapshotting %v ...", source)

	var err error
	var finalErrors []string

	fsEntry, sourceInfo, err := c.getContentToDiscover(ctx, source, rep)
	if err != nil {
		finalErrors = append(finalErrors, fmt.Sprintf("failed to prepare source: %s", err))
	}

	s := c.setupScanner(rep)

	err = s.Scan(ctx, fsEntry, sourceInfo)
	if err != nil {
		// fail-fast uploads will fail here without recording a manifest, other uploads will
		// possibly fail later.
		return errors.Wrap(err, "upload error")
	}
	return nil
}

func (c *commandDiscover) run(ctx context.Context, rep repo.RepositoryWriter) error {
	return c.scanSingleSource(ctx, rep)
}

func (c *commandDiscover) getContentToDiscover(ctx context.Context, dir string, rep repo.RepositoryWriter) (fsEntry fs.Entry, info snapshot.SourceInfo, err error) {
	var absDir string

	absDir, err = filepath.Abs(dir)
	if err != nil {
		return nil, info, errors.Wrapf(err, "invalid source %v", dir)
	}

	info = snapshot.SourceInfo{
		Path:     filepath.Clean(absDir),
		Host:     rep.ClientOptions().Hostname,
		UserName: rep.ClientOptions().Username,
	}

	fsEntry, err = getLocalFSEntry(ctx, absDir)
	if err != nil {
		return nil, info, errors.Wrap(err, "unable to get local filesystem entry")
	}
	return fsEntry, info, nil
}

func (c *commandDiscover) setupScanner(rep repo.RepositoryWriter) *snapshotfs.Scanner {
	u := snapshotfs.NewScanner()

	return u
}
