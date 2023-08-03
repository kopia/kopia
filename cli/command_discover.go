package cli

import (
	"context"

	"github.com/kopia/kopia/repo"
	"github.com/pkg/errors"
)

type commandDiscover struct {
	directoryPath string

	out textOutput
}

func (c *commandDiscover) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("discovery", " This tool introduces an all-inclusive scanning solution, efficiently scrutinizing directories and delivering comprehensive reports on file system sizes and file counts within the backup’s sources.").Alias("directory")
	cmd.Arg("path", "directory path").Required().StringVar(&c.directoryPath)
	cmd.Action(svc.repositoryReaderAction(c.run))

	c.out.setup(svc)
}

func (c *commadDiscover) scanSingleSource() error {
	log(ctx).Infof("Snapshotting %v ...", sourceInfo)

	var err error

	manifest, err := u.Upload(ctx, fsEntry, policyTree, sourceInfo, previous...)
	if err != nil {
		// fail-fast uploads will fail here without recording a manifest, other uploads will
		// possibly fail later.
		return errors.Wrap(err, "upload error")
	}
}

func (c *commandDiscover) run(ctx context.Context, rep repo.Repository) error {
	return c.snapshotSingleSource(ctx, fsEntry, setManual, rep, u, sourceInfo, tags)
}
