package cli

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/maintenance"
)

type commandLogsCleanup struct {
	maxTotalSizeMB int64
	maxCount       int
	maxAge         time.Duration
	dryRun         bool
}

func (c *commandLogsCleanup) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("cleanup", "Clean up logs")

	cmd.Flag("max-age", "Maximal age").Default("720h").DurationVar(&c.maxAge)
	cmd.Flag("max-count", "Maximal number of files to keep").Default("10000").IntVar(&c.maxCount)
	cmd.Flag("max-total-size-mb", "Maximal total size in MiB").Default("1024").Int64Var(&c.maxTotalSizeMB)
	cmd.Flag("dry-run", "Do not delete").BoolVar(&c.dryRun)

	cmd.Action(svc.directRepositoryWriteAction(c.run))
}

func (c *commandLogsCleanup) run(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	toDelete, err := maintenance.CleanupLogs(ctx, rep, maintenance.LogRetentionOptions{
		MaxTotalSize: c.maxTotalSizeMB << 20, //nolint:mnd
		MaxCount:     c.maxCount,
		MaxAge:       c.maxAge,
		DryRun:       c.dryRun,
	})
	if err != nil {
		return errors.Wrap(err, "error expiring logs")
	}

	if len(toDelete) > 0 {
		if c.dryRun {
			log(ctx).Infof("Would delete %v logs.", len(toDelete))
		} else {
			log(ctx).Infof("Deleted %v logs.", len(toDelete))
		}
	} else {
		log(ctx).Info("No logs found to delete.")
	}

	return nil
}
