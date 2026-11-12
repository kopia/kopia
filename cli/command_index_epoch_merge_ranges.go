package cli

import (
	"context"
	"runtime"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content/indexblob"
)

type commandIndexEpochMergeRanges struct {
	partitions int
	dryRun     bool
	tempDir    string

	out textOutput
}

func (c *commandIndexEpochMergeRanges) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("merge-ranges", "Merge all range checkpoints into one and drop deleted entries below the deletion watermark.")
	cmd.Flag("partitions", "Number of passes over the index (more passes need less memory)").Default("32").IntVar(&c.partitions)
	cmd.Flag("temp-dir", "Directory for the decrypted index during the merge (default: system temp)").StringVar(&c.tempDir)
	cmd.Flag("dry-run", "Only compute statistics, do not write anything").BoolVar(&c.dryRun)
	cmd.Action(svc.directRepositoryReadAction(c.run))

	c.out.setup(svc)
}

func (c *commandIndexEpochMergeRanges) run(ctx context.Context, rep repo.DirectRepository) error {
	provider, ok := rep.ContentReader().(interface {
		IndexBlobManagerV1() *indexblob.ManagerV1
	})
	if !ok {
		return errors.New("content reader does not expose the index blob manager")
	}

	if _, enabled, err := rep.ContentReader().EpochManager(ctx); err != nil || !enabled {
		return errors.Errorf("epoch manager is not active (err=%v)", err)
	}

	started := time.Now() //nolint:forbidigo

	stats, err := provider.IndexBlobManagerV1().MergeRangeCheckpoints(ctx, indexblob.MergeRangeOptions{
		Partitions: c.partitions,
		DryRun:     c.dryRun,
		TempDir:    c.tempDir,
		Progress: func(format string, args ...any) {
			log(ctx).Infof(format, args...)
		},
	})
	if err != nil {
		return errors.Wrap(err, "merge failed")
	}

	c.out.printStdout("Range:                 xr%v_%v (from %v range checkpoints, %v blobs, %v)\n",
		stats.MinEpoch, stats.MaxEpoch, stats.InputRanges, stats.InputBlobs, units.BytesString(stats.InputBytes))
	c.out.printStdout("Deletion watermark:    %v\n", stats.DeletionWatermark.UTC().Format(time.RFC3339))
	c.out.printStdout("Input entries:         %v\n", stats.InputEntries)
	c.out.printStdout("Unique contents:       %v\n", stats.UniqueContents)
	c.out.printStdout("Dropped (deleted):     %v\n", stats.DroppedDeleted)
	c.out.printStdout("Kept deleted (recent): %v\n", stats.KeptDeletedRecent)
	c.out.printStdout("Kept deleted (guard):  %v\n", stats.KeptDeletedGuard)
	c.out.printStdout("Later index blobs:     %v (%v entries not newer than the watermark)\n", stats.LaterBlobs, stats.LaterOldEntries)
	c.out.printStdout("Output entries:        %v (%v)\n", stats.OutputEntries, units.BytesString(stats.OutputBytes))

	for _, b := range stats.OutputBlobs {
		c.out.printStdout("Written:               %v\n", b)
	}

	if c.dryRun {
		c.out.printStdout("Dry run, nothing written.\n")
	}

	var memStats runtime.MemStats

	runtime.ReadMemStats(&memStats)

	c.out.printStdout("Duration:              %v\n", time.Since(started).Round(time.Second)) //nolint:forbidigo
	c.out.printStdout("Memory from OS:        %v\n", units.BytesString(int64(memStats.Sys)))  //nolint:gosec

	return nil
}
