package cli

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

type commandSnapshotEstimate struct {
	snapshotEstimateSource      string
	snapshotEstimateShowFiles   bool
	snapshotEstimateQuiet       bool
	snapshotEstimateUploadSpeed float64
	maxExamplesPerBucket        int

	out textOutput
}

func (c *commandSnapshotEstimate) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("estimate", "Estimate the snapshot size and upload time.")
	cmd.Arg("source", "File or directory to analyze.").Required().ExistingFileOrDirVar(&c.snapshotEstimateSource)
	cmd.Flag("show-files", "Show files").BoolVar(&c.snapshotEstimateShowFiles)
	cmd.Flag("quiet", "Do not display scanning progress").Short('q').BoolVar(&c.snapshotEstimateQuiet)
	cmd.Flag("upload-speed", "Upload speed to use for estimation").Default("10").PlaceHolder("mbit/s").Float64Var(&c.snapshotEstimateUploadSpeed)
	cmd.Flag("max-examples-per-bucket", "Max examples per bucket").Default("10").IntVar(&c.maxExamplesPerBucket)
	cmd.Action(svc.repositoryReaderAction(c.run))
	c.out.setup(svc)
}

type estimateProgress struct {
	stats        snapshot.Stats
	included     snapshotfs.SampleBuckets
	excluded     snapshotfs.SampleBuckets
	excludedDirs []string
	quiet        bool
}

func (ep *estimateProgress) Processing(ctx context.Context, dirname string) {
	if !ep.quiet {
		log(ctx).Infof("Analyzing %v...", dirname)
	}
}

func (ep *estimateProgress) Error(ctx context.Context, filename string, err error, isIgnored bool) {
	if isIgnored {
		log(ctx).Errorf("Ignored error in %v: %v", filename, err)
	} else {
		log(ctx).Errorf("Error in %v: %v", filename, err)
	}
}

func (ep *estimateProgress) Stats(ctx context.Context, st *snapshot.Stats, included, excluded snapshotfs.SampleBuckets, excludedDirs []string, final bool) {
	_ = final

	ep.stats = *st
	ep.included = included
	ep.excluded = excluded
	ep.excludedDirs = excludedDirs
}

func (c *commandSnapshotEstimate) run(ctx context.Context, rep repo.Repository) error {
	path, err := filepath.Abs(c.snapshotEstimateSource)
	if err != nil {
		return errors.Errorf("invalid path: '%s': %s", path, err)
	}

	sourceInfo := snapshot.SourceInfo{
		Path:     filepath.Clean(path),
		Host:     rep.ClientOptions().Hostname,
		UserName: rep.ClientOptions().Username,
	}

	entry, err := getLocalFSEntry(ctx, path)
	if err != nil {
		return err
	}

	dir, ok := entry.(fs.Directory)
	if !ok {
		return errors.Errorf("invalid path: '%s': must be a directory", path)
	}

	var ep estimateProgress

	ep.quiet = c.snapshotEstimateQuiet

	policyTree, err := policy.TreeForSource(ctx, rep, sourceInfo)
	if err != nil {
		return errors.Wrapf(err, "error creating policy tree for %v", sourceInfo)
	}

	if err := snapshotfs.Estimate(ctx, dir, policyTree, &ep, c.maxExamplesPerBucket); err != nil {
		return errors.Wrap(err, "error estimating")
	}

	c.out.printStdout("Snapshot includes %v file(s), total size %v\n", ep.stats.TotalFileCount, units.BytesString(ep.stats.TotalFileSize))
	c.showBuckets(ep.included, c.snapshotEstimateShowFiles)
	c.out.printStdout("\n")

	if ep.stats.ExcludedFileCount > 0 {
		c.out.printStdout("Snapshot excludes %v file(s), total size %v\n", ep.stats.ExcludedFileCount, units.BytesString(ep.stats.ExcludedTotalFileSize))
		c.showBuckets(ep.excluded, true)
	} else {
		c.out.printStdout("Snapshot excludes no files.\n")
	}

	if ep.stats.ExcludedDirCount > 0 {
		c.out.printStdout("Snapshot excludes %v directories. Examples:\n", ep.stats.ExcludedDirCount)

		for _, ed := range ep.excludedDirs {
			c.out.printStdout(" - %v\n", ed)
		}
	} else {
		c.out.printStdout("Snapshot excludes no directories.\n")
	}

	if ep.stats.ErrorCount > 0 {
		c.out.printStdout("Encountered %v error(s).\n", ep.stats.ErrorCount)
	}

	megabits := float64(ep.stats.TotalFileSize) * 8 / 1000000 //nolint:mnd
	seconds := megabits / c.snapshotEstimateUploadSpeed

	c.out.printStdout("\n")
	c.out.printStdout("Estimated upload time: %v at %v Mbit/s\n", time.Duration(seconds)*time.Second, c.snapshotEstimateUploadSpeed)

	return nil
}

func (c *commandSnapshotEstimate) showBuckets(buckets snapshotfs.SampleBuckets, showFiles bool) {
	for i, bucket := range buckets {
		if bucket.Count == 0 {
			continue
		}

		var sizeRange string

		if i == 0 {
			sizeRange = fmt.Sprintf("< %-6v",
				units.BytesString(bucket.MinSize))
		} else {
			sizeRange = fmt.Sprintf("%-6v...%6v",
				units.BytesString(bucket.MinSize),
				units.BytesString(buckets[i-1].MinSize))
		}

		c.out.printStdout("%18v: %7v files, total size %v\n",
			sizeRange,
			bucket.Count, units.BytesString(bucket.TotalSize))

		if showFiles {
			for _, sample := range bucket.Examples {
				c.out.printStdout(" - %v\n", sample)
			}
		}
	}
}
