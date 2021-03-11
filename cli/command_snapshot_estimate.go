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

var (
	snapshotEstimate            = snapshotCommands.Command("estimate", "Estimate the snapshot size and upload time.")
	snapshotEstimateSource      = snapshotEstimate.Arg("source", "File or directory to analyze.").Required().ExistingFileOrDir()
	snapshotEstimateShowFiles   = snapshotEstimate.Flag("show-files", "Show files").Bool()
	snapshotEstimateQuiet       = snapshotEstimate.Flag("quiet", "Do not display scanning progress").Short('q').Bool()
	snapshotEstimateUploadSpeed = snapshotEstimate.Flag("upload-speed", "Upload speed to use for estimation").Default("10").PlaceHolder("mbit/s").Float64()
)

type estimateProgress struct {
	stats        snapshot.Stats
	included     snapshotfs.SampleBuckets
	excluded     snapshotfs.SampleBuckets
	excludedDirs []string
}

func (ep *estimateProgress) Processing(ctx context.Context, dirname string) {
	if !*snapshotEstimateQuiet {
		log(ctx).Infof("Analyzing %v...", dirname)
	}
}

func (ep *estimateProgress) Error(ctx context.Context, filename string, err error, isIgnored bool) {
	if isIgnored {
		log(ctx).Warningf("Ignored error in %v: %v", filename, err)
	} else {
		log(ctx).Errorf("Error in %v: %v", filename, err)
	}
}

func (ep *estimateProgress) Stats(ctx context.Context, st *snapshot.Stats, included, excluded snapshotfs.SampleBuckets, excludedDirs []string, final bool) {
	ep.stats = *st
	ep.included = included
	ep.excluded = excluded
	ep.excludedDirs = excludedDirs
}

func runSnapshotEstimateCommand(ctx context.Context, rep repo.Repository) error {
	path, err := filepath.Abs(*snapshotEstimateSource)
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

	policyTree, err := policy.TreeForSource(ctx, rep, sourceInfo)
	if err != nil {
		return errors.Wrapf(err, "error creating policy tree for %v", sourceInfo)
	}

	if err := snapshotfs.Estimate(ctx, rep, dir, policyTree, &ep); err != nil {
		return errors.Wrap(err, "error estimating")
	}

	fmt.Printf("Snapshot includes %v files, total size %v\n", ep.stats.TotalFileCount, units.BytesStringBase10(ep.stats.TotalFileSize))
	showBuckets(ep.included, *snapshotEstimateShowFiles)
	fmt.Println()

	if ep.stats.ExcludedFileCount > 0 {
		fmt.Printf("Snapshot excludes %v files, total size %v\n", ep.stats.ExcludedFileCount, ep.stats.ExcludedTotalFileSize)
		showBuckets(ep.excluded, true)
	} else {
		fmt.Printf("Snapshots excludes no files.\n")
	}

	if ep.stats.ExcludedDirCount > 0 {
		fmt.Printf("Snapshots excludes %v directories. Examples:\n", ep.stats.ExcludedDirCount)

		for _, ed := range ep.excludedDirs {
			fmt.Printf(" - %v\n", ed)
		}
	} else {
		fmt.Printf("Snapshots excludes no directories.\n")
	}

	if ep.stats.ErrorCount > 0 {
		fmt.Printf("Encountered %v errors.\n", ep.stats.ErrorCount)
	}

	megabits := float64(ep.stats.TotalFileSize) * 8 / 1000000 //nolint:gomnd
	seconds := megabits / *snapshotEstimateUploadSpeed

	fmt.Println()
	fmt.Printf("Estimated upload time: %v at %v Mbit/s\n", time.Duration(seconds)*time.Second, *snapshotEstimateUploadSpeed)

	return nil
}

func showBuckets(buckets snapshotfs.SampleBuckets, showFiles bool) {
	for i, bucket := range buckets {
		if bucket.Count == 0 {
			continue
		}

		var sizeRange string

		if i == 0 {
			sizeRange = fmt.Sprintf("< %-6v",
				units.BytesStringBase10(bucket.MinSize))
		} else {
			sizeRange = fmt.Sprintf("%-6v...%6v",
				units.BytesStringBase10(bucket.MinSize),
				units.BytesStringBase10(buckets[i-1].MinSize))
		}

		fmt.Printf("%18v: %7v files, total size %v\n",
			sizeRange,
			bucket.Count, units.BytesStringBase10(bucket.TotalSize))

		if showFiles {
			for _, sample := range bucket.Examples {
				fmt.Printf(" - %v\n", sample)
			}
		}
	}
}

func init() {
	snapshotEstimate.Action(repositoryReaderAction(runSnapshotEstimateCommand))
}
