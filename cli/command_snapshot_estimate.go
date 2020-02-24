package cli

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/ignorefs"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

var (
	snapshotEstimate            = snapshotCommands.Command("estimate", "Estimate the snapshot size and upload time.")
	snapshotEstimateSource      = snapshotEstimate.Arg("source", "File or directory to analyze.").Required().ExistingFileOrDir()
	snapshotEstimateShowFiles   = snapshotEstimate.Flag("show-files", "Show files").Bool()
	snapshotEstimateQuiet       = snapshotEstimate.Flag("quiet", "Do not display scanning progress").Short('q').Bool()
	snapshotEstimateUploadSpeed = snapshotEstimate.Flag("upload-speed", "Upload speed to use for estimation").Default("10").PlaceHolder("mbit/s").Float64()
)

const maxExamplesPerBucket = 10

type bucket struct {
	MinSize   int64    `json:"minSize"`
	Count     int      `json:"count"`
	TotalSize int64    `json:"totalSize"`
	Examples  []string `json:"examples,omitempty"`
}

func (b *bucket) add(fname string, size int64) {
	b.Count++
	b.TotalSize += size

	if len(b.Examples) < maxExamplesPerBucket {
		b.Examples = append(b.Examples, fmt.Sprintf("%v - %v", fname, units.BytesStringBase10(size)))
	}
}

type buckets []*bucket

func (b buckets) add(fname string, size int64) {
	for _, bucket := range b {
		if size >= bucket.MinSize {
			bucket.add(fname, size)
			break
		}
	}
}

func makeBuckets() buckets {
	return buckets{
		&bucket{MinSize: 1e15},
		&bucket{MinSize: 1e12},
		&bucket{MinSize: 1e9},
		&bucket{MinSize: 1e6},
		&bucket{MinSize: 1e3},
		&bucket{MinSize: 0},
	}
}

func runSnapshotEstimateCommand(ctx context.Context, rep *repo.Repository) error {
	path, err := filepath.Abs(*snapshotEstimateSource)
	if err != nil {
		return errors.Errorf("invalid path: '%s': %s", path, err)
	}

	sourceInfo := snapshot.SourceInfo{
		Path:     filepath.Clean(path),
		Host:     rep.Hostname,
		UserName: rep.Username,
	}

	var stats snapshot.Stats

	ib := makeBuckets()
	eb := makeBuckets()

	onIgnoredFile := func(relativePath string, e fs.Entry) {
		log(ctx).Infof("ignoring %v", relativePath)
		eb.add(relativePath, e.Size())

		if e.IsDir() {
			stats.ExcludedDirCount++
		} else {
			stats.ExcludedFileCount++
			stats.ExcludedTotalFileSize += e.Size()
		}
	}

	entry, err := getLocalFSEntry(ctx, path)
	if err != nil {
		return err
	}

	if dir, ok := entry.(fs.Directory); ok {
		policyTree, err := policy.TreeForSource(ctx, rep, sourceInfo)
		if err != nil {
			return err
		}

		entry = ignorefs.New(dir, policyTree, ignorefs.ReportIgnoredFiles(onIgnoredFile))
	}

	if err := estimate(ctx, ".", entry, &stats, ib); err != nil {
		return err
	}

	fmt.Printf("Snapshot includes %v files, total size %v\n", stats.TotalFileCount, units.BytesStringBase10(stats.TotalFileSize))
	showBuckets(ib)
	fmt.Println()

	fmt.Printf("Snapshot excludes %v directories and %v files with total size %v\n", stats.ExcludedDirCount, stats.ExcludedFileCount, units.BytesStringBase10(stats.ExcludedTotalFileSize))
	showBuckets(eb)

	megabits := float64(stats.TotalFileSize) * 8 / 1000000 //nolint:gomnd
	seconds := megabits / *snapshotEstimateUploadSpeed

	fmt.Println()
	fmt.Printf("Estimated upload time: %v at %v Mbit/s\n", time.Duration(seconds)*time.Second, *snapshotEstimateUploadSpeed)

	return nil
}

func showBuckets(b buckets) {
	for _, bucket := range b {
		if bucket.Count == 0 {
			continue
		}

		fmt.Printf("  with size over %-5v: %7v files, total size %v\n", units.BytesStringBase10(bucket.MinSize), bucket.Count, units.BytesStringBase10(bucket.TotalSize))

		if *snapshotEstimateShowFiles {
			for _, sample := range bucket.Examples {
				fmt.Printf("    %v\n", sample)
			}
		}
	}
}

func estimate(ctx context.Context, relativePath string, entry fs.Entry, stats *snapshot.Stats, ib buckets) error {
	switch entry := entry.(type) {
	case fs.Directory:
		if !*snapshotEstimateQuiet {
			printStderr("Scanning %v\n", relativePath)
		}

		children, err := entry.Readdir(ctx)
		if err != nil {
			return err
		}

		for _, child := range children {
			if err := estimate(ctx, filepath.Join(relativePath, child.Name()), child, stats, ib); err != nil {
				return err
			}
		}

	case fs.File:
		ib.add(relativePath, entry.Size())
		stats.TotalFileCount++
		stats.TotalFileSize += entry.Size()
	}

	return nil
}

func init() {
	snapshotEstimate.Action(repositoryAction(runSnapshotEstimateCommand))
}
