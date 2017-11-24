package cli

import (
	"fmt"
	"log"
	"path/filepath"
	"time"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/policy"
	"github.com/kopia/kopia/snapshot"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	snapshotEstimate            = snapshotCommands.Command("estimate", "Estimate the snapshot size and upload time.")
	snapshotEstimateSource      = snapshotEstimate.Arg("source", "File or directory to analyze.").Required().ExistingFileOrDir()
	snapshotEstimateShowFiles   = snapshotEstimate.Flag("show-files", "Show files").Bool()
	snapshotEstimateQuiet       = snapshotEstimate.Flag("quiet", "Do not display scanning progress").Short('q').Bool()
	snapshotEstimateUploadSpeed = snapshotEstimate.Flag("upload-speed", "Upload speed to use for estimation").Default("10").PlaceHolder("mbit/s").Float64()
)

type bucket struct {
	MinSize   int64    `json:"minSize"`
	Count     int      `json:"count"`
	TotalSize int64    `json:"totalSize"`
	Examples  []string `json:"examples,omitempty"`
}

func (b *bucket) add(fname string, size int64) {
	b.Count++
	b.TotalSize += size
	if len(b.Examples) < 10 {
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

func runSnapshotEstimateCommand(c *kingpin.ParseContext) error {
	rep := mustOpenRepository(nil)
	defer rep.Close()

	pmgr := policy.NewManager(rep)

	path, err := filepath.Abs(*snapshotEstimateSource)
	if err != nil {
		return fmt.Errorf("invalid path: '%s': %s", path, err)
	}

	sourceInfo := &snapshot.SourceInfo{Path: filepath.Clean(path), Host: getHostName(), UserName: getUserName()}
	policy, err := pmgr.GetEffectivePolicy(sourceInfo.UserName, sourceInfo.Host, sourceInfo.Path)
	if err != nil {
		return fmt.Errorf("unable to get backup policy for source %v: %v", sourceInfo, err)
	}

	var stats snapshot.Stats
	ib := makeBuckets()
	eb := makeBuckets()
	if err := estimate(".", mustGetLocalFSEntry(path), &policy.FilesPolicy, &stats, ib, eb); err != nil {
		return err
	}

	fmt.Printf("Snapshot includes %v files, total size %v\n", stats.TotalFileCount, units.BytesStringBase10(stats.TotalFileSize))
	showBuckets(ib)
	fmt.Println()

	fmt.Printf("Snapshot excludes %v files, total size %v\n", stats.ExcludedFileCount, units.BytesStringBase10(stats.ExcludedTotalFileSize))
	showBuckets(eb)

	megabits := float64(stats.TotalFileSize) * 8 / 1000000
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
func estimate(relativePath string, entry fs.Entry, pol *policy.FilesPolicy, stats *snapshot.Stats, ib, eb buckets) error {
	if !pol.ShouldInclude(entry.Metadata()) {
		eb.add(relativePath, entry.Metadata().FileSize)
		stats.ExcludedFileCount++
		stats.ExcludedTotalFileSize += entry.Metadata().FileSize
		return nil
	}

	switch entry := entry.(type) {
	case fs.Directory:
		if !*snapshotEstimateQuiet {
			log.Printf("Scanning %q...\n", relativePath)
		}
		children, err := entry.Readdir()
		if err != nil {
			return err
		}

		for _, child := range children {
			if err := estimate(filepath.Join(relativePath, child.Metadata().Name), child, pol, stats, ib, eb); err != nil {
				return err
			}
		}

	case fs.File:
		ib.add(relativePath, entry.Metadata().FileSize)
		stats.TotalFileCount++
		stats.TotalFileSize += entry.Metadata().FileSize
	}
	return nil
}

func init() {
	snapshotEstimate.Action(runSnapshotEstimateCommand)
}
