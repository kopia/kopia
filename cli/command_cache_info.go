package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
)

type commandCacheInfo struct {
	onlyShowPath bool

	svc appServices
	out textOutput
}

func (c *commandCacheInfo) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("info", "Displays cache information and statistics")
	cmd.Flag("path", "Only display cache path").BoolVar(&c.onlyShowPath)
	cmd.Action(svc.repositoryReaderAction(c.run))

	c.svc = svc
	c.out.setup(svc)
}

func (c *commandCacheInfo) run(ctx context.Context, _ repo.Repository) error {
	opts, err := repo.GetCachingOptions(ctx, c.svc.repositoryConfigFileName())
	if err != nil {
		return errors.Wrap(err, "error getting cache options")
	}

	if c.onlyShowPath {
		c.out.printStdout("%v\n", opts.CacheDirectory)
		return nil
	}

	entries, err := os.ReadDir(opts.CacheDirectory)
	if err != nil {
		return errors.Wrap(err, "unable to scan cache directory")
	}

	path2SoftLimit := map[string]int64{
		"contents":        opts.ContentCacheSizeBytes,
		"metadata":        opts.MetadataCacheSizeBytes,
		"server-contents": opts.ContentCacheSizeBytes,
	}

	path2HardLimit := map[string]int64{
		"contents":        opts.ContentCacheSizeLimitBytes,
		"metadata":        opts.MetadataCacheSizeLimitBytes,
		"server-contents": opts.ContentCacheSizeLimitBytes,
	}

	path2SweepAgeSeconds := map[string]time.Duration{
		"contents":        opts.MinContentSweepAge.DurationOrDefault(content.DefaultDataCacheSweepAge),
		"metadata":        opts.MinMetadataSweepAge.DurationOrDefault(content.DefaultMetadataCacheSweepAge),
		"indexes":         opts.MinIndexSweepAge.DurationOrDefault(content.DefaultIndexCacheSweepAge),
		"server-contents": opts.MinContentSweepAge.DurationOrDefault(content.DefaultDataCacheSweepAge),
	}

	for _, ent := range entries {
		if !ent.IsDir() {
			continue
		}

		subdir := filepath.Join(opts.CacheDirectory, ent.Name())

		fileCount, totalFileSize, err := scanCacheDir(subdir)
		if err != nil {
			return err
		}

		maybeLimit := ""

		if l, ok := path2SoftLimit[ent.Name()]; ok {
			var hardLimit string

			if hl := path2HardLimit[ent.Name()]; hl > 0 {
				hardLimit = units.BytesString(hl)
			} else {
				hardLimit = "none"
			}

			maybeLimit = fmt.Sprintf(" (soft limit: %v, hard limit: %v, min sweep age: %v)",
				units.BytesString(l),
				hardLimit,
				path2SweepAgeSeconds[ent.Name()])
		}

		if ent.Name() == "blob-list" {
			maybeLimit = fmt.Sprintf(" (duration: %v)", opts.MaxListCacheDuration.DurationOrDefault(0))
		}

		c.out.printStdout("%v: %v files %v%v\n", subdir, fileCount, units.BytesString(totalFileSize), maybeLimit)
	}

	c.out.printStderr("To adjust cache sizes use 'kopia cache set'.\n")
	c.out.printStderr("To clear caches use 'kopia cache clear'.\n")

	return nil
}
