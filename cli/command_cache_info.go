package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
)

type commandCacheInfo struct {
	onlyShowPath bool

	svc appServices
	out textOutput
}

func (c *commandCacheInfo) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("info", "Displays cache information and statistics").Default()
	cmd.Flag("path", "Only display cache path").BoolVar(&c.onlyShowPath)
	cmd.Action(svc.repositoryReaderAction(c.run))

	c.svc = svc
	c.out.setup(svc)
}

func (c *commandCacheInfo) run(ctx context.Context, rep repo.Repository) error {
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

	path2Limit := map[string]int64{
		"contents":        opts.MaxCacheSizeBytes,
		"metadata":        opts.MaxMetadataCacheSizeBytes,
		"server-contents": opts.MaxCacheSizeBytes,
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
		if l, ok := path2Limit[ent.Name()]; ok {
			maybeLimit = fmt.Sprintf(" (limit %v)", units.BytesStringBase10(l))
		}

		if ent.Name() == "blob-list" {
			maybeLimit = fmt.Sprintf(" (duration %vs)", opts.MaxListCacheDurationSec)
		}

		c.out.printStdout("%v: %v files %v%v\n", subdir, fileCount, units.BytesStringBase10(totalFileSize), maybeLimit)
	}

	c.out.printStderr("To adjust cache sizes use 'kopia cache set'.\n")
	c.out.printStderr("To clear caches use 'kopia cache clear'.\n")

	return nil
}
