package cli

import (
	"context"
	"fmt"
	"runtime"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

type commandSnapshotVerify struct {
	verifyCommandErrorThreshold int
	verifyCommandDirObjectIDs   []string
	verifyCommandFileObjectIDs  []string
	verifyCommandAllSources     bool
	verifyCommandSources        []string
	verifyCommandParallel       int
	verifyCommandFilesPercent   float64

	fileQueueLength int
	fileParallelism int
}

func (c *commandSnapshotVerify) setup(svc appServices, parent commandParent) {
	c.fileParallelism = runtime.NumCPU()

	cmd := parent.Command("verify", "Verify the contents of stored snapshot")
	cmd.Flag("max-errors", "Maximum number of errors before stopping").Default("0").IntVar(&c.verifyCommandErrorThreshold)
	cmd.Flag("directory-id", "Directory object IDs to verify").StringsVar(&c.verifyCommandDirObjectIDs)
	cmd.Flag("file-id", "File object IDs to verify").StringsVar(&c.verifyCommandFileObjectIDs)
	cmd.Flag("all-sources", "Verify all snapshots (DEPRECATED)").Hidden().BoolVar(&c.verifyCommandAllSources)
	cmd.Flag("sources", "Verify the provided sources").StringsVar(&c.verifyCommandSources)
	cmd.Flag("parallel", "Parallelization").Default("8").IntVar(&c.verifyCommandParallel)
	cmd.Flag("file-queue-length", "Queue length for file verification").Default("20000").IntVar(&c.fileQueueLength)
	cmd.Flag("file-parallelism", "Parallelism for file verification").IntVar(&c.fileParallelism)
	cmd.Flag("verify-files-percent", "Randomly verify a percentage of files by downloading them [0.0 .. 100.0]").Default("0").Float64Var(&c.verifyCommandFilesPercent)
	cmd.Action(svc.repositoryReaderAction(c.run))
}

func (c *commandSnapshotVerify) run(ctx context.Context, rep repo.Repository) error {
	if c.verifyCommandAllSources {
		log(ctx).Errorf("DEPRECATED: --all-sources flag has no effect and is the default when no sources are provided.")
	}

	if dr, ok := rep.(repo.DirectRepositoryWriter); ok {
		dr.DisableIndexRefresh()
	}

	opts := snapshotfs.VerifierOptions{
		VerifyFilesPercent: c.verifyCommandFilesPercent,
		FileQueueLength:    c.fileQueueLength,
		Parallelism:        c.fileParallelism,
		MaxErrors:          c.verifyCommandErrorThreshold,
	}

	if dr, ok := rep.(repo.DirectRepository); ok {
		blobMap, err := blob.ReadBlobMap(ctx, dr.BlobReader())
		if err != nil {
			return errors.Wrap(err, "unable to read blob map")
		}

		opts.BlobMap = blobMap
	}

	v := snapshotfs.NewVerifier(ctx, rep, opts)
	defer v.ShowFinalStats(ctx)

	//nolint:wrapcheck
	return v.InParallel(ctx, func(tw *snapshotfs.TreeWalker) error {
		manifests, err := c.loadSourceManifests(ctx, rep, c.verifyCommandSources)
		if err != nil {
			return err
		}

		for _, man := range manifests {
			rootPath := fmt.Sprintf("%v@%v", man.Source, formatTimestamp(man.StartTime.ToTime()))

			if man.RootEntry == nil {
				continue
			}

			root, err := snapshotfs.SnapshotRoot(rep, man)
			if err != nil {
				return errors.Wrapf(err, "unable to get snapshot root: %q", rootPath)
			}

			// ignore error now, return aggregate error at a higher level.
			//nolint:errcheck
			tw.Process(ctx, root, rootPath)
		}

		for _, oidStr := range c.verifyCommandDirObjectIDs {
			oid, err := snapshotfs.ParseObjectIDWithPath(ctx, rep, oidStr)
			if err != nil {
				return errors.Wrapf(err, "unable to parse: %q", oidStr)
			}

			// ignore error now, return aggregate error at a higher level.
			//nolint:errcheck
			tw.Process(ctx, snapshotfs.DirectoryEntry(rep, oid, nil), oidStr)
		}

		for _, oidStr := range c.verifyCommandFileObjectIDs {
			oid, err := snapshotfs.ParseObjectIDWithPath(ctx, rep, oidStr)
			if err != nil {
				return errors.Wrapf(err, "unable to parse %q", oidStr)
			}

			// ignore error now, return aggregate error at a higher level.
			//nolint:errcheck
			tw.Process(ctx, snapshotfs.AutoDetectEntryFromObjectID(ctx, rep, oid, oidStr), oidStr)
		}

		return nil
	})
}

func (c *commandSnapshotVerify) loadSourceManifests(ctx context.Context, rep repo.Repository, sources []string) ([]*snapshot.Manifest, error) {
	var manifestIDs []manifest.ID

	if len(sources)+len(c.verifyCommandDirObjectIDs)+len(c.verifyCommandFileObjectIDs) == 0 {
		man, err := snapshot.ListSnapshotManifests(ctx, rep, nil, nil)
		if err != nil {
			return nil, errors.Wrap(err, "unable to list snapshot manifests")
		}

		manifestIDs = append(manifestIDs, man...)
	} else {
		for _, srcStr := range sources {
			src, err := snapshot.ParseSourceInfo(srcStr, rep.ClientOptions().Hostname, rep.ClientOptions().Username)
			if err != nil {
				return nil, errors.Wrapf(err, "error parsing %q", srcStr)
			}
			man, err := snapshot.ListSnapshotManifests(ctx, rep, &src, nil)
			if err != nil {
				return nil, errors.Wrapf(err, "unable to list snapshot manifests for %v", src)
			}
			manifestIDs = append(manifestIDs, man...)
		}
	}

	//nolint:wrapcheck
	return snapshot.LoadSnapshots(ctx, rep, manifestIDs)
}
