package cli

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/iocopy"
	"github.com/kopia/kopia/internal/timetrack"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

type verifyFileWorkItem struct {
	oid       object.ID
	entryPath string
}

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

type verifier struct {
	throttle  timetrack.Throttle
	queued    int32
	processed int32

	fileWorkItems chan verifyFileWorkItem

	rep repo.Repository

	blobMap map[blob.ID]blob.Metadata

	downloadFilesPercent float64
}

func (v *verifier) showStats(ctx context.Context) {
	processed := atomic.LoadInt32(&v.processed)

	log(ctx).Infof("Processed %v objects.", processed)
}

func (v *verifier) verifyFile(ctx context.Context, oid object.ID, entryPath string) error {
	log(ctx).Debugf("verifying object %v", oid)

	defer atomic.AddInt32(&v.processed, 1)

	contentIDs, err := v.rep.VerifyObject(ctx, oid)
	if err != nil {
		return errors.Wrap(err, "verify object")
	}

	if dr, ok := v.rep.(repo.DirectRepository); v.blobMap != nil && ok {
		for _, cid := range contentIDs {
			ci, err := dr.ContentReader().ContentInfo(ctx, cid)
			if err != nil {
				return errors.Wrapf(err, "error verifying content %v", cid)
			}

			if _, ok := v.blobMap[ci.GetPackBlobID()]; !ok {
				return errors.Errorf("object %v is backed by missing blob %v", oid, ci.GetPackBlobID())
			}
		}
	}

	//nolint:gosec
	if 100*rand.Float64() < v.downloadFilesPercent {
		if err := v.readEntireObject(ctx, oid, entryPath); err != nil {
			return errors.Wrapf(err, "error reading object %v", oid)
		}
	}

	return nil
}

func (v *verifier) doVerifyObject(ctx context.Context, e fs.Entry, oid object.ID, entryPath string) error {
	if v.throttle.ShouldOutput(time.Second) {
		v.showStats(ctx)
	}

	if !e.IsDir() {
		v.fileWorkItems <- verifyFileWorkItem{oid, entryPath}
		atomic.AddInt32(&v.queued, 1)
	} else {
		atomic.AddInt32(&v.queued, 1)
		atomic.AddInt32(&v.processed, 1)
	}

	return nil
}

func (v *verifier) readEntireObject(ctx context.Context, oid object.ID, path string) error {
	log(ctx).Debugf("reading object %v %v", oid, path)

	// also read the entire file
	r, err := v.rep.OpenObject(ctx, oid)
	if err != nil {
		return errors.Wrapf(err, "unable to open object %v", oid)
	}
	defer r.Close() //nolint:errcheck

	return errors.Wrap(iocopy.JustCopy(io.Discard, r), "unable to read data")
}

func (c *commandSnapshotVerify) run(ctx context.Context, rep repo.Repository) error {
	if c.verifyCommandAllSources {
		log(ctx).Errorf("DEPRECATED: --all-sources flag has no effect and is the default when no sources are provided.")
	}

	if dr, ok := rep.(repo.DirectRepositoryWriter); ok {
		dr.DisableIndexRefresh()
	}

	v := &verifier{
		rep:                  rep,
		downloadFilesPercent: c.verifyCommandFilesPercent,
		fileWorkItems:        make(chan verifyFileWorkItem, c.fileQueueLength),
	}

	tw, twerr := snapshotfs.NewTreeWalker(snapshotfs.TreeWalkerOptions{
		Parallelism:   c.verifyCommandParallel,
		EntryCallback: v.doVerifyObject,
		MaxErrors:     c.verifyCommandErrorThreshold,
	})
	if twerr != nil {
		return errors.Wrap(twerr, "unable to initialize tree walker")
	}

	defer tw.Close()

	if dr, ok := rep.(repo.DirectRepository); ok {
		blobMap, err := readBlobMap(ctx, dr.BlobReader())
		if err != nil {
			return err
		}

		v.blobMap = blobMap
	}

	var vwg sync.WaitGroup

	for i := 0; i < c.fileParallelism; i++ {
		vwg.Add(1)

		go func() {
			defer vwg.Done()

			for wi := range v.fileWorkItems {
				if tw.TooManyErrors() {
					continue
				}

				if err := v.verifyFile(ctx, wi.oid, wi.entryPath); err != nil {
					tw.ReportError(ctx, wi.entryPath, err)
				}
			}
		}()
	}

	err := c.processRoots(ctx, tw, rep)

	close(v.fileWorkItems)
	vwg.Wait()

	if err != nil {
		return err
	}

	v.showStats(ctx)

	// nolint:wrapcheck
	return tw.Err()
}

func (c *commandSnapshotVerify) processRoots(ctx context.Context, tw *snapshotfs.TreeWalker, rep repo.Repository) error {
	manifests, err := c.loadSourceManifests(ctx, rep, c.verifyCommandSources)
	if err != nil {
		return err
	}

	for _, man := range manifests {
		rootPath := fmt.Sprintf("%v@%v", man.Source, formatTimestamp(man.StartTime))

		if man.RootEntry == nil {
			continue
		}

		root, err := snapshotfs.SnapshotRoot(rep, man)
		if err != nil {
			return errors.Wrapf(err, "unable to get snapshot root: %q", rootPath)
		}

		// ignore error now, return aggregate error at a higher level.
		// nolint:errcheck
		tw.Process(ctx, root, rootPath)
	}

	for _, oidStr := range c.verifyCommandDirObjectIDs {
		oid, err := snapshotfs.ParseObjectIDWithPath(ctx, rep, oidStr)
		if err != nil {
			return errors.Wrapf(err, "unable to parse: %q", oidStr)
		}

		// ignore error now, return aggregate error at a higher level.
		// nolint:errcheck
		tw.Process(ctx, snapshotfs.DirectoryEntry(rep, oid, nil), oidStr)
	}

	for _, oidStr := range c.verifyCommandFileObjectIDs {
		oid, err := snapshotfs.ParseObjectIDWithPath(ctx, rep, oidStr)
		if err != nil {
			return errors.Wrapf(err, "unable to parse %q", oidStr)
		}

		// ignore error now, return aggregate error at a higher level.
		// nolint:errcheck
		tw.Process(ctx, snapshotfs.AutoDetectEntryFromObjectID(ctx, rep, oid, oidStr), oidStr)
	}

	return nil
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

	// nolint:wrapcheck
	return snapshot.LoadSnapshots(ctx, rep, manifestIDs)
}
