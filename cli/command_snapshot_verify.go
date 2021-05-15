package cli

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"sync"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/iocopy"
	"github.com/kopia/kopia/internal/parallelwork"
	"github.com/kopia/kopia/internal/timetrack"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
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
	verifyCommandFilesPercent   int
}

func (c *commandSnapshotVerify) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("verify", "Verify the contents of stored snapshot")
	cmd.Flag("max-errors", "Maximum number of errors before stopping").Default("0").IntVar(&c.verifyCommandErrorThreshold)
	cmd.Flag("directory-id", "Directory object IDs to verify").StringsVar(&c.verifyCommandDirObjectIDs)
	cmd.Flag("file-id", "File object IDs to verify").StringsVar(&c.verifyCommandFileObjectIDs)
	cmd.Flag("all-sources", "Verify all snapshots (DEPRECATED)").Hidden().BoolVar(&c.verifyCommandAllSources)
	cmd.Flag("sources", "Verify the provided sources").StringsVar(&c.verifyCommandSources)
	cmd.Flag("parallel", "Parallelization").Default("16").IntVar(&c.verifyCommandParallel)
	cmd.Flag("verify-files-percent", "Randomly verify a percentage of files").Default("0").IntVar(&c.verifyCommandFilesPercent)
	cmd.Action(svc.repositoryReaderAction(c.run))
}

type verifier struct {
	rep       repo.Repository
	workQueue *parallelwork.Queue
	tt        timetrack.Estimator

	mu   sync.Mutex
	seen map[object.ID]bool

	blobMap map[blob.ID]blob.Metadata

	errors []error

	errorsThreshold      int
	downloadFilesPercent int
}

func (v *verifier) progressCallback(ctx context.Context, enqueued, active, completed int64) {
	maybeTimeRemaining := ""

	if est, ok := v.tt.Estimate(float64(active), float64(completed)); ok {
		maybeTimeRemaining = fmt.Sprintf(" remaining %v (ETA %v)", est.Remaining, formatTimestamp(est.EstimatedEndTime))
	}

	log(ctx).Infof("Found %v objects, verifying %v, completed %v objects%v.", enqueued, active, completed, maybeTimeRemaining)
}

func (v *verifier) tooManyErrors() bool {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.errorsThreshold == 0 {
		return false
	}

	return len(v.errors) >= v.errorsThreshold
}

func (v *verifier) reportError(ctx context.Context, path string, err error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	log(ctx).Errorf("failed on %v: %v", path, err)
	v.errors = append(v.errors, err)
}

func (v *verifier) shouldEnqueue(oid object.ID) bool {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.seen[oid] {
		return false
	}

	v.seen[oid] = true

	return true
}

func (v *verifier) enqueueVerifyDirectory(ctx context.Context, oid object.ID, path string) {
	// push to the front of the queue, so that we quickly discover all directories to get reliable ETA.
	if !v.shouldEnqueue(oid) {
		return
	}

	v.workQueue.EnqueueFront(ctx, func() error {
		return v.doVerifyDirectory(ctx, oid, path)
	})
}

func (v *verifier) enqueueVerifyObject(ctx context.Context, oid object.ID, path string) {
	// push to the back of the queue, so that we process non-directories at the end.
	if !v.shouldEnqueue(oid) {
		return
	}

	v.workQueue.EnqueueBack(ctx, func() error {
		return v.doVerifyObject(ctx, oid, path)
	})
}

func (v *verifier) doVerifyDirectory(ctx context.Context, oid object.ID, path string) error {
	log(ctx).Debugf("verifying directory %q (%v)", path, oid)

	d := snapshotfs.DirectoryEntry(v.rep, oid, nil)

	entries, err := d.Readdir(ctx)
	if err != nil {
		v.reportError(ctx, path, errors.Wrapf(err, "error reading %v", oid))
		return nil
	}

	for _, e := range entries {
		if v.tooManyErrors() {
			break
		}

		objectID := e.(object.HasObjectID).ObjectID()
		childPath := path + "/" + e.Name()

		if e.IsDir() {
			v.enqueueVerifyDirectory(ctx, objectID, childPath)
		} else {
			v.enqueueVerifyObject(ctx, objectID, childPath)
		}
	}

	return nil
}

func (v *verifier) doVerifyObject(ctx context.Context, oid object.ID, path string) error {
	log(ctx).Debugf("verifying object %v", oid)

	contentIDs, err := v.rep.VerifyObject(ctx, oid)
	if err != nil {
		v.reportError(ctx, path, errors.Wrapf(err, "error verifying %v", oid))
	}

	if dr, ok := v.rep.(repo.DirectRepository); v.blobMap != nil && ok {
		for _, cid := range contentIDs {
			ci, err := dr.ContentReader().ContentInfo(ctx, cid)
			if err != nil {
				v.reportError(ctx, path, errors.Wrapf(err, "error verifying content %v: %v", cid, err))
				continue
			}

			if _, ok := v.blobMap[ci.GetPackBlobID()]; !ok {
				v.reportError(ctx, path, errors.Errorf("object %v is backed by missing blob %v", oid, ci.GetPackBlobID()))
				continue
			}
		}
	}

	//nolint:gomnd,gosec
	if rand.Intn(100) < v.downloadFilesPercent {
		if err := v.readEntireObject(ctx, oid, path); err != nil {
			v.reportError(ctx, path, errors.Wrapf(err, "error reading object %v", oid))
		}
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

	_, err = iocopy.Copy(ioutil.Discard, r)

	return errors.Wrap(err, "unable to read data")
}

func (c *commandSnapshotVerify) run(ctx context.Context, rep repo.Repository) error {
	if c.verifyCommandAllSources {
		log(ctx).Errorf("DEPRECATED: --all-sources flag has no effect and is the default when no sources are provided.")
	}

	v := &verifier{
		rep:                  rep,
		tt:                   timetrack.Start(),
		workQueue:            parallelwork.NewQueue(),
		seen:                 map[object.ID]bool{},
		errorsThreshold:      c.verifyCommandErrorThreshold,
		downloadFilesPercent: c.verifyCommandFilesPercent,
	}

	if dr, ok := rep.(repo.DirectRepository); ok {
		blobMap, err := readBlobMap(ctx, dr.BlobReader())
		if err != nil {
			return err
		}

		v.blobMap = blobMap
	}

	if err := c.enqueueRootsToVerify(ctx, v, rep); err != nil {
		return err
	}

	v.workQueue.ProgressCallback = v.progressCallback
	if err := v.workQueue.Process(ctx, c.verifyCommandParallel); err != nil {
		return errors.Wrap(err, "error processing work queue")
	}

	if len(v.errors) == 0 {
		return nil
	}

	return errors.Errorf("encountered %v errors", len(v.errors))
}

func (c *commandSnapshotVerify) enqueueRootsToVerify(ctx context.Context, v *verifier, rep repo.Repository) error {
	manifests, err := c.loadSourceManifests(ctx, rep, c.verifyCommandSources)
	if err != nil {
		return err
	}

	for _, man := range manifests {
		path := fmt.Sprintf("%v@%v", man.Source, formatTimestamp(man.StartTime))

		if man.RootEntry == nil {
			continue
		}

		if man.RootEntry.Type == snapshot.EntryTypeDirectory {
			v.enqueueVerifyDirectory(ctx, man.RootObjectID(), path)
		} else {
			v.enqueueVerifyObject(ctx, man.RootObjectID(), path)
		}
	}

	for _, oidStr := range c.verifyCommandDirObjectIDs {
		oid, err := snapshotfs.ParseObjectIDWithPath(ctx, rep, oidStr)
		if err != nil {
			return errors.Wrapf(err, "unable to parse: %q", oidStr)
		}

		v.enqueueVerifyDirectory(ctx, oid, oidStr)
	}

	for _, oidStr := range c.verifyCommandFileObjectIDs {
		oid, err := snapshotfs.ParseObjectIDWithPath(ctx, rep, oidStr)
		if err != nil {
			return errors.Wrapf(err, "unable to parse %q", oidStr)
		}

		v.enqueueVerifyObject(ctx, oid, oidStr)
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
