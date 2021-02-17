package cli

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/iocopy"
	"github.com/kopia/kopia/internal/parallelwork"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

var (
	verifyCommand               = snapshotCommands.Command("verify", "Verify the contents of stored snapshot")
	verifyCommandErrorThreshold = verifyCommand.Flag("max-errors", "Maximum number of errors before stopping").Default("0").Int()
	verifyCommandDirObjectIDs   = verifyCommand.Flag("directory-id", "Directory object IDs to verify").Strings()
	verifyCommandFileObjectIDs  = verifyCommand.Flag("file-id", "File object IDs to verify").Strings()
	verifyCommandAllSources     = verifyCommand.Flag("all-sources", "Verify all snapshots (DEPRECATED)").Hidden().Bool()
	verifyCommandSources        = verifyCommand.Flag("sources", "Verify the provided sources").Strings()
	verifyCommandParallel       = verifyCommand.Flag("parallel", "Parallelization").Default("16").Int()
	verifyCommandFilesPercent   = verifyCommand.Flag("verify-files-percent", "Randomly verify a percentage of files").Default("0").Int()
)

type verifier struct {
	rep       repo.Repository
	workQueue *parallelwork.Queue
	startTime time.Time

	mu   sync.Mutex
	seen map[object.ID]bool

	blobMap map[blob.ID]blob.Metadata

	errors []error
}

func (v *verifier) progressCallback(ctx context.Context, enqueued, active, completed int64) {
	elapsed := clock.Since(v.startTime)
	maybeTimeRemaining := ""

	if elapsed > 1*time.Second && enqueued > 0 && completed > 0 {
		completedRatio := float64(completed) / float64(enqueued)
		predictedSeconds := elapsed.Seconds() / completedRatio
		predictedEndTime := v.startTime.Add(time.Duration(predictedSeconds) * time.Second)

		dt := clock.Until(predictedEndTime)
		if dt > 0 {
			maybeTimeRemaining = fmt.Sprintf(" remaining %v (ETA %v)", dt.Truncate(1*time.Second), formatTimestamp(predictedEndTime.Truncate(1*time.Second)))
		}
	}

	log(ctx).Infof("Found %v objects, verifying %v, completed %v objects%v.", enqueued, active, completed, maybeTimeRemaining)
}

func (v *verifier) tooManyErrors() bool {
	v.mu.Lock()
	defer v.mu.Unlock()

	if *verifyCommandErrorThreshold == 0 {
		return false
	}

	return len(v.errors) >= *verifyCommandErrorThreshold
}

func (v *verifier) reportError(ctx context.Context, path string, err error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	log(ctx).Warningf("failed on %v: %v", path, err)
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

			if _, ok := v.blobMap[ci.PackBlobID]; !ok {
				v.reportError(ctx, path, errors.Errorf("object %v is backed by missing blob %v", oid, ci.PackBlobID))
				continue
			}
		}
	}

	//nolint:gomnd,gosec
	if rand.Intn(100) < *verifyCommandFilesPercent {
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

func runVerifyCommand(ctx context.Context, rep repo.Repository) error {
	if *verifyCommandAllSources {
		log(ctx).Noticef("DEPRECATED: --all-sources flag has no effect and is the default when no sources are provided.")
	}

	v := &verifier{
		rep:       rep,
		startTime: clock.Now(),
		workQueue: parallelwork.NewQueue(),
		seen:      map[object.ID]bool{},
	}

	if dr, ok := rep.(repo.DirectRepository); ok {
		blobMap, err := readBlobMap(ctx, dr.BlobReader())
		if err != nil {
			return err
		}

		v.blobMap = blobMap
	}

	if err := enqueueRootsToVerify(ctx, v, rep); err != nil {
		return err
	}

	v.workQueue.ProgressCallback = v.progressCallback
	if err := v.workQueue.Process(ctx, *verifyCommandParallel); err != nil {
		return errors.Wrap(err, "error processing work queue")
	}

	if len(v.errors) == 0 {
		return nil
	}

	return errors.Errorf("encountered %v errors", len(v.errors))
}

func enqueueRootsToVerify(ctx context.Context, v *verifier, rep repo.Repository) error {
	manifests, err := loadSourceManifests(ctx, rep, *verifyCommandSources)
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

	for _, oidStr := range *verifyCommandDirObjectIDs {
		oid, err := snapshotfs.ParseObjectIDWithPath(ctx, rep, oidStr)
		if err != nil {
			return errors.Wrapf(err, "unable to parse: %q", oidStr)
		}

		v.enqueueVerifyDirectory(ctx, oid, oidStr)
	}

	for _, oidStr := range *verifyCommandFileObjectIDs {
		oid, err := snapshotfs.ParseObjectIDWithPath(ctx, rep, oidStr)
		if err != nil {
			return errors.Wrapf(err, "unable to parse %q", oidStr)
		}

		v.enqueueVerifyObject(ctx, oid, oidStr)
	}

	return nil
}

func loadSourceManifests(ctx context.Context, rep repo.Repository, sources []string) ([]*snapshot.Manifest, error) {
	var manifestIDs []manifest.ID

	if len(sources)+len(*verifyCommandDirObjectIDs)+len(*verifyCommandFileObjectIDs) == 0 {
		man, err := snapshot.ListSnapshotManifests(ctx, rep, nil)
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
			man, err := snapshot.ListSnapshotManifests(ctx, rep, &src)
			if err != nil {
				return nil, errors.Wrapf(err, "unable to list snapshot manifests for %v", src)
			}
			manifestIDs = append(manifestIDs, man...)
		}
	}

	return snapshot.LoadSnapshots(ctx, rep, manifestIDs)
}

func init() {
	verifyCommand.Action(repositoryReaderAction(runVerifyCommand))
}
