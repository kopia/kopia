package cli

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/parallelwork"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
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
	verifyCommandAllSources     = verifyCommand.Flag("all-sources", "Verify all snapshots").Bool()
	verifyCommandSources        = verifyCommand.Flag("sources", "Verify the provided sources").Strings()
	verifyCommandParallel       = verifyCommand.Flag("parallel", "Parallelization").Default("16").Int()
	verifyCommandFilesPercent   = verifyCommand.Flag("verify-files-percent", "Randomly verify a percentage of files").Default("0").Int()
)

type verifier struct {
	rep       *repo.Repository
	om        *object.Manager
	workQueue *parallelwork.Queue
	startTime time.Time

	mu   sync.Mutex
	seen map[object.ID]bool

	errors []error
}

func (v *verifier) progressCallback(enqueued, active, completed int64) {
	elapsed := time.Since(v.startTime)
	maybeTimeRemaining := ""

	if elapsed > 1*time.Second && enqueued > 0 && completed > 0 {
		completedRatio := float64(completed) / float64(enqueued)
		predictedSeconds := elapsed.Seconds() / completedRatio
		predictedEndTime := v.startTime.Add(time.Duration(predictedSeconds) * time.Second)

		dt := time.Until(predictedEndTime)
		if dt > 0 {
			maybeTimeRemaining = fmt.Sprintf(" remaining %v (ETA %v)", dt.Truncate(1*time.Second), formatTimestamp(predictedEndTime.Truncate(1*time.Second)))
		}
	}

	printStderr("Found %v objects, verifying %v, completed %v objects%v.\n", enqueued, active, completed, maybeTimeRemaining)
}

func (v *verifier) tooManyErrors() bool {
	v.mu.Lock()
	defer v.mu.Unlock()

	if *verifyCommandErrorThreshold == 0 {
		return false
	}

	return len(v.errors) >= *verifyCommandErrorThreshold
}

func (v *verifier) reportError(path string, err error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	log.Warningf("failed on %v: %v", path, err)
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

	v.workQueue.EnqueueFront(func() error {
		return v.doVerifyDirectory(ctx, oid, path)
	})
}

func (v *verifier) enqueueVerifyObject(ctx context.Context, oid object.ID, path string) {
	// push to the back of the queue, so that we process non-directories at the end.
	if !v.shouldEnqueue(oid) {
		return
	}

	v.workQueue.EnqueueBack(func() error {
		return v.doVerifyObject(ctx, oid, path)
	})
}

func (v *verifier) doVerifyDirectory(ctx context.Context, oid object.ID, path string) error {
	log.Debugf("verifying directory %q (%v)", path, oid)

	d := snapshotfs.DirectoryEntry(v.rep, oid, nil)

	entries, err := d.Readdir(ctx)
	if err != nil {
		v.reportError(path, errors.Wrapf(err, "error reading %v", oid))
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
	log.Debugf("verifying object %v", oid)

	if _, err := v.om.VerifyObject(ctx, oid); err != nil {
		v.reportError(path, errors.Wrapf(err, "error verifying %v", oid))
	}

	if rand.Intn(100) < *verifyCommandFilesPercent {
		if err := v.readEntireObject(ctx, oid, path); err != nil {
			v.reportError(path, errors.Wrapf(err, "error reading object %v", oid))
		}
	}

	return nil
}

func (v *verifier) readEntireObject(ctx context.Context, oid object.ID, path string) error {
	log.Debugf("reading object %v %v", oid, path)

	ctx = content.UsingContentCache(ctx, false)

	// also read the entire file
	r, err := v.om.Open(ctx, oid)
	if err != nil {
		return err
	}
	defer r.Close() //nolint:errcheck

	_, err = io.Copy(ioutil.Discard, r)

	return err
}

func runVerifyCommand(ctx context.Context, rep *repo.Repository) error {
	v := &verifier{
		rep:       rep,
		om:        rep.Objects,
		startTime: time.Now(),
		workQueue: parallelwork.NewQueue(),
		seen:      map[object.ID]bool{},
	}

	if err := enqueueRootsToVerify(ctx, v, rep); err != nil {
		return err
	}

	v.workQueue.ProgressCallback = v.progressCallback
	if err := v.workQueue.Process(*verifyCommandParallel); err != nil {
		return errors.Wrap(err, "error processing work queue")
	}

	if len(v.errors) == 0 {
		return nil
	}

	return errors.Errorf("encountered %v errors", len(v.errors))
}

func enqueueRootsToVerify(ctx context.Context, v *verifier, rep *repo.Repository) error {
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
		oid, err := parseObjectID(ctx, rep, oidStr)
		if err != nil {
			return err
		}

		v.enqueueVerifyDirectory(ctx, oid, oidStr)
	}

	for _, oidStr := range *verifyCommandFileObjectIDs {
		oid, err := parseObjectID(ctx, rep, oidStr)
		if err != nil {
			return err
		}

		v.enqueueVerifyObject(ctx, oid, oidStr)
	}

	return nil
}

func loadSourceManifests(ctx context.Context, rep *repo.Repository, sources []string) ([]*snapshot.Manifest, error) {
	var manifestIDs []manifest.ID

	if *verifyCommandAllSources {
		man, err := snapshot.ListSnapshotManifests(ctx, rep, nil)
		if err != nil {
			return nil, err
		}

		manifestIDs = append(manifestIDs, man...)
	} else {
		for _, srcStr := range sources {
			src, err := snapshot.ParseSourceInfo(srcStr, getHostName(), getUserName())
			if err != nil {
				return nil, errors.Wrapf(err, "error parsing %q", srcStr)
			}
			man, err := snapshot.ListSnapshotManifests(ctx, rep, &src)
			if err != nil {
				return nil, err
			}
			manifestIDs = append(manifestIDs, man...)
		}
	}

	return snapshot.LoadSnapshots(ctx, rep, manifestIDs)
}

func init() {
	addUserAndHostFlags(verifyCommand)
	verifyCommand.Action(repositoryAction(runVerifyCommand))
}
