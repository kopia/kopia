package snapshotfs

import (
	"context"
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
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/repo/object"
)

var verifierLog = logging.Module("verifier")

type verifyFileWorkItem struct {
	oid       object.ID
	entryPath string
}

// Verifier allows efficient verification of large amounts of filesystem entries in parallel.
type Verifier struct {
	throttle timetrack.Throttle

	queued    atomic.Int32
	processed atomic.Int32

	fileWorkQueue chan verifyFileWorkItem
	rep           repo.Repository
	opts          VerifierOptions
	workersWG     sync.WaitGroup

	blobMap map[blob.ID]blob.Metadata // when != nil, will check that each backing blob exists
}

// ShowStats logs verification statistics.
func (v *Verifier) ShowStats(ctx context.Context) {
	processed := v.processed.Load()

	verifierLog(ctx).Infof("Processed %v objects.", processed)
}

// ShowFinalStats logs final verification statistics.
func (v *Verifier) ShowFinalStats(ctx context.Context) {
	processed := v.processed.Load()

	verifierLog(ctx).Infof("Finished processing %v objects.", processed)
}

// VerifyFile verifies a single file object (using content check, blob map check or full read).
func (v *Verifier) VerifyFile(ctx context.Context, oid object.ID, entryPath string) error {
	verifierLog(ctx).Debugf("verifying object %v", oid)

	defer func() {
		v.processed.Add(1)
	}()

	contentIDs, err := v.rep.VerifyObject(ctx, oid)
	if err != nil {
		return errors.Wrap(err, "verify object")
	}

	if v.blobMap != nil {
		for _, cid := range contentIDs {
			ci, err := v.rep.ContentInfo(ctx, cid)
			if err != nil {
				return errors.Wrapf(err, "error verifying content %v", cid)
			}

			if _, ok := v.blobMap[ci.PackBlobID]; !ok {
				return errors.Errorf("object %v is backed by missing blob %v", oid, ci.PackBlobID)
			}
		}
	}

	//nolint:gosec
	if 100*rand.Float64() < v.opts.VerifyFilesPercent {
		if err := v.readEntireObject(ctx, oid, entryPath); err != nil {
			return errors.Wrapf(err, "error reading object %v", oid)
		}
	}

	return nil
}

// verifyObject enqueues a single object for verification.
func (v *Verifier) verifyObject(ctx context.Context, e fs.Entry, oid object.ID, entryPath string) error {
	if v.throttle.ShouldOutput(time.Second) {
		v.ShowStats(ctx)
	}

	if !e.IsDir() {
		v.fileWorkQueue <- verifyFileWorkItem{oid, entryPath}
		v.queued.Add(1)
	} else {
		v.queued.Add(1)
		v.processed.Add(1)
	}

	return nil
}

func (v *Verifier) readEntireObject(ctx context.Context, oid object.ID, path string) error {
	verifierLog(ctx).Debugf("reading object %v %v", oid, path)

	// read the entire file
	r, err := v.rep.OpenObject(ctx, oid)
	if err != nil {
		return errors.Wrapf(err, "unable to open object %v", oid)
	}
	defer r.Close() //nolint:errcheck

	return errors.Wrap(iocopy.JustCopy(io.Discard, r), "unable to read data")
}

// VerifierOptions provides options for the verifier.
type VerifierOptions struct {
	VerifyFilesPercent float64
	FileQueueLength    int
	Parallelism        int
	MaxErrors          int
	BlobMap            map[blob.ID]blob.Metadata
}

// InParallel starts parallel verification and invokes the provided function which can
// call Process() on in the provided TreeWalker.
func (v *Verifier) InParallel(ctx context.Context, enqueue func(tw *TreeWalker) error) error {
	tw, twerr := NewTreeWalker(ctx, TreeWalkerOptions{
		Parallelism:   v.opts.Parallelism,
		EntryCallback: v.verifyObject,
		MaxErrors:     v.opts.MaxErrors,
	})
	if twerr != nil {
		return errors.Wrap(twerr, "tree walker")
	}
	defer tw.Close(ctx)

	v.fileWorkQueue = make(chan verifyFileWorkItem, v.opts.FileQueueLength)

	for range v.opts.Parallelism {
		v.workersWG.Add(1)

		go func() {
			defer v.workersWG.Done()

			for wi := range v.fileWorkQueue {
				if tw.TooManyErrors() {
					continue
				}

				if err := v.VerifyFile(ctx, wi.oid, wi.entryPath); err != nil {
					tw.ReportError(ctx, wi.entryPath, err)
				}
			}
		}()
	}

	err := enqueue(tw)

	close(v.fileWorkQueue)
	v.workersWG.Wait()
	v.fileWorkQueue = nil

	if err != nil {
		return err
	}

	return tw.Err()
}

// NewVerifier creates a verifier.
func NewVerifier(ctx context.Context, rep repo.Repository, opts VerifierOptions) *Verifier {
	if opts.Parallelism == 0 {
		opts.Parallelism = runtime.NumCPU()
	}

	if opts.FileQueueLength == 0 {
		opts.FileQueueLength = 20000
	}

	return &Verifier{
		opts:    opts,
		rep:     rep,
		blobMap: opts.BlobMap,
	}
}
