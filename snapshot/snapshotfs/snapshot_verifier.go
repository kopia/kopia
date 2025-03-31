package snapshotfs

import (
	"context"
	"encoding/json"
	"io"
	"math/rand"
	"runtime"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/iocopy"
	"github.com/kopia/kopia/internal/timetrack"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/repo/object"
)

var verifierLog = logging.Module("verifier")

type verifyFileWorkItem struct {
	oid       object.ID
	entryPath string
	size      int64
}

// Verifier allows efficient verification of large amounts of filesystem entries in parallel.
type Verifier struct {
	throttle timetrack.Throttle

	statsMu sync.RWMutex

	// +checklocks:statsMu
	queued int32
	// +checklocks:statsMu
	processed int32
	// +checklocks:statsMu
	processedBytes int64
	// +checklocks:statsMu
	readFiles int64
	// +checklocks:statsMu
	readBytes int64
	// +checklocks:statsMu
	expectedTotalObjects int64
	// +checklocks:statsMu
	expectedTotalFiles int64
	// +checklocks:statsMu
	expectedTotalBytes int64

	fileWorkQueue chan verifyFileWorkItem
	rep           repo.Repository
	opts          VerifierOptions
	workersWG     sync.WaitGroup

	blobMap map[blob.ID]blob.Metadata // when != nil, will check that each backing blob exists
}

// AddToExpectedTotals adds the provided values to the corresponding stat
// total values. If the caller can precompute the expected number
// of entries that will be iterated over, they can add them to
// the progress output.
func (v *Verifier) AddToExpectedTotals(objs, files, bytes int64) {
	v.statsMu.Lock()
	defer v.statsMu.Unlock()

	v.expectedTotalObjects += objs
	v.expectedTotalFiles += files
	v.expectedTotalBytes += bytes
}

func (v *Verifier) getStats() VerifierStats {
	v.statsMu.RLock()
	defer v.statsMu.RUnlock()

	return VerifierStats{
		ProcessedObjectCount: int64(v.processed),
		ProcessedBytes:       v.processedBytes,
		ReadFileCount:        v.readFiles,
		ReadBytes:            v.readBytes,
		ExpectedTotalObjects: v.expectedTotalObjects,
		ExpectedTotalFiles:   v.expectedTotalFiles,
		ExpectedTotalBytes:   v.expectedTotalBytes,
	}
}

const (
	inProgressStatsMsg = "Processed"
	finishedStatsMsg   = "Finished processing"
)

// ShowStats logs verification statistics.
func (v *Verifier) ShowStats(ctx context.Context) {
	v.showStatsf(ctx, inProgressStatsMsg)
}

// ShowFinalStats logs final verification statistics.
func (v *Verifier) ShowFinalStats(ctx context.Context) {
	v.showStatsf(ctx, finishedStatsMsg)
}

func (v *Verifier) showStatsf(ctx context.Context, msg string) {
	st := v.getStats()

	if v.opts.JSONStats {
		v.showStatsJSON(ctx, st)
		return
	}

	processed := st.ProcessedObjectCount
	processedBytes := st.ProcessedBytes
	readFiles := st.ReadFileCount
	readBytes := st.ReadBytes

	// "<message> <x> objects. Read <y> files (<z> MB)"
	verifierLog(ctx).Infof("%v %v objects (%v). Read %v files (%v).", msg, processed, units.BytesString(processedBytes), readFiles, units.BytesString(readBytes))
}

// VerifierStats contains stats on the amount of work done and current progress.
type VerifierStats struct {
	ProcessedObjectCount int64 `json:"processedObjectCount"`
	ProcessedBytes       int64 `json:"processedBytes"`
	ReadFileCount        int64 `json:"readFileCount"`
	ReadBytes            int64 `json:"readBytes"`
	ExpectedTotalObjects int64 `json:"expectedTotalObjectCount"`
	ExpectedTotalFiles   int64 `json:"expectedTotalFileCount"`
	ExpectedTotalBytes   int64 `json:"expectedTotalBytes"`
}

func (v *Verifier) showStatsJSON(ctx context.Context, st VerifierStats) {
	b, err := json.Marshal(st)
	if err != nil {
		verifierLog(ctx).Errorw("failed to marshal stats", "err", err, "stats", st)
		return
	}

	verifierLog(ctx).Infof("%s", b)
}

// VerifyFile verifies a single file object (using content check, blob map check or full read).
func (v *Verifier) VerifyFile(ctx context.Context, oid object.ID, entryPath string, size int64) error {
	verifierLog(ctx).Debugf("verifying object %v", oid)

	defer func() {
		v.updateProcessedStats(size)

		if v.throttle.ShouldOutput(time.Second) {
			v.ShowStats(ctx)
		}
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

func (v *Verifier) updateProcessedStats(size int64) {
	v.statsMu.Lock()
	defer v.statsMu.Unlock()

	v.processed++
	v.processedBytes += size
}

// verifyObject enqueues a single object for verification.
func (v *Verifier) verifyObject(ctx context.Context, e fs.Entry, oid object.ID, entryPath string) error {
	if v.throttle.ShouldOutput(time.Second) {
		v.ShowStats(ctx)
	}

	if !e.IsDir() {
		v.fileWorkQueue <- verifyFileWorkItem{oid, entryPath, e.Size()}

		v.statsMu.Lock()
		defer v.statsMu.Unlock()

		v.queued++
	} else {
		v.statsMu.Lock()
		defer v.statsMu.Unlock()

		v.queued++
		v.processed++
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

	n, err := iocopy.Copy(io.Discard, r)
	if err != nil {
		return errors.Wrap(err, "unable to read data")
	}

	v.statsMu.Lock()
	defer v.statsMu.Unlock()

	v.readBytes += n
	v.readFiles++

	return nil
}

// VerifierOptions provides options for the verifier.
type VerifierOptions struct {
	VerifyFilesPercent float64
	FileQueueLength    int
	Parallelism        int
	MaxErrors          int
	BlobMap            map[blob.ID]blob.Metadata
	JSONStats          bool
}

// VerifierResult returns results from the verifier.
type VerifierResult struct {
	Stats        VerifierStats `json:"stats"`
	ErrorCount   int           `json:"errorCount"`
	Errors       []error       `json:"-"`
	ErrorStrings []string      `json:"errorStrings,omitempty"`
}

// InParallel starts parallel verification and invokes the provided function
// which can call Process() on in the provided TreeWalker. Errors and stats
// are accumulated into a VerifierResult and returned, independent of whether
// the error return is nil, that is, `VerifierResult` will contain useful,
// partial stats when an error is returned, including a collection of errors
// found in the verification process.
func (v *Verifier) InParallel(ctx context.Context, enqueue func(tw *TreeWalker) error) (VerifierResult, error) {
	tw, twerr := NewTreeWalker(ctx, TreeWalkerOptions{
		Parallelism:   v.opts.Parallelism,
		EntryCallback: v.verifyObject,
		MaxErrors:     v.opts.MaxErrors,
	})
	if twerr != nil {
		return VerifierResult{}, errors.Wrap(twerr, "tree walker")
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

				if err := v.VerifyFile(ctx, wi.oid, wi.entryPath, wi.size); err != nil {
					tw.ReportError(ctx, wi.entryPath, err)
				}
			}
		}()
	}

	err := enqueue(tw)
	if err != nil {
		// Pass the enqueue error to the tree walker for later accumulation.
		tw.ReportError(ctx, "tree walker enqueue", err)
	}

	close(v.fileWorkQueue)
	v.workersWG.Wait()
	v.fileWorkQueue = nil

	twErrs, numErrors := tw.GetErrors()

	errStrs := make([]string, 0, len(twErrs))
	for _, twErr := range twErrs {
		errStrs = append(errStrs, twErr.Error())
	}

	// Return the tree walker error output along with result details.
	return VerifierResult{
		Stats:        v.getStats(),
		Errors:       twErrs,
		ErrorStrings: errStrs,
		ErrorCount:   numErrors,
	}, tw.Err()
}

// NewVerifier creates a verifier.
func NewVerifier(_ context.Context, rep repo.Repository, opts VerifierOptions) *Verifier {
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
