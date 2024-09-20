// Package providervalidation implements validation to ensure the blob storage is compatible with Kopia requirements.
package providervalidation

import (
	"bytes"
	"context"
	"encoding/hex"
	stderrors "errors"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/logging"

	loggingwrapper "github.com/kopia/kopia/repo/blob/logging"
)

// Options provides options for provider validation.
type Options struct {
	MaxClockDrift           time.Duration
	ConcurrencyTestDuration time.Duration

	NumEquivalentStorageConnections int

	NumPutBlobWorkers     int
	NumGetBlobWorkers     int
	NumGetMetadataWorkers int
	NumListBlobsWorkers   int
	MaxBlobLength         int
}

// DefaultOptions is the default set of options.
//
//nolint:mnd,gochecknoglobals
var DefaultOptions = Options{
	MaxClockDrift:                   3 * time.Minute,
	ConcurrencyTestDuration:         30 * time.Second,
	NumEquivalentStorageConnections: 5,
	NumPutBlobWorkers:               3,
	NumGetBlobWorkers:               3,
	NumGetMetadataWorkers:           3,
	NumListBlobsWorkers:             3,
	MaxBlobLength:                   10e6,
}

var log = logging.Module("providervalidation")

// equivalentBlobStorageConnections is a slice of different instances of the same blob storage provider
// connecting to the same underlying storage.
type equivalentBlobStorageConnections []blob.Storage

func (st equivalentBlobStorageConnections) pickOne() blob.Storage {
	return st[rand.Intn(len(st))] //nolint:gosec
}

// closeAdditional closes all but the first connection to the underlying storage.
func (st equivalentBlobStorageConnections) closeAdditional(ctx context.Context) error {
	var err error

	for i := 1; i < len(st); i++ {
		err = stderrors.Join(err, st[i].Close(ctx))
	}

	return errors.Wrap(err, "error closing additional connections")
}

// openEquivalentStorageConnections creates n-1 additional connections to the same underlying storage
// and returns a slice of all connections.
func openEquivalentStorageConnections(ctx context.Context, st blob.Storage, n int) (equivalentBlobStorageConnections, error) {
	result := equivalentBlobStorageConnections{st}
	ci := st.ConnectionInfo()

	log(ctx).Infof("Opening %v equivalent storage connections...", n-1)

	for i := 1; i < n; i++ {
		c, err := blob.NewStorage(ctx, ci, false)
		if err != nil {
			if cerr := result.closeAdditional(ctx); cerr != nil {
				log(ctx).Warn("unable to close storage connection", "err", cerr)
			}

			return nil, errors.Wrap(err, "unable to open storage connection")
		}

		log(ctx).Debugw("opened equivalent storage connection", "connectionID", i)

		result = append(result, loggingwrapper.NewWrapper(c, log(ctx), fmt.Sprintf("[STORAGE-%v] ", i)))
	}

	return result, nil
}

// ValidateProvider runs a series of tests against provided storage to validate that
// it can be used with Kopia.
//
//nolint:mnd,funlen,gocyclo,cyclop
func ValidateProvider(ctx context.Context, st0 blob.Storage, opt Options) error {
	if os.Getenv("KOPIA_SKIP_PROVIDER_VALIDATION") != "" {
		return nil
	}

	st, err := openEquivalentStorageConnections(ctx, st0, opt.NumEquivalentStorageConnections)
	if err != nil {
		return errors.Wrap(err, "unable to open additional storage connections")
	}

	defer func() {
		if cerr := st.closeAdditional(ctx); cerr != nil {
			log(ctx).Warn("unable to close additional connections", "err", cerr)
		}
	}()

	uberPrefix := blob.ID("z" + uuid.NewString())
	defer cleanupAllBlobs(ctx, st[0], uberPrefix)

	prefix1 := uberPrefix + "a"
	prefix2 := uberPrefix + "b"

	log(ctx).Info("Validating storage capacity and usage")

	c, err := st.pickOne().GetCapacity(ctx)

	switch {
	case errors.Is(err, blob.ErrNotAVolume):
		// This is okay. We expect some implementations to not support this method.
	case c.FreeB > c.SizeB:
		return errors.Errorf("expected volume's free space (%dB) to be at most volume size (%dB)", c.FreeB, c.SizeB)
	case err != nil:
		return errors.Wrapf(err, "unexpected error")
	}

	log(ctx).Info("Validating blob list responses")

	if err := verifyBlobCount(ctx, st.pickOne(), uberPrefix, 0); err != nil {
		return errors.Wrap(err, "invalid blob count")
	}

	log(ctx).Info("Validating non-existent blob responses")

	var out gather.WriteBuffer
	defer out.Close()

	// read non-existent full blob
	if err := st.pickOne().GetBlob(ctx, prefix1+"1", 0, -1, &out); !errors.Is(err, blob.ErrBlobNotFound) {
		return errors.Errorf("got unexpected error when reading non-existent blob: %v", err)
	}

	// read non-existent partial blob
	if err := st.pickOne().GetBlob(ctx, prefix1+"1", 0, 5, &out); !errors.Is(err, blob.ErrBlobNotFound) {
		return errors.Errorf("got unexpected error when reading non-existent partial blob: %v", err)
	}

	// get metadata for non-existent blob
	if _, err := st.pickOne().GetMetadata(ctx, prefix1+"1"); !errors.Is(err, blob.ErrBlobNotFound) {
		return errors.Errorf("got unexpected error when getting metadata for non-existent blob: %v", err)
	}

	blobData := bytes.Repeat([]byte{1, 2, 3, 4, 5}, 1e6)

	log(ctx).Infof("Writing blob (%v bytes)", len(blobData))

	// write blob
	if err := st.pickOne().PutBlob(ctx, prefix1+"1", gather.FromSlice(blobData), blob.PutOptions{}); err != nil {
		return errors.Wrap(err, "error writing blob #1")
	}

	log(ctx).Info("Validating conditional creates...")

	err2 := st.pickOne().PutBlob(ctx, prefix1+"1", gather.FromSlice([]byte{99}), blob.PutOptions{DoNotRecreate: true})

	switch {
	case errors.Is(err2, blob.ErrUnsupportedPutBlobOption):
		// this is fine, server does not support DoNotRecreate
	case errors.Is(err2, blob.ErrBlobAlreadyExists):
		// this is fine, server honored DoNotRecreate, we will validate in a moment that they did not
		// in fact overwrite
	default:
		return errors.Errorf("unexpected error returned from PutBlob with DoNotRecreate: %v", err2)
	}

	log(ctx).Info("Validating list responses...")

	if err := verifyBlobCount(ctx, st.pickOne(), uberPrefix, 1); err != nil {
		return errors.Wrap(err, "invalid uber blob count")
	}

	if err := verifyBlobCount(ctx, st.pickOne(), prefix1, 1); err != nil {
		return errors.Wrap(err, "invalid blob count with prefix 1")
	}

	if err := verifyBlobCount(ctx, st.pickOne(), prefix2, 0); err != nil {
		return errors.Wrap(err, "invalid blob count with prefix 2")
	}

	log(ctx).Info("Validating partial reads...")

	partialBlobCases := []struct {
		offset int64
		length int64
	}{
		{0, 10},
		{1, 10},
		{2, 1},
		{5, 0},
		{int64(len(blobData)) - 5, 5},
	}

	for _, tc := range partialBlobCases {
		err := st.pickOne().GetBlob(ctx, prefix1+"1", tc.offset, tc.length, &out)
		if err != nil {
			return errors.Wrapf(err, "got unexpected error when reading partial blob @%v+%v", tc.offset, tc.length)
		}

		if got, want := out.ToByteSlice(), blobData[tc.offset:tc.offset+tc.length]; !bytes.Equal(got, want) {
			return errors.Errorf("got unexpected data after reading partial blob @%v+%v: %x, wanted %x", tc.offset, tc.length, got, want)
		}
	}

	log(ctx).Info("Validating full reads...")

	// read full blob
	err2 = st.pickOne().GetBlob(ctx, prefix1+"1", 0, -1, &out)
	if err2 != nil {
		return errors.Wrap(err2, "got unexpected error when reading partial blob")
	}

	if got, want := out.ToByteSlice(), blobData; !bytes.Equal(got, want) {
		return errors.Errorf("got unexpected data after reading partial blob: %x, wanted %x", got, want)
	}

	log(ctx).Info("Validating metadata...")

	// get metadata for non-existent blob
	bm, err2 := st.pickOne().GetMetadata(ctx, prefix1+"1")
	if err2 != nil {
		return errors.Wrap(err2, "got unexpected error when getting metadata for blob")
	}

	if got, want := bm.Length, int64(len(blobData)); got != want {
		return errors.Errorf("invalid length returned by GetMetadata(): %v, wanted %v", got, want)
	}

	now := clock.Now()

	timeDiff := now.Sub(bm.Timestamp)
	if timeDiff < 0 {
		timeDiff = -timeDiff
	}

	if timeDiff > opt.MaxClockDrift {
		return errors.Errorf(
			"newly-written blob has a timestamp very different from local clock: %v, expected %v. Max difference allowed is %v",
			bm.Timestamp,
			now,
			opt.MaxClockDrift,
		)
	}

	ct := newConcurrencyTest(st, prefix2, opt)
	log(ctx).Infof("Running concurrency test for %v...", opt.ConcurrencyTestDuration)

	if err := ct.run(ctx); err != nil {
		return errors.Wrap(err, "error validating concurrency")
	}

	log(ctx).Info("All good.")

	return nil
}

type concurrencyTest struct {
	opt      Options
	st       equivalentBlobStorageConnections
	prefix   blob.ID
	deadline time.Time

	mu sync.Mutex
	// +checklocks:mu
	blobSeeds map[blob.ID]int64
	// +checklocks:mu
	blobIDs []blob.ID
	// +checklocks:mu
	blobWritten map[blob.ID]bool
}

func newConcurrencyTest(st []blob.Storage, prefix blob.ID, opt Options) *concurrencyTest {
	return &concurrencyTest{
		opt:      opt,
		st:       st,
		prefix:   prefix,
		deadline: clock.Now().Add(opt.ConcurrencyTestDuration),

		blobSeeds:   make(map[blob.ID]int64),
		blobWritten: make(map[blob.ID]bool),
	}
}

func (c *concurrencyTest) dataFromSeed(seed int64, buf []byte) []byte {
	rnd := rand.New(rand.NewSource(seed)) //nolint:gosec
	length := rnd.Int31n(int32(len(buf))) //nolint:gosec
	result := buf[0:length]
	rnd.Read(result)

	return result
}

func (c *concurrencyTest) putBlobWorker(ctx context.Context, worker int) func() error {
	data0 := make([]byte, c.opt.MaxBlobLength)

	return func() error {
		for clock.Now().Before(c.deadline) {
			seed := rand.Int63() //nolint:gosec
			data := c.dataFromSeed(seed, data0)

			id := c.prefix + blob.ID(hex.EncodeToString(data[0:16]))

			c.mu.Lock()
			c.blobSeeds[id] = seed
			c.blobIDs = append(c.blobIDs, id)
			c.mu.Unlock()

			// sleep for a short time so that readers can start getting the blob when it's still
			// not written.
			c.randomSleep()

			log(ctx).Debugf("PutBlob worker %v writing %v (%v bytes)", worker, id, len(data))

			if err := c.st.pickOne().PutBlob(ctx, id, gather.FromSlice(data), blob.PutOptions{}); err != nil {
				return errors.Wrap(err, "error writing blob")
			}

			c.mu.Lock()
			c.blobWritten[id] = true
			c.mu.Unlock()

			log(ctx).Debugf("PutBlob worker %v finished writing %v (%v bytes)", worker, id, len(data))
		}

		return nil
	}
}

func (c *concurrencyTest) randomSleep() {
	time.Sleep(time.Duration(rand.Intn(int(500 * time.Millisecond)))) //nolint:gosec,mnd
}

func (c *concurrencyTest) pickBlob() (blob.ID, int64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.blobIDs) == 0 {
		return "", 0, false
	}

	id := c.blobIDs[rand.Intn(len(c.blobIDs))] //nolint:gosec

	return id, c.blobSeeds[id], c.blobWritten[id]
}

func (c *concurrencyTest) getBlobWorker(ctx context.Context, worker int) func() error {
	data0 := make([]byte, c.opt.MaxBlobLength)
	data1 := make([]byte, c.opt.MaxBlobLength)

	return func() error {
		var out gather.WriteBuffer
		defer out.Close()

		for clock.Now().Before(c.deadline) {
			c.randomSleep()

			blobID, blobSeed, fullyWritten := c.pickBlob()
			if blobID == "" {
				continue
			}

			log(ctx).Debugf("GetBlob worker %v reading %v", worker, blobID)

			err := c.st.pickOne().GetBlob(ctx, blobID, 0, -1, &out)
			if err != nil {
				if !errors.Is(err, blob.ErrBlobNotFound) || fullyWritten {
					return errors.Wrapf(err, "unexpected error when reading %v", blobID)
				}

				log(ctx).Debugf("GetBlob worker %v - valid error when reading %v", worker, blobID)

				continue
			}

			wantBytes := c.dataFromSeed(blobSeed, data0)
			gotBytes := out.Bytes().AppendToSlice(data1[:0])

			if !bytes.Equal(gotBytes, wantBytes) {
				return errors.Wrapf(err, "invalid data read for %v", blobID)
			}

			log(ctx).Debugf("GetBlob worker %v - valid data read %v", worker, blobID)
		}

		return nil
	}
}

func (c *concurrencyTest) getMetadataWorker(ctx context.Context, worker int) func() error {
	data0 := make([]byte, c.opt.MaxBlobLength)

	return func() error {
		for clock.Now().Before(c.deadline) {
			c.randomSleep()

			blobID, blobSeed, fullyWritten := c.pickBlob()
			if blobID == "" {
				continue
			}

			log(ctx).Debugf("GetMetadata worker %v: %v", worker, blobID)

			bm, err := c.st.pickOne().GetMetadata(ctx, blobID)
			if err != nil {
				if !errors.Is(err, blob.ErrBlobNotFound) || fullyWritten {
					return errors.Wrapf(err, "unexpected error when reading %v", blobID)
				}

				log(ctx).Debugf("GetMetadata worker %v - valid error when reading %v", worker, blobID)

				continue
			}

			blobData := c.dataFromSeed(blobSeed, data0)

			if bm.Length != int64(len(blobData)) {
				return errors.Wrapf(err, "unexpected partial read - invalid length read for %v: %v wanted %v", blobID, bm.Length, len(blobData))
			}

			log(ctx).Debugf("GetMetadata worker %v - valid data read %v", worker, blobID)
		}

		return nil
	}
}

func (c *concurrencyTest) listBlobWorker(ctx context.Context, worker int) func() error {
	// TODO: implement me
	_ = worker

	return func() error {
		return nil
	}
}

func (c *concurrencyTest) run(ctx context.Context) error {
	eg, ctx := errgroup.WithContext(ctx)

	for worker := range c.opt.NumPutBlobWorkers {
		eg.Go(c.putBlobWorker(ctx, worker))
	}

	for worker := range c.opt.NumGetBlobWorkers {
		eg.Go(c.getBlobWorker(ctx, worker))
	}

	for worker := range c.opt.NumGetMetadataWorkers {
		eg.Go(c.getMetadataWorker(ctx, worker))
	}

	for worker := range c.opt.NumListBlobsWorkers {
		eg.Go(c.listBlobWorker(ctx, worker))
	}

	return errors.Wrap(eg.Wait(), "encountered errors")
}

func cleanupAllBlobs(ctx context.Context, st blob.Storage, prefix blob.ID) {
	log(ctx).Info("Cleaning up temporary data...")

	if err := st.ListBlobs(ctx, prefix, func(bm blob.Metadata) error {
		return errors.Wrapf(st.DeleteBlob(ctx, bm.BlobID), "error deleting blob %v", bm.BlobID)
	}); err != nil {
		log(ctx).Debug("error cleaning up")
	}
}

func verifyBlobCount(ctx context.Context, st blob.Storage, prefix blob.ID, want int) error {
	got, err := blob.ListAllBlobs(ctx, st, prefix)
	if err != nil {
		return errors.Wrap(err, "error listing blobs")
	}

	if len(got) != want {
		return errors.Errorf("unexpected number of blobs returned for prefix %v: %v, wanted %v", prefix, len(got), want)
	}

	return nil
}
