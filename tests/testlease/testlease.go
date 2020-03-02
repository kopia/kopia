// Package testlease manages test leases in GCS bucket, as configured by environment
// variables.
package testlease

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

const (
	maxLeaseAttempts = 10

	leaseAttemptInterval = 15 * time.Second
	ownedLeaseWaitTime   = 30 * time.Second
)

var (
	once      sync.Once
	gcsBucket *storage.BucketHandle
)

type leaseData struct {
	Owner   string    `json:"owned"`
	Expires time.Time `json:"expires"`
}

func initStorageBucket(t *testing.T) *storage.BucketHandle {
	ctx := context.Background()

	bucket := os.Getenv("KOPIA_GCS_MUTEX_BUCKET")
	if bucket == "" {
		t.Logf("GCS mutex bucket not defined")
		return nil
	}

	credData, err := ioutil.ReadFile(os.Getenv("KOPIA_GCS_CREDENTIALS_FILE"))
	if err != nil {
		t.Logf("unable to open GCS credentials file")
		return nil
	}

	ts, err := tokenSourceFromCredentialsJSON(ctx, credData, storage.ScopeReadWrite)
	if err != nil {
		t.Logf("unable to get token source: %v", err)
		return nil
	}

	cli, err := storage.NewClient(ctx, option.WithTokenSource(ts))
	if err != nil {
		t.Logf("unable to initialize GCS client: %v", err)
		return nil
	}

	return cli.Bucket(bucket)
}

func getBucket(t *testing.T) *storage.BucketHandle {
	once.Do(func() {
		gcsBucket = initStorageBucket(t)
	})

	return gcsBucket
}

// RunWithLease executes the provided callback while holding a timed lease.
// The lease will be released when the callback returns or will automatically expire after specified time
// elapses.
func RunWithLease(t *testing.T, key string, leaseDuration time.Duration, cb func()) {
	b := getBucket(t)
	if b == nil {
		t.Logf("running without lease")
		cb()

		return
	}

	t.Logf("acquiring lease on GCS object %q", key)
	oh := b.Object(key)
	ctx := context.Background()

	myOwner := uuid.New().String()

	if err := tryAcquireLease(ctx, t, oh, myOwner, leaseDuration); err != nil {
		t.Fatalf("unable to acquire lease: %v", err)
	}

	defer releaseLeaseIfStillOwned(ctx, t, oh, myOwner)
	cb()
}

func tryAcquireLease(ctx context.Context, t *testing.T, oh *storage.ObjectHandle, myOwner string, dur time.Duration) error {
	var nextSleepAmount time.Duration
	for attempt := 0; attempt < maxLeaseAttempts; attempt++ {
		if nextSleepAmount > 0 {
			t.Logf("sleeping for %v", nextSleepAmount)
			time.Sleep(nextSleepAmount)
		}

		// set up default sleep amount, which may be overridden by code below
		nextSleepAmount = leaseAttemptInterval

		ok, err := conditionalWrite(ctx,
			oh,
			storage.Conditions{DoesNotExist: true},
			&leaseData{Owner: myOwner, Expires: time.Now().Add(dur)})

		if err != nil {
			// unable to write, retry
			continue
		}

		if ok {
			// write succeeded, we got a lease
			return nil
		}

		// object already existed, read-modify-write to grab the lease.
		ld, gen, err := readDataAndGeneration(ctx, oh)
		if err != nil {
			return errors.Wrap(err, "unable to read lease")
		}

		if ld.Owner != "" && time.Now().Before(ld.Expires) {
			// lease owned and non-expired
			t.Logf("lease currently owned by %v until %v", ld.Owner, ld.Expires)

			// don't wait full duration for the lease to expire, because it will be released
			// by its owner after actual work is done
			nextSleepAmount = time.Until(ld.Expires)
			if nextSleepAmount > ownedLeaseWaitTime {
				nextSleepAmount = ownedLeaseWaitTime
			}

			continue
		}

		if ld.Owner != "" {
			t.Logf("taking ownership of expired lease")
		}

		ok, err = conditionalWrite(ctx, oh,
			storage.Conditions{GenerationMatch: gen},
			&leaseData{Owner: myOwner, Expires: time.Now().Add(dur)})
		if ok {
			return nil
		}

		t.Logf("unable to update lease: %v", err)
	}

	return errors.Errorf("unable to acquire lease despite %v retries", maxLeaseAttempts)
}

func releaseLeaseIfStillOwned(ctx context.Context, t *testing.T, oh *storage.ObjectHandle, myOwner string) {
	// object already existed, read-modify-write to grab the lease.
	ld, gen, err := readDataAndGeneration(ctx, oh)
	if err != nil {
		t.Logf("WARNING: unable to read lease to release it: %v, will let it expire", err)
		return
	}

	if ld.Owner != myOwner {
		// lease not owned anymore
		return
	}

	t.Logf("releasing lease on GCS object %q", oh.ObjectName())

	ok, err := conditionalWrite(ctx, oh,
		storage.Conditions{GenerationMatch: gen},
		&leaseData{Owner: "", Expires: time.Now()})
	if !ok {
		t.Logf("WARNING: unable to release lease: %v, will let it expire", err)
	}
}

func conditionalWrite(ctx context.Context, oh *storage.ObjectHandle, cond storage.Conditions, ld *leaseData) (bool, error) {
	w := oh.If(cond).NewWriter(ctx)

	if err := json.NewEncoder(w).Encode(ld); err != nil {
		return false, err
	}

	if err := w.Close(); err != nil {
		if ae, ok := err.(*googleapi.Error); ok {
			if ae.Code == http.StatusPreconditionFailed {
				// already exists
				return false, nil
			}
		}

		return false, err
	}

	return true, nil
}

func readDataAndGeneration(ctx context.Context, oh *storage.ObjectHandle) (*leaseData, int64, error) {
	r, err := oh.NewReader(ctx)
	if err != nil {
		return nil, 0, errors.Wrap(err, "NewReader() error")
	}

	defer r.Close() //nolint:errcheck

	ld := &leaseData{}
	if err := json.NewDecoder(r).Decode(ld); err != nil {
		return nil, 0, errors.Wrap(err, "decode")
	}

	return ld, r.Attrs.Generation, nil
}

func tokenSourceFromCredentialsJSON(ctx context.Context, data []byte, scopes ...string) (oauth2.TokenSource, error) {
	cfg, err := google.JWTConfigFromJSON(data, scopes...)
	if err != nil {
		return nil, errors.Wrap(err, "google.JWTConfigFromJSON")
	}

	return cfg.TokenSource(ctx), nil
}
