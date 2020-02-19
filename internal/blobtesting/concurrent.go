package blobtesting

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/kopia/kopia/repo/blob"
)

// ConcurrentAccessOptions encapsulates parameters for VerifyConcurrentAccess
type ConcurrentAccessOptions struct {
	Getters  int
	Putters  int
	Deleters int
	Listers  int

	Iterations int

	RangeGetPercentage              int // 0..100 - probability of issuing range get
	NonExistentListPrefixPercentage int // probability of issuing non-matching list prefix
}

// VerifyConcurrentAccess tests data races on a repository to ensure only clean errors are returned.
// nolint:gocognit,gocyclo,funlen
func VerifyConcurrentAccess(t *testing.T, st blob.Storage, options ConcurrentAccessOptions) {
	t.Helper()

	blobs := []blob.ID{
		"77066607-c5a2-45ea-adca-6a70032d016d",
		"9ece49fe-9c50-4f85-8907-7fe3c0727d75",
		"2ea38c02-95e7-4552-97ff-9c20f54e5050",
		"5e790b69-2c84-40ac-ad87-45eb0301a484",
		"fda4b2b5-252a-4822-af18-2cbcf677a7c7",
		"484e7bf5-444d-453a-bf56-d9dbe15ceb81",
		"1fd31065-3b5c-4d3a-ba2e-6b423a30051e",
		"c0742196-9c57-490f-b941-94812d85ef1f",
		"247f6f6b-2d96-459d-9dfa-5c987a8a328d",
		"268a350f-27d7-40a3-829b-0aa39d374a6e",
		"45f69e53-f5b2-4d85-b9cf-000ff53965b6",
		"9dec212a-8c37-4352-a42f-7b1467faedd8",
		"95e887b8-f583-48e5-a1eb-3ff59adf4b2d",
		"a1b64cca-d386-409b-a59b-58e4082c99d0",
		"87744c3b-c0dd-4276-a6c8-242dd894794a",
		"890bce5c-b106-4aa8-857f-5f989f2541c4",
	}

	randomBlobID := func() blob.ID {
		return blobs[rand.Intn(len(blobs))]
	}

	eg, ctx := errgroup.WithContext(context.Background())

	// start readers that will be reading random blob out of the pool
	for i := 0; i < options.Getters; i++ {
		eg.Go(func() error {
			for i := 0; i < options.Iterations; i++ {
				blobID := randomBlobID()
				offset := int64(0)
				length := int64(-1)

				if rand.Intn(100) < options.RangeGetPercentage { //nolint:gomnd
					offset = 10
					length = 3
				}

				data, err := st.GetBlob(ctx, blobID, offset, length)
				switch err {
				case nil:
					if got, want := string(data), string(blobID); !strings.HasPrefix(got, want) {
						return errors.Wrapf(err, "GetBlob returned invalid data for %v: %v, want prefix of %v", blobID, got, want)
					}

				case blob.ErrBlobNotFound:
					// clean error

				default:
					return errors.Wrapf(err, "GetBlob %v returned unexpected error", blobID)
				}
			}

			return nil
		})
	}

	// start putters that will be writing random blob out of the pool
	for i := 0; i < options.Putters; i++ {
		eg.Go(func() error {
			for i := 0; i < options.Iterations; i++ {
				blobID := randomBlobID()
				data := fmt.Sprintf("%v-%v", blobID, rand.Int63())
				err := st.PutBlob(ctx, blobID, []byte(data))
				switch err {
				case nil:
					// clean success

				default:
					return errors.Wrapf(err, "PutBlob %v returned unexpected error", blobID)
				}
			}

			return nil
		})
	}

	// start deleters that will be deleting random blob out of the pool
	for i := 0; i < options.Deleters; i++ {
		eg.Go(func() error {
			for i := 0; i < options.Iterations; i++ {
				blobID := randomBlobID()
				err := st.DeleteBlob(ctx, blobID)
				switch err {
				case nil:
					// clean success

				case blob.ErrBlobNotFound:
					// clean error

				default:
					return errors.Wrapf(err, "DeleteBlob %v returned unexpected error", blobID)
				}
			}

			return nil
		})
	}

	// start listers that will be listing blobs by random prefixes of existing objects.
	for i := 0; i < options.Listers; i++ {
		eg.Go(func() error {
			for i := 0; i < options.Iterations; i++ {
				blobID := randomBlobID()
				prefix := blobID[0:rand.Intn(len(blobID))]
				if rand.Intn(100) < options.NonExistentListPrefixPercentage { //nolint:gomnd
					prefix = "zzz"
				}

				err := st.ListBlobs(ctx, prefix, func(blob.Metadata) error {
					return nil
				})
				switch err {
				case nil:
					// clean success

				default:
					return errors.Wrapf(err, "ListBlobs(%v) returned unexpected error", prefix)
				}
			}

			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}
