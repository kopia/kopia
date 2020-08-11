package blobtesting

import (
	cryptorand "crypto/rand"
	"encoding/hex"
	"fmt"
	"math/rand"
	"strings"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo/blob"
)

// ConcurrentAccessOptions encapsulates parameters for VerifyConcurrentAccess.
type ConcurrentAccessOptions struct {
	NumBlobs int // number of shared blos in the pool

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
func VerifyConcurrentAccess(t testingT, st blob.Storage, options ConcurrentAccessOptions) {
	t.Helper()

	// generate random blob IDs for the pool
	var blobs []blob.ID

	for i := 0; i < options.NumBlobs; i++ {
		blobIDBytes := make([]byte, 32)
		cryptorand.Read(blobIDBytes)
		blobs = append(blobs, blob.ID(hex.EncodeToString(blobIDBytes)))
	}

	randomBlobID := func() blob.ID {
		return blobs[rand.Intn(len(blobs))]
	}

	eg, ctx := errgroup.WithContext(testlogging.Context(t))

	// start readers that will be reading random blob out of the pool
	for i := 0; i < options.Getters; i++ {
		eg.Go(func() error {
			for i := 0; i < options.Iterations; i++ {
				blobID := randomBlobID()
				offset := int64(0)
				length := int64(-1)

				if rand.Intn(100) < options.RangeGetPercentage {
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
				err := st.PutBlob(ctx, blobID, gather.FromSlice([]byte(data)))
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
				if rand.Intn(100) < options.NonExistentListPrefixPercentage {
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
