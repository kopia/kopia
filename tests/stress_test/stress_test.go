package stress_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/kopia/kopia/repo/format"
)

const goroutineCount = 16

func TestStressBlockManager(t *testing.T) {
	if os.Getenv("KOPIA_STRESS_TEST") == "" {
		t.Skip("skipping stress test")
	}

	if testing.Short() {
		return
	}

	data := blobtesting.DataMap{}
	keyTimes := map[blob.ID]time.Time{}
	memst := blobtesting.NewMapStorage(data, keyTimes, clock.Now)

	duration := 3 * time.Second
	if os.Getenv("CI") != "" {
		duration = 30 * time.Second
	}

	stressTestWithStorage(t, memst, duration)
}

//nolint:thelper
func stressTestWithStorage(t *testing.T, st blob.Storage, duration time.Duration) {
	ctx := testlogging.Context(t)

	fop, err := format.NewFormattingOptionsProvider(&format.ContentFormat{
		Hash:       "HMAC-SHA256-128",
		Encryption: encryption.DefaultAlgorithm,
		MutableParameters: format.MutableParameters{
			Version:     1,
			MaxPackSize: 20000000,
		},
		MasterKey: []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
	}, nil)
	require.NoError(t, err)

	openMgr := func() (*content.WriteManager, error) {
		return content.NewManagerForTesting(ctx, st, fop, nil, nil)
	}

	seed0 := clock.Now().Nanosecond()

	t.Logf("running with seed %v", seed0)

	deadline := clock.Now().Add(duration)

	t.Run("workers", func(t *testing.T) {
		for i := range goroutineCount {
			t.Run(fmt.Sprintf("worker-%v", i), func(t *testing.T) {
				t.Parallel()
				stressWorker(ctx, t, deadline, openMgr, int64(seed0+i))
			})
		}
	})
}

//nolint:thelper
func stressWorker(ctx context.Context, t *testing.T, deadline time.Time, openMgr func() (*content.WriteManager, error), seed int64) {
	src := rand.NewSource(seed)
	rnd := rand.New(src)

	bm, err := openMgr()
	if err != nil {
		t.Fatalf("error opening manager: %v", err)
	}

	type writtenBlock struct {
		contentID content.ID
		data      []byte
	}

	var workerBlocks []writtenBlock

	for clock.Now().Before(deadline) {
		l := rnd.Intn(30000)
		data := make([]byte, l)

		if _, err := rnd.Read(data); err != nil {
			t.Errorf("err: %v", err)
			return
		}

		dataCopy := append([]byte{}, data...)

		contentID, err := bm.WriteContent(ctx, gather.FromSlice(data), "", content.NoCompression)
		if err != nil {
			t.Errorf("err: %v", err)
			return
		}

		switch rnd.Intn(20) {
		case 0:
			if ferr := bm.Flush(ctx); ferr != nil {
				t.Errorf("flush error: %v", ferr)
				return
			}
		case 1:
			if ferr := bm.Flush(ctx); ferr != nil {
				t.Errorf("flush error: %v", ferr)
				return
			}

			if cerr := bm.CloseShared(ctx); cerr != nil {
				t.Errorf("close error: %v", cerr)
				return
			}

			bm, err = openMgr()
			if err != nil {
				t.Errorf("error opening: %v", err)
				return
			}
		}

		workerBlocks = append(workerBlocks, writtenBlock{contentID, dataCopy})
		if len(workerBlocks) > 5 {
			pos := rnd.Intn(len(workerBlocks))
			previous := workerBlocks[pos]

			d2, err := bm.GetContent(ctx, previous.contentID)
			if err != nil {
				t.Errorf("error verifying content %q: %v", previous.contentID, err)
				return
			}

			if !bytes.Equal(previous.data, d2) {
				t.Errorf("invalid previous data for %q %x %x", previous.contentID, d2, previous.data)
				return
			}

			workerBlocks = append(workerBlocks[0:pos], workerBlocks[pos+1:]...)
		}
	}
}
