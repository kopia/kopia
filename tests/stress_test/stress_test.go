package stress_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/kopia/repo/block"
	"github.com/kopia/repo/internal/storagetesting"
	"github.com/kopia/repo/storage"
)

const goroutineCount = 16

func TestStressBlockManager(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test during short tests")
	}

	data := map[string][]byte{}
	keyTimes := map[string]time.Time{}
	memst := storagetesting.NewMapStorage(data, keyTimes, time.Now)

	var duration = 3 * time.Second
	if os.Getenv("KOPIA_LONG_STRESS_TEST") != "" {
		duration = 3 * time.Minute
	}

	stressTestWithStorage(t, memst, duration)
}

func stressTestWithStorage(t *testing.T, st storage.Storage, duration time.Duration) {
	ctx := context.Background()

	openMgr := func() (*block.Manager, error) {
		return block.NewManager(ctx, st, block.FormattingOptions{
			Version:     1,
			Hash:        "HMAC-SHA256-128",
			Encryption:  "AES-256-CTR",
			MaxPackSize: 20000000,
			MasterKey:   []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
		}, block.CachingOptions{}, nil)
	}

	seed0 := time.Now().Nanosecond()

	t.Logf("running with seed %v", seed0)

	deadline := time.Now().Add(duration)

	t.Run("workers", func(t *testing.T) {
		for i := 0; i < goroutineCount; i++ {
			i := i
			t.Run(fmt.Sprintf("worker-%v", i), func(t *testing.T) {
				t.Parallel()
				stressWorker(ctx, t, deadline, i, openMgr, int64(seed0+i))
			})
		}
	})
}

func stressWorker(ctx context.Context, t *testing.T, deadline time.Time, workerID int, openMgr func() (*block.Manager, error), seed int64) {
	src := rand.NewSource(seed)
	rand := rand.New(src)

	bm, err := openMgr()
	if err != nil {
		t.Fatalf("error opening manager: %v", err)
	}

	type writtenBlock struct {
		contentID string
		data      []byte
	}

	var workerBlocks []writtenBlock

	for time.Now().Before(deadline) {
		l := rand.Intn(30000)
		data := make([]byte, l)
		if _, err := rand.Read(data); err != nil {
			t.Errorf("err: %v", err)
			return
		}
		dataCopy := append([]byte{}, data...)
		contentID, err := bm.WriteBlock(ctx, data, "")
		if err != nil {
			t.Errorf("err: %v", err)
			return
		}

		switch rand.Intn(20) {
		case 0:
			if err := bm.Flush(ctx); err != nil {
				t.Errorf("flush error: %v", err)
				return
			}
		case 1:
			if err := bm.Flush(ctx); err != nil {
				t.Errorf("flush error: %v", err)
				return
			}
			bm, err = openMgr()
			if err != nil {
				t.Errorf("error opening: %v", err)
				return
			}
		}

		//log.Printf("wrote %v", contentID)
		workerBlocks = append(workerBlocks, writtenBlock{contentID, dataCopy})
		if len(workerBlocks) > 5 {
			pos := rand.Intn(len(workerBlocks))
			previous := workerBlocks[pos]
			//log.Printf("reading %v", previous.contentID)
			d2, err := bm.GetBlock(ctx, previous.contentID)
			if err != nil {
				t.Errorf("error verifying block %q: %v", previous.contentID, err)
				return
			}
			if !reflect.DeepEqual(previous.data, d2) {
				t.Errorf("invalid previous data for %q %x %x", previous.contentID, d2, previous.data)
				return
			}
			workerBlocks = append(workerBlocks[0:pos], workerBlocks[pos+1:]...)
		}
	}
}
