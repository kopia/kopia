package content

import (
	"bytes"
	"context"
	"crypto/hmac"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	logging "github.com/op/go-logging"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/repo/blob"
)

const (
	maxPackSize = 2000
	maxRetries  = 100
)

var fakeTime = time.Date(2017, 1, 1, 0, 0, 0, 0, time.UTC)
var hmacSecret = []byte{1, 2, 3}

func init() {
	logging.SetLevel(logging.DEBUG, "")
}

func TestContentManagerEmptyFlush(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestContentManager(data, keyTime, nil)
	bm.Flush(ctx)

	if got, want := len(data), 0; got != want {
		t.Errorf("unexpected number of contents: %v, wanted %v", got, want)
	}
}

func TestContentZeroBytes1(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestContentManager(data, keyTime, nil)
	contentID := writeContentAndVerify(ctx, t, bm, []byte{})
	bm.Flush(ctx)

	if got, want := len(data), 2; got != want {
		t.Errorf("unexpected number of contents: %v, wanted %v", got, want)
	}

	dumpContentManagerData(t, data)
	bm = newTestContentManager(data, keyTime, nil)
	verifyContent(ctx, t, bm, contentID, []byte{})
}

func TestContentZeroBytes2(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestContentManager(data, keyTime, nil)
	writeContentAndVerify(ctx, t, bm, seededRandomData(10, 10))
	writeContentAndVerify(ctx, t, bm, []byte{})
	bm.Flush(ctx)

	if got, want := len(data), 2; got != want {
		t.Errorf("unexpected number of contents: %v, wanted %v", got, want)
		dumpContentManagerData(t, data)
	}
}

func TestContentManagerSmallContentWrites(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestContentManager(data, keyTime, nil)

	for i := 0; i < 100; i++ {
		writeContentAndVerify(ctx, t, bm, seededRandomData(i, 10))
	}

	if got, want := len(data), 0; got != want {
		t.Errorf("unexpected number of contents: %v, wanted %v", got, want)
	}

	bm.Flush(ctx)

	if got, want := len(data), 2; got != want {
		t.Errorf("unexpected number of contents: %v, wanted %v", got, want)
	}
}

func TestContentManagerDedupesPendingContents(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestContentManager(data, keyTime, nil)

	for i := 0; i < 100; i++ {
		writeContentAndVerify(ctx, t, bm, seededRandomData(0, 999))
	}

	if got, want := len(data), 0; got != want {
		t.Errorf("unexpected number of contents: %v, wanted %v", got, want)
	}

	bm.Flush(ctx)

	if got, want := len(data), 2; got != want {
		t.Errorf("unexpected number of contents: %v, wanted %v", got, want)
	}
}

func TestContentManagerDedupesPendingAndUncommittedContents(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestContentManager(data, keyTime, nil)

	// no writes here, all data fits in a single pack.
	writeContentAndVerify(ctx, t, bm, seededRandomData(0, 950))
	writeContentAndVerify(ctx, t, bm, seededRandomData(1, 950))
	writeContentAndVerify(ctx, t, bm, seededRandomData(2, 10))

	if got, want := len(data), 0; got != want {
		t.Errorf("unexpected number of contents: %v, wanted %v", got, want)
	}

	// no writes here
	writeContentAndVerify(ctx, t, bm, seededRandomData(0, 950))
	writeContentAndVerify(ctx, t, bm, seededRandomData(1, 950))
	writeContentAndVerify(ctx, t, bm, seededRandomData(2, 10))

	if got, want := len(data), 0; got != want {
		t.Errorf("unexpected number of contents: %v, wanted %v", got, want)
	}

	bm.Flush(ctx)

	// this flushes the pack content + index blob
	if got, want := len(data), 2; got != want {
		dumpContentManagerData(t, data)
		t.Errorf("unexpected number of contents: %v, wanted %v", got, want)
	}
}

func TestContentManagerEmpty(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestContentManager(data, keyTime, nil)

	noSuchContentID := ID(hashValue([]byte("foo")))

	b, err := bm.GetContent(ctx, noSuchContentID)
	if err != ErrContentNotFound {
		t.Errorf("unexpected error when getting non-existent content: %v, %v", b, err)
	}

	bi, err := bm.ContentInfo(ctx, noSuchContentID)
	if err != ErrContentNotFound {
		t.Errorf("unexpected error when getting non-existent content info: %v, %v", bi, err)
	}

	if got, want := len(data), 0; got != want {
		t.Errorf("unexpected number of contents: %v, wanted %v", got, want)
	}
}

func verifyActiveIndexBlobCount(ctx context.Context, t *testing.T, bm *Manager, expected int) {
	t.Helper()

	blks, err := bm.IndexBlobs(ctx)
	if err != nil {
		t.Errorf("error listing active index blobs: %v", err)
		return
	}

	if got, want := len(blks), expected; got != want {
		t.Errorf("unexpected number of active index blobs %v, expected %v (%v)", got, want, blks)
	}
}
func TestContentManagerInternalFlush(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestContentManager(data, keyTime, nil)

	for i := 0; i < 100; i++ {
		b := make([]byte, 25)
		cryptorand.Read(b) //nolint:errcheck
		writeContentAndVerify(ctx, t, bm, b)
	}

	// 1 data content written, but no index yet.
	if got, want := len(data), 1; got != want {
		t.Errorf("unexpected number of contents: %v, wanted %v", got, want)
	}

	// do it again - should be 2 contents + 1000 bytes pending.
	for i := 0; i < 100; i++ {
		b := make([]byte, 25)
		cryptorand.Read(b) //nolint:errcheck
		writeContentAndVerify(ctx, t, bm, b)
	}

	// 2 data contents written, but no index yet.
	if got, want := len(data), 2; got != want {
		t.Errorf("unexpected number of contents: %v, wanted %v", got, want)
	}

	bm.Flush(ctx)

	// third content gets written, followed by index.
	if got, want := len(data), 4; got != want {
		dumpContentManagerData(t, data)
		t.Errorf("unexpected number of contents: %v, wanted %v", got, want)
	}
}

func TestContentManagerWriteMultiple(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	timeFunc := fakeTimeNowWithAutoAdvance(fakeTime, 1*time.Second)
	bm := newTestContentManager(data, keyTime, timeFunc)

	var contentIDs []ID

	for i := 0; i < 5000; i++ {
		b := seededRandomData(i, i%113)

		blkID, err := bm.WriteContent(ctx, b, "")
		if err != nil {
			t.Errorf("err: %v", err)
		}

		contentIDs = append(contentIDs, blkID)

		if i%17 == 0 {
			if err := bm.Flush(ctx); err != nil {
				t.Fatalf("error flushing: %v", err)
			}
		}

		if i%41 == 0 {
			if err := bm.Flush(ctx); err != nil {
				t.Fatalf("error flushing: %v", err)
			}

			bm = newTestContentManager(data, keyTime, timeFunc)
		}

		pos := rand.Intn(len(contentIDs))
		if _, err := bm.GetContent(ctx, contentIDs[pos]); err != nil {
			dumpContentManagerData(t, data)
			t.Fatalf("can't read content %q: %v", contentIDs[pos], err)

			continue
		}
	}
}

// This is regression test for a bug where we would corrupt data when encryption
// was done in place and clobbered pending data in memory.
func TestContentManagerFailedToWritePack(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	st := blobtesting.NewMapStorage(data, keyTime, nil)
	faulty := &blobtesting.FaultyStorage{
		Base: st,
	}
	st = faulty

	bm, err := newManagerWithOptions(context.Background(), st, &FormattingOptions{
		Version:     1,
		Hash:        "HMAC-SHA256-128",
		Encryption:  "AES-256-CTR",
		MaxPackSize: maxPackSize,
		HMACSecret:  []byte("foo"),
		MasterKey:   []byte("0123456789abcdef0123456789abcdef"),
	}, CachingOptions{}, fakeTimeNowFrozen(fakeTime), nil)
	if err != nil {
		t.Fatalf("can't create bm: %v", err)
	}

	logging.SetLevel(logging.DEBUG, "faulty-storage")

	faulty.Faults = map[string][]*blobtesting.Fault{
		"PutContent": {
			{Err: errors.New("booboo")},
		},
	}

	b1, err := bm.WriteContent(ctx, seededRandomData(1, 10), "")
	if err != nil {
		t.Fatalf("can't create content: %v", err)
	}

	if err := bm.Flush(ctx); err != nil {
		t.Logf("expected flush error: %v", err)
	}

	verifyContent(ctx, t, bm, b1, seededRandomData(1, 10))
}

func TestContentManagerConcurrency(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestContentManager(data, keyTime, nil)
	preexistingContent := writeContentAndVerify(ctx, t, bm, seededRandomData(10, 100))
	bm.Flush(ctx)

	dumpContentManagerData(t, data)
	bm1 := newTestContentManager(data, keyTime, nil)
	bm2 := newTestContentManager(data, keyTime, nil)
	bm3 := newTestContentManager(data, keyTime, fakeTimeNowWithAutoAdvance(fakeTime.Add(1), 1*time.Second))

	// all bm* can see pre-existing content
	verifyContent(ctx, t, bm1, preexistingContent, seededRandomData(10, 100))
	verifyContent(ctx, t, bm2, preexistingContent, seededRandomData(10, 100))
	verifyContent(ctx, t, bm3, preexistingContent, seededRandomData(10, 100))

	// write the same content in all managers.
	sharedContent := writeContentAndVerify(ctx, t, bm1, seededRandomData(20, 100))
	writeContentAndVerify(ctx, t, bm2, seededRandomData(20, 100))
	writeContentAndVerify(ctx, t, bm3, seededRandomData(20, 100))

	// write unique content per manager.
	bm1content := writeContentAndVerify(ctx, t, bm1, seededRandomData(31, 100))
	bm2content := writeContentAndVerify(ctx, t, bm2, seededRandomData(32, 100))
	bm3content := writeContentAndVerify(ctx, t, bm3, seededRandomData(33, 100))

	// make sure they can't see each other's unflushed contents.
	verifyContentNotFound(ctx, t, bm1, bm2content)
	verifyContentNotFound(ctx, t, bm1, bm3content)
	verifyContentNotFound(ctx, t, bm2, bm1content)
	verifyContentNotFound(ctx, t, bm2, bm3content)
	verifyContentNotFound(ctx, t, bm3, bm1content)
	verifyContentNotFound(ctx, t, bm3, bm2content)

	// now flush all writers, they still can't see each others' data.
	bm1.Flush(ctx)
	bm2.Flush(ctx)
	bm3.Flush(ctx)
	verifyContentNotFound(ctx, t, bm1, bm2content)
	verifyContentNotFound(ctx, t, bm1, bm3content)
	verifyContentNotFound(ctx, t, bm2, bm1content)
	verifyContentNotFound(ctx, t, bm2, bm3content)
	verifyContentNotFound(ctx, t, bm3, bm1content)
	verifyContentNotFound(ctx, t, bm3, bm2content)

	// new content manager at this point can see all data.
	bm4 := newTestContentManager(data, keyTime, nil)
	verifyContent(ctx, t, bm4, preexistingContent, seededRandomData(10, 100))
	verifyContent(ctx, t, bm4, sharedContent, seededRandomData(20, 100))
	verifyContent(ctx, t, bm4, bm1content, seededRandomData(31, 100))
	verifyContent(ctx, t, bm4, bm2content, seededRandomData(32, 100))
	verifyContent(ctx, t, bm4, bm3content, seededRandomData(33, 100))

	if got, want := getIndexCount(data), 4; got != want {
		t.Errorf("unexpected index count before compaction: %v, wanted %v", got, want)
	}

	if err := bm4.CompactIndexes(ctx, CompactOptions{MaxSmallBlobs: 1}); err != nil {
		t.Errorf("compaction error: %v", err)
	}

	if got, want := getIndexCount(data), 1; got != want {
		t.Errorf("unexpected index count after compaction: %v, wanted %v", got, want)
	}

	// new content manager at this point can see all data.
	bm5 := newTestContentManager(data, keyTime, nil)
	verifyContent(ctx, t, bm5, preexistingContent, seededRandomData(10, 100))
	verifyContent(ctx, t, bm5, sharedContent, seededRandomData(20, 100))
	verifyContent(ctx, t, bm5, bm1content, seededRandomData(31, 100))
	verifyContent(ctx, t, bm5, bm2content, seededRandomData(32, 100))
	verifyContent(ctx, t, bm5, bm3content, seededRandomData(33, 100))

	if err := bm5.CompactIndexes(ctx, CompactOptions{MaxSmallBlobs: 1}); err != nil {
		t.Errorf("compaction error: %v", err)
	}
}

func TestDeleteContent(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestContentManager(data, keyTime, nil)

	content1 := writeContentAndVerify(ctx, t, bm, seededRandomData(10, 100))

	if err := bm.Flush(ctx); err != nil {
		t.Fatalf("error flushing: %v", err)
	}

	dumpContents(t, bm, "after first flush")

	content2 := writeContentAndVerify(ctx, t, bm, seededRandomData(11, 100))

	log.Infof("xxx deleting.")

	if err := bm.DeleteContent(content1); err != nil {
		t.Fatalf("unable to delete content %v: %v", content1, err)
	}

	log.Infof("yyy deleting.")

	if err := bm.DeleteContent(content2); err != nil {
		t.Fatalf("unable to delete content %v: %v", content2, err)
	}

	verifyContentNotFound(ctx, t, bm, content1)
	verifyContentNotFound(ctx, t, bm, content2)
	log.Infof("flushing")
	bm.Flush(ctx)
	log.Infof("flushed")
	log.Debugf("-----------")

	bm = newTestContentManager(data, keyTime, nil)
	verifyContentNotFound(ctx, t, bm, content1)
	verifyContentNotFound(ctx, t, bm, content2)
}

func TestParallelWrites(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	st := blobtesting.NewMapStorage(data, keyTime, nil)

	// set up fake storage that is slow at PutBlob causing writes to be piling up
	fs := &blobtesting.FaultyStorage{
		Base: st,
		Faults: map[string][]*blobtesting.Fault{
			"PutBlob": {
				{
					Repeat: 1000000000,
					Sleep:  1 * time.Second,
				},
			},
		},
	}

	var workersWG sync.WaitGroup

	var workerLock sync.RWMutex

	bm := newTestContentManagerWithStorage(fs, nil)
	numWorkers := 8
	closeWorkers := make(chan bool)

	// workerLock allows workers to append to their own list of IDs (when R-locked) in parallel.
	// W-lock allows flusher to capture the state without any worker being able to modify it.
	workerWritten := make([][]ID, numWorkers)

	// start numWorkers, each writing random block and recording it
	for workerID := 0; workerID < numWorkers; workerID++ {
		workerID := workerID

		workersWG.Add(1)

		go func() {
			defer workersWG.Done()

			for {
				select {
				case <-closeWorkers:
					return
				case <-time.After(1 * time.Nanosecond):
					id := writeContentAndVerify(ctx, t, bm, seededRandomData(rand.Int(), 100)) //nolint:gosec

					workerLock.RLock()
					workerWritten[workerID] = append(workerWritten[workerID], id)
					workerLock.RUnlock()
				}
			}
		}()
	}

	closeFlusher := make(chan bool)

	var flusherWG sync.WaitGroup

	flusherWG.Add(1)

	go func() {
		defer flusherWG.Done()

		for {
			select {
			case <-closeFlusher:
				log.Infof("closing flusher goroutine")
				return
			case <-time.After(2 * time.Second):
				log.Infof("about to flush")

				// capture snapshot of all content IDs while holding a writer lock
				allWritten := map[ID]bool{}

				workerLock.Lock()

				for _, ww := range workerWritten {
					for _, id := range ww {
						allWritten[id] = true
					}
				}

				workerLock.Unlock()

				log.Infof("captured %v contents", len(allWritten))

				if err := bm.Flush(ctx); err != nil {
					t.Errorf("flush error: %v", err)
				}

				// open new content manager and verify all contents are visible there.
				verifyAllDataPresent(t, data, allWritten)
			}
		}
	}()

	// run workers and flushers for some time, enough for 2 flushes to complete
	time.Sleep(5 * time.Second)

	// shut down workers and wait for them
	close(closeWorkers)
	workersWG.Wait()

	close(closeFlusher)
	flusherWG.Wait()
}

func TestFlushResumesWriters(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	st := blobtesting.NewMapStorage(data, keyTime, nil)

	// set up fake storage that is slow at PutBlob causing writes to be piling up
	fs := &blobtesting.FaultyStorage{
		Base: st,
		Faults: map[string][]*blobtesting.Fault{
			"PutBlob": {
				{
					Repeat: 1000000000,
					Sleep:  3 * time.Second,
				},
			},
		},
	}

	bm := newTestContentManagerWithStorage(fs, nil)
	first := writeContentAndVerify(ctx, t, bm, []byte{1, 2, 3})

	var second ID

	var writeWG sync.WaitGroup

	writeWG.Add(1)

	go func() {
		defer writeWG.Done()

		// start a write while flush is ongoing, the write will block on the condition variable
		time.Sleep(1 * time.Second)
		log.Infof("write started")

		second = writeContentAndVerify(ctx, t, bm, []byte{3, 4, 5})

		log.Infof("write finished")
	}()

	// flush will take 5 seconds, 1 second into that we will start a write
	bm.Flush(ctx)

	// wait for write to complete, if this times out, Flush() is not waking up writers
	writeWG.Wait()

	verifyAllDataPresent(t, data, map[ID]bool{
		first: true,
	})

	// flush again, this will include buffer
	bm.Flush(ctx)

	verifyAllDataPresent(t, data, map[ID]bool{
		first:  true,
		second: true,
	})
}

func verifyAllDataPresent(t *testing.T, data map[blob.ID][]byte, contentIDs map[ID]bool) {
	bm := newTestContentManager(data, nil, nil)
	_ = bm.IterateContents(IterateOptions{}, func(ci Info) error {
		delete(contentIDs, ci.ID)
		return nil
	})

	if len(contentIDs) != 0 {
		t.Errorf("some blocks not written: %v", contentIDs)
	}
}

func TestHandleWriteErrors(t *testing.T) {
	ctx := context.Background()

	// genFaults(S0,F0,S1,F1,...,) generates a list of faults
	// where success is returned Sn times followed by failure returned Fn times
	genFaults := func(counts ...int) []*blobtesting.Fault {
		var result []*blobtesting.Fault

		for i, cnt := range counts {
			if i%2 == 0 {
				result = append(result, &blobtesting.Fault{
					Repeat: cnt - 1,
				})
			} else {
				result = append(result, &blobtesting.Fault{
					Repeat: cnt - 1,
					Err:    errors.Errorf("some write error"),
				})
			}
		}

		return result
	}

	// simulate a stream of PutBlob failures, write some contents followed by flush
	// count how many times we retried writes/flushes
	// also, verify that all the data is durable
	cases := []struct {
		faults               []*blobtesting.Fault // failures to similuate
		numContents          int                  // how many contents to write
		contentSize          int                  // size of each content
		expectedFlushRetries int
		expectedWriteRetries int
	}{
		{faults: genFaults(0, 10, 10, 10, 10, 10, 10, 10, 10, 10), numContents: 5, contentSize: maxPackSize, expectedWriteRetries: 10, expectedFlushRetries: 0},
		{faults: genFaults(1, 2), numContents: 1, contentSize: maxPackSize, expectedWriteRetries: 0, expectedFlushRetries: 2},
		{faults: genFaults(1, 2), numContents: 10, contentSize: maxPackSize, expectedWriteRetries: 2, expectedFlushRetries: 0},
		// 2 failures, 2 successes (pack blobs), 1 failure (flush), 1 success (flush)
		{faults: genFaults(0, 2, 2, 1, 1, 1, 1), numContents: 2, contentSize: maxPackSize, expectedWriteRetries: 2, expectedFlushRetries: 1},
		{faults: genFaults(0, 2, 2, 1, 1, 1, 1), numContents: 4, contentSize: maxPackSize / 2, expectedWriteRetries: 2, expectedFlushRetries: 1},
	}

	for n, tc := range cases {
		tc := tc

		t.Run(fmt.Sprintf("case-%v", n), func(t *testing.T) {
			data := blobtesting.DataMap{}
			keyTime := map[blob.ID]time.Time{}
			st := blobtesting.NewMapStorage(data, keyTime, nil)

			// set up fake storage that is slow at PutBlob causing writes to be piling up
			fs := &blobtesting.FaultyStorage{
				Base: st,
				Faults: map[string][]*blobtesting.Fault{
					"PutBlob": tc.faults,
				},
			}

			bm := newTestContentManagerWithStorage(fs, nil)
			writeRetries := 0
			var cids []ID
			for i := 0; i < tc.numContents; i++ {
				cid, retries := writeContentWithRetriesAndVerify(ctx, t, bm, seededRandomData(i, tc.contentSize))
				writeRetries += retries
				cids = append(cids, cid)
			}
			if got, want := flushWithRetries(ctx, t, bm), tc.expectedFlushRetries; got != want {
				t.Errorf("invalid # of flush retries %v, wanted %v", got, want)
			}
			if got, want := writeRetries, tc.expectedWriteRetries; got != want {
				t.Errorf("invalid # of write retries %v, wanted %v", got, want)
			}
			bm2 := newTestContentManagerWithStorage(st, nil)
			for i, cid := range cids {
				verifyContent(ctx, t, bm2, cid, seededRandomData(i, tc.contentSize))
			}
		})
	}
}

func TestRewriteNonDeleted(t *testing.T) {
	const stepBehaviors = 3

	// perform a sequence WriteContent() <action1> RewriteContent() <action2> GetContent()
	// where actionX can be (0=flush and reopen, 1=flush, 2=nothing)
	for action1 := 0; action1 < stepBehaviors; action1++ {
		for action2 := 0; action2 < stepBehaviors; action2++ {
			action1 := action1
			action2 := action2

			t.Run(fmt.Sprintf("case-%v-%v", action1, action2), func(t *testing.T) {
				ctx := context.Background()
				data := blobtesting.DataMap{}
				keyTime := map[blob.ID]time.Time{}
				fakeNow := fakeTimeNowWithAutoAdvance(fakeTime, 1*time.Second)
				bm := newTestContentManager(data, keyTime, fakeNow)

				applyStep := func(action int) {
					switch action {
					case 0:
						t.Logf("flushing and reopening")
						bm.Flush(ctx)
						bm = newTestContentManager(data, keyTime, fakeNow)
					case 1:
						t.Logf("flushing")
						bm.Flush(ctx)
					case 2:
						t.Logf("doing nothing")
					}
				}

				content1 := writeContentAndVerify(ctx, t, bm, seededRandomData(10, 100))
				applyStep(action1)
				assertNoError(t, bm.RewriteContent(ctx, content1))
				applyStep(action2)
				verifyContent(ctx, t, bm, content1, seededRandomData(10, 100))
				dumpContentManagerData(t, data)
			})
		}
	}
}

func TestDisableFlush(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestContentManager(data, keyTime, nil)
	bm.DisableIndexFlush()
	bm.DisableIndexFlush()

	for i := 0; i < 500; i++ {
		writeContentAndVerify(ctx, t, bm, seededRandomData(i, 100))
	}
	bm.Flush(ctx) // flush will not have effect
	bm.EnableIndexFlush()
	bm.Flush(ctx) // flush will not have effect
	bm.EnableIndexFlush()

	verifyActiveIndexBlobCount(ctx, t, bm, 0)
	bm.EnableIndexFlush()
	verifyActiveIndexBlobCount(ctx, t, bm, 0)
	bm.Flush(ctx) // flush will happen now
	verifyActiveIndexBlobCount(ctx, t, bm, 1)
}

func TestRewriteDeleted(t *testing.T) {
	const stepBehaviors = 3

	// perform a sequence WriteContent() <action1> Delete() <action2> RewriteContent() <action3> GetContent()
	// where actionX can be (0=flush and reopen, 1=flush, 2=nothing)
	for action1 := 0; action1 < stepBehaviors; action1++ {
		for action2 := 0; action2 < stepBehaviors; action2++ {
			for action3 := 0; action3 < stepBehaviors; action3++ {
				action1 := action1
				action2 := action2
				action3 := action3
				t.Run(fmt.Sprintf("case-%v-%v-%v", action1, action2, action3), func(t *testing.T) {
					ctx := context.Background()
					data := blobtesting.DataMap{}
					keyTime := map[blob.ID]time.Time{}
					fakeNow := fakeTimeNowWithAutoAdvance(fakeTime, 1*time.Second)
					bm := newTestContentManager(data, keyTime, fakeNow)

					applyStep := func(action int) {
						switch action {
						case 0:
							t.Logf("flushing and reopening")
							bm.Flush(ctx)
							bm = newTestContentManager(data, keyTime, fakeNow)
						case 1:
							t.Logf("flushing")
							bm.Flush(ctx)
						case 2:
							t.Logf("doing nothing")
						}
					}

					content1 := writeContentAndVerify(ctx, t, bm, seededRandomData(10, 100))
					applyStep(action1)
					assertNoError(t, bm.DeleteContent(content1))
					applyStep(action2)
					if got, want := bm.RewriteContent(ctx, content1), ErrContentNotFound; got != want && got != nil {
						t.Errorf("unexpected error %v, wanted %v", got, want)
					}
					applyStep(action3)
					verifyContentNotFound(ctx, t, bm, content1)
					dumpContentManagerData(t, data)
				})
			}
		}
	}
}

func TestDeleteAndRecreate(t *testing.T) {
	ctx := context.Background()
	// simulate race between delete/recreate and delete
	// delete happens at t0+10, recreate at t0+20 and second delete time is parameterized.
	// depending on it, the second delete results will be visible.
	cases := []struct {
		desc         string
		deletionTime time.Time
		isVisible    bool
	}{
		{"deleted before delete and-recreate", fakeTime.Add(5 * time.Second), true},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			// write a content
			data := blobtesting.DataMap{}
			keyTime := map[blob.ID]time.Time{}
			bm := newTestContentManager(data, keyTime, fakeTimeNowFrozen(fakeTime))
			content1 := writeContentAndVerify(ctx, t, bm, seededRandomData(10, 100))
			bm.Flush(ctx)

			// delete but at given timestamp but don't commit yet.
			bm0 := newTestContentManager(data, keyTime, fakeTimeNowWithAutoAdvance(tc.deletionTime, 1*time.Second))
			assertNoError(t, bm0.DeleteContent(content1))

			// delete it at t0+10
			bm1 := newTestContentManager(data, keyTime, fakeTimeNowWithAutoAdvance(fakeTime.Add(10*time.Second), 1*time.Second))
			verifyContent(ctx, t, bm1, content1, seededRandomData(10, 100))
			assertNoError(t, bm1.DeleteContent(content1))
			bm1.Flush(ctx)

			// recreate at t0+20
			bm2 := newTestContentManager(data, keyTime, fakeTimeNowWithAutoAdvance(fakeTime.Add(20*time.Second), 1*time.Second))
			content2 := writeContentAndVerify(ctx, t, bm2, seededRandomData(10, 100))
			bm2.Flush(ctx)

			// commit deletion from bm0 (t0+5)
			bm0.Flush(ctx)

			if content1 != content2 {
				t.Errorf("got invalid content %v, expected %v", content2, content1)
			}

			bm3 := newTestContentManager(data, keyTime, nil)
			dumpContentManagerData(t, data)
			if tc.isVisible {
				verifyContent(ctx, t, bm3, content1, seededRandomData(10, 100))
			} else {
				verifyContentNotFound(ctx, t, bm3, content1)
			}
		})
	}
}

// nolint:funlen
func TestIterateContents(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestContentManager(data, keyTime, nil)
	// flushed, non-deleted
	contentID1 := writeContentAndVerify(ctx, t, bm, seededRandomData(10, 100))

	// flushed, deleted
	contentID2 := writeContentAndVerify(ctx, t, bm, seededRandomData(11, 100))
	bm.Flush(ctx)

	if err := bm.DeleteContent(contentID2); err != nil {
		t.Errorf("error deleting content 2 %v", err)
	}

	// pending, non-deleted
	contentID3 := writeContentAndVerify(ctx, t, bm, seededRandomData(12, 100))

	// pending, deleted - is completely discarded
	contentID4 := writeContentAndVerify(ctx, t, bm, seededRandomData(13, 100))
	if err := bm.DeleteContent(contentID4); err != nil {
		t.Fatalf("error deleting content 4 %v", err)
	}

	t.Logf("contentID1: %v", contentID1)
	t.Logf("contentID2: %v", contentID2)
	t.Logf("contentID3: %v", contentID3)
	t.Logf("contentID4: %v", contentID4)

	someError := errors.New("some error")
	cases := []struct {
		desc    string
		options IterateOptions
		want    map[ID]bool
		fail    error
	}{
		{
			desc:    "default options",
			options: IterateOptions{},
			want:    map[ID]bool{contentID1: true, contentID3: true},
		},
		{
			desc:    "include deleted",
			options: IterateOptions{IncludeDeleted: true},
			want: map[ID]bool{
				contentID1: true,
				contentID2: true,
				contentID3: true,
			},
		},
		{
			desc: "parallel",
			options: IterateOptions{
				Parallel: 10,
			},
			want: map[ID]bool{
				contentID1: true,
				contentID3: true,
			},
		},
		{
			desc:    "failure",
			options: IterateOptions{},
			fail:    someError,
			want:    map[ID]bool{},
		},
		{
			desc: "failure-parallel",
			options: IterateOptions{
				Parallel: 10,
			},
			fail: someError,
			want: map[ID]bool{},
		},
		{
			desc: "parallel, include deleted",
			options: IterateOptions{
				Parallel:       10,
				IncludeDeleted: true,
			},
			want: map[ID]bool{
				contentID1: true,
				contentID2: true,
				contentID3: true,
			},
		},
		{
			desc: "prefix match",
			options: IterateOptions{
				Prefix: contentID1,
			},
			want: map[ID]bool{contentID1: true},
		},
		{
			desc: "prefix, include deleted",
			options: IterateOptions{
				Prefix:         contentID2,
				IncludeDeleted: true,
			},
			want: map[ID]bool{
				contentID2: true,
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			var mu sync.Mutex
			got := map[ID]bool{}

			err := bm.IterateContents(tc.options, func(ci Info) error {
				if tc.fail != nil {
					return tc.fail
				}

				mu.Lock()
				got[ci.ID] = true
				mu.Unlock()
				return nil
			})

			if tc.fail != err {
				t.Errorf("error iterating: %v", err)
			}

			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("invalid content IDs got: %v, want %v", got, tc.want)
			}
		})
	}
}

func TestFindUnreferencedBlobs(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestContentManager(data, keyTime, nil)
	verifyUnreferencedBlobsCount(ctx, t, bm, 0)
	contentID := writeContentAndVerify(ctx, t, bm, seededRandomData(10, 100))

	log.Infof("flushing")

	if err := bm.Flush(ctx); err != nil {
		t.Errorf("flush error: %v", err)
	}

	dumpContents(t, bm, "after flush #1")
	dumpContentManagerData(t, data)
	verifyUnreferencedBlobsCount(ctx, t, bm, 0)

	if err := bm.DeleteContent(contentID); err != nil {
		t.Errorf("error deleting content: %v", contentID)
	}

	if err := bm.Flush(ctx); err != nil {
		t.Errorf("flush error: %v", err)
	}

	dumpContents(t, bm, "after flush #2")
	dumpContentManagerData(t, data)
	// content still present in first pack
	verifyUnreferencedBlobsCount(ctx, t, bm, 0)

	assertNoError(t, bm.RewriteContent(ctx, contentID))

	if err := bm.Flush(ctx); err != nil {
		t.Errorf("flush error: %v", err)
	}

	verifyUnreferencedBlobsCount(ctx, t, bm, 1)
	assertNoError(t, bm.RewriteContent(ctx, contentID))

	if err := bm.Flush(ctx); err != nil {
		t.Errorf("flush error: %v", err)
	}

	verifyUnreferencedBlobsCount(ctx, t, bm, 2)
}

func TestFindUnreferencedBlobs2(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestContentManager(data, keyTime, nil)
	verifyUnreferencedBlobsCount(ctx, t, bm, 0)
	contentID := writeContentAndVerify(ctx, t, bm, seededRandomData(10, 100))
	writeContentAndVerify(ctx, t, bm, seededRandomData(11, 100))
	dumpContents(t, bm, "after writing")

	if err := bm.Flush(ctx); err != nil {
		t.Errorf("flush error: %v", err)
	}

	dumpContents(t, bm, "after flush")
	verifyUnreferencedBlobsCount(ctx, t, bm, 0)

	if err := bm.DeleteContent(contentID); err != nil {
		t.Errorf("error deleting content: %v", contentID)
	}

	dumpContents(t, bm, "after delete")

	if err := bm.Flush(ctx); err != nil {
		t.Errorf("flush error: %v", err)
	}

	dumpContents(t, bm, "after flush")
	// content present in first pack, original pack is still referenced
	verifyUnreferencedBlobsCount(ctx, t, bm, 0)
}

func dumpContents(t *testing.T, bm *Manager, caption string) {
	t.Helper()

	count := 0

	log.Infof("dumping %v contents", caption)

	if err := bm.IterateContents(IterateOptions{IncludeDeleted: true},
		func(ci Info) error {
			log.Debugf(" ci[%v]=%#v", count, ci)
			count++
			return nil
		}); err != nil {
		t.Errorf("error listing contents: %v", err)
		return
	}

	log.Infof("finished dumping %v %v contents", count, caption)
}

func verifyUnreferencedBlobsCount(ctx context.Context, t *testing.T, bm *Manager, want int) {
	t.Helper()

	var unrefCount int32

	err := bm.IterateUnreferencedBlobs(ctx, 1, func(_ blob.Metadata) error {
		atomic.AddInt32(&unrefCount, 1)
		return nil
	})
	if err != nil {
		t.Errorf("error in IterateUnreferencedBlobs: %v", err)
	}

	log.Infof("got %v expecting %v", unrefCount, want)

	if got := int(unrefCount); got != want {
		t.Errorf("invalid number of unreferenced contents: %v, wanted %v", got, want)
	}
}

func TestContentWriteAliasing(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestContentManager(data, keyTime, fakeTimeNowFrozen(fakeTime))

	contentData := []byte{100, 0, 0}
	id1 := writeContentAndVerify(ctx, t, bm, contentData)
	contentData[0] = 101
	id2 := writeContentAndVerify(ctx, t, bm, contentData)
	bm.Flush(ctx)

	contentData[0] = 102

	id3 := writeContentAndVerify(ctx, t, bm, contentData)

	contentData[0] = 103

	id4 := writeContentAndVerify(ctx, t, bm, contentData)

	verifyContent(ctx, t, bm, id1, []byte{100, 0, 0})
	verifyContent(ctx, t, bm, id2, []byte{101, 0, 0})
	verifyContent(ctx, t, bm, id3, []byte{102, 0, 0})
	verifyContent(ctx, t, bm, id4, []byte{103, 0, 0})
}

func TestContentReadAliasing(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestContentManager(data, keyTime, fakeTimeNowFrozen(fakeTime))

	contentData := []byte{100, 0, 0}
	id1 := writeContentAndVerify(ctx, t, bm, contentData)

	contentData2, err := bm.GetContent(ctx, id1)
	if err != nil {
		t.Fatalf("can't get content data: %v", err)
	}

	contentData2[0]++

	verifyContent(ctx, t, bm, id1, contentData)
	bm.Flush(ctx)
	verifyContent(ctx, t, bm, id1, contentData)
}

func TestVersionCompatibility(t *testing.T) {
	for writeVer := minSupportedReadVersion; writeVer <= currentWriteVersion; writeVer++ {
		writeVer := writeVer
		t.Run(fmt.Sprintf("version-%v", writeVer), func(t *testing.T) {
			verifyVersionCompat(t, writeVer)
		})
	}
}

func verifyVersionCompat(t *testing.T, writeVersion int) {
	ctx := context.Background()

	// create content manager that writes 'writeVersion' and reads all versions >= minSupportedReadVersion
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	mgr := newTestContentManager(data, keyTime, nil)
	mgr.writeFormatVersion = int32(writeVersion)

	dataSet := map[ID][]byte{}

	for i := 0; i < 3000000; i = (i + 1) * 2 {
		data := make([]byte, i)
		cryptorand.Read(data) //nolint:errcheck

		cid, err := mgr.WriteContent(ctx, data, "")
		if err != nil {
			t.Fatalf("unable to write %v bytes: %v", len(data), err)
		}

		dataSet[cid] = data
	}
	verifyContentManagerDataSet(ctx, t, mgr, dataSet)

	// delete random 3 items (map iteration order is random)
	cnt := 0

	for blobID := range dataSet {
		t.Logf("deleting %v", blobID)
		assertNoError(t, mgr.DeleteContent(blobID))
		delete(dataSet, blobID)
		cnt++

		if cnt >= 3 {
			break
		}
	}

	if err := mgr.Flush(ctx); err != nil {
		t.Fatalf("failed to flush: %v", err)
	}

	// create new manager that reads and writes using new version.
	mgr = newTestContentManager(data, keyTime, nil)

	// make sure we can read everything
	verifyContentManagerDataSet(ctx, t, mgr, dataSet)

	if err := mgr.CompactIndexes(ctx, CompactOptions{MaxSmallBlobs: 1}); err != nil {
		t.Fatalf("unable to compact indexes: %v", err)
	}

	if err := mgr.Flush(ctx); err != nil {
		t.Fatalf("failed to flush: %v", err)
	}

	verifyContentManagerDataSet(ctx, t, mgr, dataSet)

	// now open one more manager
	mgr = newTestContentManager(data, keyTime, nil)
	verifyContentManagerDataSet(ctx, t, mgr, dataSet)
}

func verifyContentManagerDataSet(ctx context.Context, t *testing.T, mgr *Manager, dataSet map[ID][]byte) {
	for contentID, originalPayload := range dataSet {
		v, err := mgr.GetContent(ctx, contentID)
		if err != nil {
			t.Errorf("unable to read content %q: %v", contentID, err)
			continue
		}

		if !reflect.DeepEqual(v, originalPayload) {
			t.Errorf("payload for %q does not match original: %v", v, originalPayload)
		}
	}
}

func newTestContentManager(data blobtesting.DataMap, keyTime map[blob.ID]time.Time, timeFunc func() time.Time) *Manager {
	st := blobtesting.NewMapStorage(data, keyTime, timeFunc)
	return newTestContentManagerWithStorage(st, timeFunc)
}

func newTestContentManagerWithStorage(st blob.Storage, timeFunc func() time.Time) *Manager {
	if timeFunc == nil {
		timeFunc = fakeTimeNowWithAutoAdvance(fakeTime, 1*time.Second)
	}

	bm, err := newManagerWithOptions(context.Background(), st, &FormattingOptions{
		Hash:        "HMAC-SHA256",
		Encryption:  "NONE",
		HMACSecret:  hmacSecret,
		MaxPackSize: maxPackSize,
		Version:     1,
	}, CachingOptions{}, timeFunc, nil)
	if err != nil {
		panic("can't create content manager: " + err.Error())
	}

	bm.checkInvariantsOnUnlock = true

	return bm
}

func getIndexCount(d blobtesting.DataMap) int {
	var cnt int

	for blobID := range d {
		if strings.HasPrefix(string(blobID), newIndexBlobPrefix) {
			cnt++
		}
	}

	return cnt
}

func fakeTimeNowFrozen(t time.Time) func() time.Time {
	return fakeTimeNowWithAutoAdvance(t, 0)
}

func fakeTimeNowWithAutoAdvance(t time.Time, dt time.Duration) func() time.Time {
	var mu sync.Mutex

	return func() time.Time {
		mu.Lock()
		defer mu.Unlock()

		ret := t
		t = t.Add(dt)

		return ret
	}
}

func verifyContentNotFound(ctx context.Context, t *testing.T, bm *Manager, contentID ID) {
	t.Helper()

	b, err := bm.GetContent(ctx, contentID)
	if err != ErrContentNotFound {
		t.Errorf("unexpected response from GetContent(%q), got %v,%v, expected %v", contentID, b, err, ErrContentNotFound)
	}
}

func verifyContent(ctx context.Context, t *testing.T, bm *Manager, contentID ID, b []byte) {
	t.Helper()

	b2, err := bm.GetContent(ctx, contentID)
	if err != nil {
		t.Errorf("unable to read content %q: %v", contentID, err)
		return
	}

	if got, want := b2, b; !reflect.DeepEqual(got, want) {
		t.Errorf("content %q data mismatch: got %x (nil:%v), wanted %x (nil:%v)", contentID, got, got == nil, want, want == nil)
	}

	bi, err := bm.ContentInfo(ctx, contentID)
	if err != nil {
		t.Errorf("error getting content info %q: %v", contentID, err)
	}

	if got, want := bi.Length, uint32(len(b)); got != want {
		t.Errorf("invalid content size for %q: %v, wanted %v", contentID, got, want)
	}
}

func writeContentAndVerify(ctx context.Context, t *testing.T, bm *Manager, b []byte) ID {
	t.Helper()

	contentID, err := bm.WriteContent(ctx, b, "")
	if err != nil {
		t.Errorf("err: %v", err)
	}

	if got, want := contentID, ID(hashValue(b)); got != want {
		t.Errorf("invalid content ID for %x, got %v, want %v", b, got, want)
	}

	verifyContent(ctx, t, bm, contentID, b)

	return contentID
}

func flushWithRetries(ctx context.Context, t *testing.T, bm *Manager) int {
	t.Helper()

	var retryCount int

	err := bm.Flush(ctx)
	for i := 0; err != nil && i < maxRetries; i++ {
		log.Warningf("flush failed %v, retrying", err)
		err = bm.Flush(ctx)
		retryCount++
	}

	if err != nil {
		t.Errorf("err: %v", err)
	}

	return retryCount
}

func writeContentWithRetriesAndVerify(ctx context.Context, t *testing.T, bm *Manager, b []byte) (contentID ID, retryCount int) {
	t.Helper()

	contentID, err := bm.WriteContent(ctx, b, "")
	for i := 0; err != nil && i < maxRetries; i++ {
		retryCount++

		log.Warningf("WriteContent failed %v, retrying", err)

		contentID, err = bm.WriteContent(ctx, b, "")
	}

	if err != nil {
		t.Errorf("err: %v", err)
	}

	if got, want := contentID, ID(hashValue(b)); got != want {
		t.Errorf("invalid content ID for %x, got %v, want %v", b, got, want)
	}

	verifyContent(ctx, t, bm, contentID, b)

	return contentID, retryCount
}

func seededRandomData(seed, length int) []byte {
	b := make([]byte, length)
	rnd := rand.New(rand.NewSource(int64(seed)))
	rnd.Read(b)

	return b
}

func hashValue(b []byte) string {
	h := hmac.New(sha256.New, hmacSecret)
	h.Write(b) //nolint:errcheck

	return hex.EncodeToString(h.Sum(nil))
}

func dumpContentManagerData(t *testing.T, data blobtesting.DataMap) {
	t.Helper()
	log.Infof("***data - %v items", len(data))

	for k, v := range data {
		if k[0] == 'n' {
			ndx, err := openPackIndex(bytes.NewReader(v))
			if err == nil {
				log.Infof("index %v (%v bytes)", k, len(v))
				assertNoError(t, ndx.Iterate("", func(i Info) error {
					log.Infof("  %+v\n", i)
					return nil
				}))
			}
		} else {
			log.Infof("non-index %v (%v bytes)\n", k, len(v))
		}
	}

	log.Infof("*** end of data")
}
