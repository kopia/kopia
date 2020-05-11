package repositorystress_test

import (
	"context"
	cryptorand "crypto/rand"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob/filesystem"
	"github.com/kopia/kopia/repo/content"
)

const masterPassword = "foo-bar-baz-1234" // nolint:gosec

var (
	knownBlocks      []content.ID
	knownBlocksMutex sync.Mutex
)

func TestStressRepository(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test during short tests")
	}

	ctx := testlogging.Context(t)

	tmpPath, err := ioutil.TempDir("", "kopia")
	if err != nil {
		t.Fatalf("unable to create temp directory")
	}

	defer func() {
		if !t.Failed() {
			os.RemoveAll(tmpPath)
		}
	}()

	t.Logf("path: %v", tmpPath)

	storagePath := filepath.Join(tmpPath, "storage")
	configFile1 := filepath.Join(tmpPath, "kopia1.config")
	configFile2 := filepath.Join(tmpPath, "kopia2.config")

	assertNoError(t, os.MkdirAll(storagePath, 0700))

	st, err := filesystem.New(ctx, &filesystem.Options{
		Path: storagePath,
	})

	if err != nil {
		t.Fatalf("unable to initialize storage: %v", err)
	}

	// create repository
	if err := repo.Initialize(ctx, st, &repo.NewRepositoryOptions{}, masterPassword); err != nil {
		t.Fatalf("unable to initialize repository: %v", err)
	}

	// set up two parallel kopia connections, each with its own config file and cache.
	if err := repo.Connect(ctx, configFile1, st, masterPassword, &repo.ConnectOptions{
		CachingOptions: content.CachingOptions{
			CacheDirectory:    filepath.Join(tmpPath, "cache1"),
			MaxCacheSizeBytes: 2000000000,
		},
	}); err != nil {
		t.Fatalf("unable to connect 1: %v", err)
	}

	if err := repo.Connect(ctx, configFile2, st, masterPassword, &repo.ConnectOptions{
		CachingOptions: content.CachingOptions{
			CacheDirectory:    filepath.Join(tmpPath, "cache2"),
			MaxCacheSizeBytes: 2000000000,
		},
	}); err != nil {
		t.Fatalf("unable to connect 2: %v", err)
	}

	cancel := make(chan struct{})

	var wg sync.WaitGroup

	wg.Add(1)

	go longLivedRepositoryTest(ctx, t, cancel, configFile1, &wg)

	wg.Add(1)

	go longLivedRepositoryTest(ctx, t, cancel, configFile1, &wg)

	wg.Add(1)

	go longLivedRepositoryTest(ctx, t, cancel, configFile1, &wg)

	wg.Add(1)

	go longLivedRepositoryTest(ctx, t, cancel, configFile1, &wg)

	wg.Add(1)

	go longLivedRepositoryTest(ctx, t, cancel, configFile2, &wg)

	wg.Add(1)

	go longLivedRepositoryTest(ctx, t, cancel, configFile2, &wg)

	wg.Add(1)

	go longLivedRepositoryTest(ctx, t, cancel, configFile2, &wg)

	wg.Add(1)

	go longLivedRepositoryTest(ctx, t, cancel, configFile2, &wg)

	time.Sleep(5 * time.Second)
	close(cancel)

	wg.Wait()
}

func longLivedRepositoryTest(ctx context.Context, t *testing.T, cancel chan struct{}, configFile string, wg *sync.WaitGroup) {
	defer wg.Done()

	rep, err := repo.Open(ctx, configFile, masterPassword, &repo.Options{})
	if err != nil {
		t.Errorf("error opening repository: %v", err)
		return
	}
	defer rep.Close(ctx)

	var wg2 sync.WaitGroup

	for i := 0; i < 4; i++ {
		wg2.Add(1)

		go func() {
			defer wg2.Done()

			repositoryTest(ctx, t, cancel, rep.(*repo.DirectRepository))
		}()
	}

	wg2.Wait()
}

func repositoryTest(ctx context.Context, t *testing.T, cancel chan struct{}, rep *repo.DirectRepository) {
	workTypes := []*struct {
		name     string
		fun      func(ctx context.Context, t *testing.T, r *repo.DirectRepository) error
		weight   int
		hitCount int
	}{
		// {"reopen", reopen, 1, 0},
		{"writeRandomBlock", writeRandomBlock, 100, 0},
		{"writeRandomManifest", writeRandomManifest, 100, 0},
		{"readKnownBlock", readKnownBlock, 500, 0},
		{"listContents", listContents, 50, 0},
		{"listAndReadAllContents", listAndReadAllContents, 5, 0},
		{"readRandomManifest", readRandomManifest, 50, 0},
		{"compact", compact, 1, 0},
		{"refresh", refresh, 3, 0},
		{"flush", flush, 1, 0},
	}

	var totalWeight int
	for _, w := range workTypes {
		totalWeight += w.weight
	}

	iter := 0

	for {
		select {
		case <-cancel:
			return
		default:
		}

		if iter%1000 == 0 {
			var bits []string
			for _, w := range workTypes {
				bits = append(bits, fmt.Sprintf("%v:%v", w.name, w.hitCount))
			}

			log.Printf("#%v %v %v goroutines", iter, strings.Join(bits, " "), runtime.NumGoroutine())
		}
		iter++

		roulette := rand.Intn(totalWeight)
		for _, w := range workTypes {
			if roulette < w.weight {
				w.hitCount++

				if err := w.fun(ctx, t, rep); err != nil {
					w.hitCount++
					t.Errorf("error: %v", errors.Wrapf(err, "error running %v", w.name))

					return
				}

				break
			}

			roulette -= w.weight
		}
	}
}

func writeRandomBlock(ctx context.Context, t *testing.T, r *repo.DirectRepository) error {
	data := make([]byte, 1000)
	cryptorand.Read(data) //nolint:errcheck

	contentID, err := r.Content.WriteContent(ctx, data, "")
	if err == nil {
		knownBlocksMutex.Lock()
		if len(knownBlocks) >= 1000 {
			n := rand.Intn(len(knownBlocks))
			knownBlocks[n] = contentID
		} else {
			knownBlocks = append(knownBlocks, contentID)
		}
		knownBlocksMutex.Unlock()
	}

	return err
}

func readKnownBlock(ctx context.Context, t *testing.T, r *repo.DirectRepository) error {
	knownBlocksMutex.Lock()
	if len(knownBlocks) == 0 {
		knownBlocksMutex.Unlock()
		return nil
	}

	contentID := knownBlocks[rand.Intn(len(knownBlocks))]
	knownBlocksMutex.Unlock()

	_, err := r.Content.GetContent(ctx, contentID)
	if err == nil || err == content.ErrContentNotFound {
		return nil
	}

	return err
}

func listContents(ctx context.Context, t *testing.T, r *repo.DirectRepository) error {
	return r.Content.IterateContents(
		ctx,
		content.IterateOptions{},
		func(i content.Info) error { return nil },
	)
}

func listAndReadAllContents(ctx context.Context, t *testing.T, r *repo.DirectRepository) error {
	return r.Content.IterateContents(
		ctx,
		content.IterateOptions{},
		func(ci content.Info) error {
			cid := ci.ID
			_, err := r.Content.GetContent(ctx, cid)
			if err != nil {
				if err == content.ErrContentNotFound && strings.HasPrefix(string(cid), "m") {
					// this is ok, sometimes manifest manager will perform compaction and 'm' contents will be marked as deleted
					return nil
				}
				return errors.Wrapf(err, "error reading content %v", cid)
			}

			return nil
		})
}

func compact(ctx context.Context, t *testing.T, r *repo.DirectRepository) error {
	return r.Content.CompactIndexes(ctx, content.CompactOptions{MaxSmallBlobs: 1})
}

func flush(ctx context.Context, t *testing.T, r *repo.DirectRepository) error {
	return r.Flush(ctx)
}

func refresh(ctx context.Context, t *testing.T, r *repo.DirectRepository) error {
	return r.Refresh(ctx)
}

func readRandomManifest(ctx context.Context, t *testing.T, r *repo.DirectRepository) error {
	manifests, err := r.FindManifests(ctx, nil)
	if err != nil {
		return err
	}

	if len(manifests) == 0 {
		return nil
	}

	n := rand.Intn(len(manifests))

	_, err = r.GetManifest(ctx, manifests[n].ID, nil)

	return err
}

func writeRandomManifest(ctx context.Context, t *testing.T, r *repo.DirectRepository) error {
	key1 := fmt.Sprintf("key-%v", rand.Intn(10))
	key2 := fmt.Sprintf("key-%v", rand.Intn(10))
	val1 := fmt.Sprintf("val1-%v", rand.Intn(10))
	val2 := fmt.Sprintf("val2-%v", rand.Intn(10))
	content1 := fmt.Sprintf("content-%v", rand.Intn(10))
	content2 := fmt.Sprintf("content-%v", rand.Intn(10))
	content1val := fmt.Sprintf("val1-%v", rand.Intn(10))
	content2val := fmt.Sprintf("val2-%v", rand.Intn(10))
	_, err := r.PutManifest(ctx, map[string]string{
		"type": key1,
		key1:   val1,
		key2:   val2,
	}, map[string]string{
		content1: content1val,
		content2: content2val,
	})

	return err
}

func assertNoError(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Errorf("err: %v", err)
	}
}
