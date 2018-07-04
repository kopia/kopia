package block_manager_stress_test

import (
	"context"
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

	"github.com/kopia/kopia/auth"
	"github.com/kopia/kopia/block"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/storage"
	"github.com/kopia/kopia/storage/filesystem"
)

type testContext struct {
	r *repo.Repository
}

var (
	knownBlocks      []string
	knownBlocksMutex sync.Mutex
)

func TestStressRepository(t *testing.T) {
	ctx := context.Background()
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

	creds, err := auth.Password("foo-bar-baz-1234")
	if err != nil {
		t.Fatalf("unable to initialize credentials: %v", err)
	}

	storagePath := filepath.Join(tmpPath, "storage")
	configFile1 := filepath.Join(tmpPath, "kopia1.config")
	configFile2 := filepath.Join(tmpPath, "kopia2.config")

	os.MkdirAll(storagePath, 0700)
	st, err := filesystem.New(ctx, &filesystem.Options{
		Path: storagePath,
	})
	if err != nil {
		t.Fatalf("unable to initialize storage: %v", err)
	}

	// create repository
	if err := repo.Initialize(ctx, st, &repo.NewRepositoryOptions{}, creds); err != nil {
		t.Fatalf("unable to initialize repository: %v", err)
	}

	// set up two parallel kopia connections, each with its own config file and cache.
	if err := repo.Connect(ctx, configFile1, st, creds, repo.ConnectOptions{
		PersistCredentials: true,
		CachingOptions: block.CachingOptions{
			CacheDirectory:    filepath.Join(tmpPath, "cache1"),
			MaxCacheSizeBytes: 2000000000,
		},
	}); err != nil {
		t.Fatalf("unable to connect 1: %v", err)
	}

	if err := repo.Connect(ctx, configFile2, st, creds, repo.ConnectOptions{
		PersistCredentials: true,
		CachingOptions: block.CachingOptions{
			CacheDirectory:    filepath.Join(tmpPath, "cache2"),
			MaxCacheSizeBytes: 2000000000,
		},
	}); err != nil {
		t.Fatalf("unable to connect 2: %v", err)
	}

	cancel := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(8)

	go longLivedRepositoryTest(t, cancel, configFile1, &wg)
	go longLivedRepositoryTest(t, cancel, configFile1, &wg)
	go longLivedRepositoryTest(t, cancel, configFile1, &wg)
	go longLivedRepositoryTest(t, cancel, configFile1, &wg)
	go longLivedRepositoryTest(t, cancel, configFile2, &wg)
	go longLivedRepositoryTest(t, cancel, configFile2, &wg)
	go longLivedRepositoryTest(t, cancel, configFile2, &wg)
	go longLivedRepositoryTest(t, cancel, configFile2, &wg)

	time.Sleep(5 * time.Second)
	close(cancel)

	wg.Wait()
}

func longLivedRepositoryTest(t *testing.T, cancel chan struct{}, configFile string, wg *sync.WaitGroup) {
	defer wg.Done()
	ctx := context.Background()

	rep, err := repo.Open(ctx, configFile, &repo.Options{})
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

			repositoryTest(t, cancel, rep)
		}()
	}

	wg2.Wait()
}

func repositoryTest(t *testing.T, cancel chan struct{}, rep *repo.Repository) {
	ctx := context.Background()
	// reopen := func(t *testing.T, r *repo.Repository) error {
	// 	if err := rep.Close(ctx); err != nil {
	// 		return fmt.Errorf("error closing: %v", err)
	// 	}

	// 	t0 := time.Now()
	// 	rep, err = repo.Open(ctx, configFile, &repo.Options{})
	// 	log.Printf("reopened in %v", time.Since(t0))
	// 	return err
	// }

	workTypes := []*struct {
		name     string
		fun      func(t *testing.T, r *repo.Repository) error
		weight   int
		hitCount int
	}{
		//{"reopen", reopen, 1, 0},
		{"writeRandomBlock", writeRandomBlock, 100, 0},
		{"writeRandomManifest", writeRandomManifest, 100, 0},
		{"readKnownBlock", readKnownBlock, 500, 0},
		{"listBlocks", listBlocks, 50, 0},
		{"listAndReadAllBlocks", listAndReadAllBlocks, 5, 0},
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
			rep.Close(ctx)
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
				//log.Printf("running %v", w.name)
				if err := w.fun(t, rep); err != nil {
					w.hitCount++
					t.Errorf("error: %v", fmt.Errorf("error running %v: %v", w.name, err))
					return
				}
				break
			}

			roulette -= w.weight
		}
	}

}

func writeRandomBlock(t *testing.T, r *repo.Repository) error {
	ctx := context.Background()

	data := make([]byte, 1000)
	rand.Read(data)
	blockID, err := r.Blocks.WriteBlock(ctx, data, "")
	if err == nil {
		knownBlocksMutex.Lock()
		if len(knownBlocks) >= 1000 {
			n := rand.Intn(len(knownBlocks))
			knownBlocks[n] = blockID
		} else {
			knownBlocks = append(knownBlocks, blockID)
		}
		knownBlocksMutex.Unlock()
	}
	return err
}

func readKnownBlock(t *testing.T, r *repo.Repository) error {
	ctx := context.Background()

	knownBlocksMutex.Lock()
	if len(knownBlocks) == 0 {
		knownBlocksMutex.Unlock()
		return nil
	}
	blockID := knownBlocks[rand.Intn(len(knownBlocks))]
	knownBlocksMutex.Unlock()

	_, err := r.Blocks.GetBlock(ctx, blockID)
	if err == nil || err == storage.ErrBlockNotFound {
		return nil
	}

	return err
}

func listBlocks(t *testing.T, r *repo.Repository) error {
	_, err := r.Blocks.ListBlocks("")
	return err
}

func listAndReadAllBlocks(t *testing.T, r *repo.Repository) error {
	blocks, err := r.Blocks.ListBlocks("")
	if err != nil {
		return err
	}

	for _, bi := range blocks {
		_, err := r.Blocks.GetBlock(context.Background(), bi)
		if err != nil {
			return err
		}
	}

	return nil
}

func compact(t *testing.T, r *repo.Repository) error {
	return r.Blocks.CompactIndexes(context.Background(), 1, 1)
}

func flush(t *testing.T, r *repo.Repository) error {
	return r.Flush(context.Background())
}

func refresh(t *testing.T, r *repo.Repository) error {
	return r.Refresh(context.Background())
}

func readRandomManifest(t *testing.T, r *repo.Repository) error {
	manifests := r.Manifests.Find(nil)
	if len(manifests) == 0 {
		return nil
	}
	n := rand.Intn(len(manifests))
	_, err := r.Manifests.GetRaw(manifests[n].ID)
	return err
}

func writeRandomManifest(t *testing.T, r *repo.Repository) error {
	key1 := fmt.Sprintf("key-%v", rand.Intn(10))
	key2 := fmt.Sprintf("key-%v", rand.Intn(10))
	val1 := fmt.Sprintf("val1-%v", rand.Intn(10))
	val2 := fmt.Sprintf("val2-%v", rand.Intn(10))
	content1 := fmt.Sprintf("content-%v", rand.Intn(10))
	content2 := fmt.Sprintf("content-%v", rand.Intn(10))
	content1val := fmt.Sprintf("val1-%v", rand.Intn(10))
	content2val := fmt.Sprintf("val2-%v", rand.Intn(10))
	_, err := r.Manifests.Put(map[string]string{
		"type": key1,
		key1:   val1,
		key2:   val2,
	}, map[string]string{
		content1: content1val,
		content2: content2val,
	})
	return err
}
