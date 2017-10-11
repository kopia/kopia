package cli

import (
	"container/list"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/object"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	cleanupCommand   = objectCommands.Command("cleanup", "Remove old repository objects not used by any snapshots.").Alias("gc")
	cleanupIgnoreAge = cleanupCommand.Flag("min-age", "Minimum block age to be considered for cleanup.").Default("24h").Duration()

	cleanupDelete = cleanupCommand.Flag("delete", "Whether to actually delete unused blocks.").Default("no").String()
)

type cleanupWorkItem struct {
	oid         object.ID
	isDirectory bool
	debug       string
}

func (c *cleanupWorkItem) String() string {
	return fmt.Sprintf("%v - %v", c.debug, c.oid.String())
}

type cleanupWorkQueue struct {
	items          *list.List
	cond           *sync.Cond
	visited        map[string]bool
	processing     int
	totalCompleted int
	totalPending   int
}

func (wq *cleanupWorkQueue) add(it *cleanupWorkItem) {
	var os = it.oid.String()

	wq.cond.L.Lock()
	if wq.visited[os] {
		// Already processed.
		wq.cond.L.Unlock()
		return
	}

	wq.visited[os] = true
	wq.totalPending++

	wq.items.PushBack(it)
	wq.cond.Signal()
	wq.cond.L.Unlock()
}

func (wq *cleanupWorkQueue) get() (*cleanupWorkItem, bool) {
	wq.cond.L.Lock()
	for wq.items.Len() == 0 && wq.processing > 0 {
		wq.cond.Wait()
	}

	var v *cleanupWorkItem

	if wq.items.Len() > 0 {
		f := wq.items.Front()
		v = f.Value.(*cleanupWorkItem)
		wq.items.Remove(f)
		wq.processing++
	} else {
		wq.cond.Signal()
	}
	wq.cond.L.Unlock()

	if v != nil {
		return v, true
	}

	return nil, false
}

func (wq *cleanupWorkQueue) finished() {
	wq.cond.L.Lock()
	wq.processing--
	wq.totalCompleted++
	wq.cond.Signal()
	wq.cond.L.Unlock()
}

func (wq *cleanupWorkQueue) stats() (totalCompleted int, processing int, totalPending int) {
	wq.cond.L.Lock()
	totalCompleted = wq.totalCompleted
	processing = wq.processing
	totalPending = wq.totalPending
	wq.cond.L.Unlock()
	return
}

type cleanupContext struct {
	sync.Mutex

	repo    *repo.Repository
	mgr     *snapshot.Manager
	inuse   map[string]bool
	visited map[string]bool
	queue   *cleanupWorkQueue

	inuseCollector chan string
}

func findAliveBlocks(ctx *cleanupContext, wi *cleanupWorkItem) error {
	_, blks, err := ctx.repo.Objects.VerifyObject(wi.oid)
	if err != nil {
		return err
	}

	for _, b := range blks {
		ctx.inuseCollector <- b
	}

	if wi.isDirectory {
		entries, err := ctx.mgr.DirectoryEntry(wi.oid).Readdir()

		if err != nil {
			return err
		}

		for _, entry := range entries {
			entryObjectID := entry.(object.HasObjectID).ObjectID()
			_, isSubdir := entry.(fs.Directory)

			ctx.queue.add(&cleanupWorkItem{oid: entryObjectID, isDirectory: isSubdir, debug: wi.debug + "/" + entry.Metadata().Name})
		}
	}

	return nil
}

func runCleanupCommand(context *kingpin.ParseContext) error {
	rep := mustOpenRepository(nil)
	defer rep.Close()

	mgr := snapshot.NewManager(rep)

	log.Printf("Listing active snapshots...")
	snapshotNames, err := mgr.ListSnapshotManifests(nil)
	if err != nil {
		return err
	}

	q := &cleanupWorkQueue{
		items:   list.New(),
		cond:    sync.NewCond(&sync.Mutex{}),
		visited: map[string]bool{},
	}

	ctx := &cleanupContext{
		repo:           rep,
		mgr:            mgr,
		inuse:          map[string]bool{},
		visited:        map[string]bool{},
		queue:          q,
		inuseCollector: make(chan string, 100),
	}

	t0 := time.Now()

	log.Printf("Scanning active objects...")
	workerCount := 32
	var wg sync.WaitGroup
	wg.Add(workerCount)

	snapshots, err := mgr.LoadSnapshots(snapshotNames)
	if err != nil {
		return err
	}

	for _, manifest := range snapshots {
		ctx.queue.add(&cleanupWorkItem{manifest.RootObjectID, true, "root"})
		ctx.queue.add(&cleanupWorkItem{manifest.HashCacheID, false, "root-hashcache"})
	}

	_, _, queued := q.stats()
	log.Printf("Found %v root objects.", queued)

	go func() {
		for iu := range ctx.inuseCollector {
			ctx.inuse[iu] = true
		}
	}()

	for i := 0; i < workerCount; i++ {
		go func(workerID int) {
			for wi, ok := ctx.queue.get(); ok; wi, ok = ctx.queue.get() {
				findAliveBlocks(ctx, wi)
				ctx.queue.finished()
			}
			defer wg.Done()
		}(i)
	}

	var statsWaitGroup sync.WaitGroup
	statsWaitGroup.Add(1)
	cancelStats := make(chan bool)

	cutoffTime := time.Now().Add(-*cleanupIgnoreAge)

	go func() {
		defer statsWaitGroup.Done()

		for {
			select {
			case <-cancelStats:
				return
			case <-time.After(1 * time.Second):
				done, _, queued := q.stats()
				log.Printf("Processed %v objects out of %v (%v objects/sec).", done, queued, int(float64(done)/time.Since(t0).Seconds()))
			}
		}
	}()

	wg.Wait()
	close(cancelStats)

	statsWaitGroup.Wait()
	dt := time.Since(t0)

	log.Printf("Found %v in-use objects in %v blocks in %v", len(ctx.queue.visited), len(ctx.inuse), dt)

	rep.Blocks.CompactIndexes(cutoffTime, ctx.inuse)

	var totalBlocks int
	var totalBytes int64

	var ignoredBlocks int
	var ignoredBytes int64

	var inuseBlocks int
	var inuseBytes int64

	var unreferencedBlocks int
	var unreferencedBytes int64

	var physicalBlocksToDelete []string

	blocks, cancel := rep.Storage.ListBlocks("")
	defer cancel()
	for b := range blocks {
		totalBlocks++
		totalBytes += b.Length

		if strings.HasPrefix(b.BlockID, repo.MetadataBlockPrefix) || strings.HasPrefix(b.BlockID, "P") {
			ignoredBlocks++
			ignoredBytes += b.Length
			continue
		}

		if !ctx.inuse[b.BlockID] {
			if b.TimeStamp.After(cutoffTime) {
				log.Printf("Ignored unreferenced block: %v (%v) at %v", b.BlockID, units.BytesStringBase10(b.Length), b.TimeStamp.Local())
				ignoredBlocks++
				ignoredBytes += b.Length
			} else {
				log.Printf("Unreferenced physical block: %v (%v) at %v", b.BlockID, units.BytesStringBase10(b.Length), b.TimeStamp.Local())
				unreferencedBlocks++
				unreferencedBytes += b.Length

				physicalBlocksToDelete = append(physicalBlocksToDelete, b.BlockID)
			}
		} else {
			inuseBlocks++
			inuseBytes += b.Length
		}
	}

	log.Printf("Found %v (%v) total blocks.", totalBlocks, units.BytesStringBase10(totalBytes))
	log.Printf("Ignored %v blocks (%v).", ignoredBlocks, units.BytesStringBase10(ignoredBytes))
	log.Printf("In-use objects: %v, %v blocks (%v)", len(ctx.queue.visited), inuseBlocks, units.BytesStringBase10(inuseBytes))
	log.Printf("Unreferenced: %v blocks (%v)", unreferencedBlocks, units.BytesStringBase10(unreferencedBytes))

	return nil
}

func init() {
	cleanupCommand.Action(runCleanupCommand)
}
