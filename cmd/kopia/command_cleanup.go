package main

import (
	"container/list"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/kopia/kopia/internal/units"

	"github.com/kopia/kopia/vault"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/repofs"

	"github.com/kopia/kopia/repo"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	cleanupCommand   = app.Command("cleanup", "Remove old repository blocks not used by current backups").Alias("gc")
	cleanupIgnoreAge = cleanupCommand.Flag("min-age", "Minimum block age to be considered for cleanup.").Default("24h").Duration()

	cleanupDelete = cleanupCommand.Flag("delete", "Whether to actually delete unused blocks.").Default("no").String()
)

type cleanupWorkItem struct {
	oid         repo.ObjectID
	isDirectory bool
	debug       string
}

func (c *cleanupWorkItem) String() string {
	return fmt.Sprintf("%v - %v", c.debug, c.oid.String())
}

type cleanupWorkQueue struct {
	items      *list.List
	cond       *sync.Cond
	visited    map[string]bool
	processing int
}

func (wq *cleanupWorkQueue) add(it *cleanupWorkItem) {
	var os string

	if !it.isDirectory && it.oid.Section != nil {
		os = it.oid.Section.Base.String()
	} else {
		os = it.oid.String()
	}

	wq.cond.L.Lock()
	if wq.visited[os] {
		// Already processed.
		wq.cond.L.Unlock()
		return
	}

	wq.visited[os] = true

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
	wq.cond.Signal()
	wq.cond.L.Unlock()
}

type cleanupContext struct {
	sync.Mutex

	repo    *repo.Repository
	inuse   map[string]bool
	visited map[string]bool
	queue   *cleanupWorkQueue

	inuseCollector chan string
}

func findAliveBlocks(ctx *cleanupContext, wi *cleanupWorkItem) error {
	blks, err := ctx.repo.GetStorageBlocks(wi.oid)
	if err != nil {
		return err
	}

	for _, b := range blks {
		ctx.inuseCollector <- b
	}

	if wi.isDirectory {
		entries, err := repofs.Directory(ctx.repo, wi.oid).Readdir()

		if err != nil {
			return err
		}

		for _, entry := range entries {
			entryObjectID := entry.(repo.HasObjectID).ObjectID()
			_, isSubdir := entry.(fs.Directory)

			ctx.queue.add(&cleanupWorkItem{oid: entryObjectID, isDirectory: isSubdir, debug: wi.debug + "/" + entry.Metadata().Name})
		}
	}

	return nil
}

func runCleanupCommand(context *kingpin.ParseContext) error {
	conn := mustOpenConnection()
	defer conn.Close()

	log.Printf("Listing active snapshots...")
	snapshotNames, err := conn.Vault.List("B")
	if err != nil {
		return err
	}

	q := &cleanupWorkQueue{
		items:   list.New(),
		cond:    sync.NewCond(&sync.Mutex{}),
		visited: map[string]bool{},
	}

	ctx := &cleanupContext{
		repo:           conn.Repository,
		inuse:          map[string]bool{},
		visited:        map[string]bool{},
		queue:          q,
		inuseCollector: make(chan string, 100),
	}

	t0 := time.Now()

	log.Printf("Scanning active objects...")
	workerCount := 10
	var wg sync.WaitGroup
	wg.Add(workerCount)

	snapshots := loadBackupManifests(conn.Vault, snapshotNames)

	for _, manifest := range snapshots {
		ctx.queue.add(&cleanupWorkItem{manifest.RootObjectID, true, "root"})
		ctx.queue.add(&cleanupWorkItem{manifest.HashCacheID, false, "root-hashcache"})
	}

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

	wg.Wait()
	dt := time.Since(t0)

	log.Printf("Found %v in-use objects in %v blocks in %v", len(ctx.queue.visited), len(ctx.inuse), dt)

	var totalBlocks int
	var totalBytes int64

	var ignoredBlocks int
	var ignoredBytes int64

	var inuseBlocks int
	var inuseBytes int64

	var unreferencedBlocks int
	var unreferencedBytes int64

	for b := range conn.Repository.Storage.ListBlocks("") {
		totalBlocks++
		totalBytes += b.Length

		if strings.HasPrefix(b.BlockID, vault.ColocatedBlockPrefix) {
			ignoredBlocks++
			ignoredBytes += b.Length
			continue
		}

		if !ctx.inuse[b.BlockID] {
			if time.Since(b.TimeStamp) < *cleanupIgnoreAge {
				log.Printf("Ignored unreferenced block: %v (%v) at %v", b.BlockID, units.BytesString(b.Length), b.TimeStamp.Local())
				ignoredBlocks++
				ignoredBytes += b.Length
			} else {
				log.Printf("Unreferenced block: %v (%v) at %v", b.BlockID, units.BytesString(b.Length), b.TimeStamp.Local())
				unreferencedBlocks++
				unreferencedBytes += b.Length
			}
		} else {
			inuseBlocks++
			inuseBytes += b.Length
		}
	}

	log.Printf("Found %v (%v) total blocks.", totalBlocks, units.BytesString(totalBytes))
	log.Printf("Ignored %v blocks (%v).", ignoredBlocks, units.BytesString(ignoredBytes))
	log.Printf("In-use objects: %v, %v blocks (%v)", len(ctx.queue.visited), inuseBlocks, units.BytesString(inuseBytes))
	log.Printf("Unreferenced: %v blocks (%v)", unreferencedBlocks, units.BytesString(unreferencedBytes))

	return nil
}

func init() {
	cleanupCommand.Action(runCleanupCommand)
}
