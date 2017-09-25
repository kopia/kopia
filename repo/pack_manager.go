package repo

import (
	"bytes"
	"compress/gzip"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kopia/kopia/blob"
)

const flushPackIndexTimeout = 10 * time.Minute
const packObjectPrefix = "P"
const unpackedObjectsPackGroup = "_unpacked_"
const maxNonPackedBlocksPerPackIndex = 200

type packInfo struct {
	currentPackData  bytes.Buffer
	currentPackIndex *packIndex
	currentPackID    string
	splitter         objectSplitter
}

type blockLocation struct {
	packIndex   int
	objectIndex int
}

type packManager struct {
	objectManager *ObjectManager
	storage       blob.Storage

	mu           sync.Mutex
	blockToIndex map[string]*packIndex

	pendingPackIndexes    packIndexes
	flushPackIndexesAfter time.Time

	openPackGroups map[string]*packInfo
}

func (p *packManager) blockIDToPackSection(blockID string) (ObjectIDSection, bool, error) {
	if strings.HasPrefix(blockID, packObjectPrefix) {
		return ObjectIDSection{}, false, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	pi, err := p.ensurePackIndexesLoaded()
	if err != nil {
		return ObjectIDSection{}, false, fmt.Errorf("can't load pack index: %v", err)
	}

	ndx := pi[blockID]
	if ndx == nil {
		return ObjectIDSection{}, false, nil
	}

	if ndx.PackBlockID == "" {
		return ObjectIDSection{}, false, nil
	}

	blk := ndx.Items[blockID]
	if blk == "" {
		return ObjectIDSection{}, false, nil
	}

	if plus := strings.IndexByte(blk, '+'); plus > 0 {
		if start, err := strconv.ParseInt(blk[0:plus], 10, 64); err == nil {
			if length, err := strconv.ParseInt(blk[plus+1:], 10, 64); err == nil {
				if ndx.PackBlockID != "" {
					if ndx.PackBlockID == blockID {
						// this is possible for a single-element pack
						return ObjectIDSection{}, false, nil
					}
					return ObjectIDSection{
						Base:   ObjectID{StorageBlock: ndx.PackBlockID},
						Start:  start,
						Length: length,
					}, true, nil
				}
			}
		}
	}

	return ObjectIDSection{}, false, fmt.Errorf("invalid pack index for %q", blockID)
}

func (p *packManager) RegisterUnpackedBlock(blockID string, dataLength int64) error {
	if strings.HasPrefix(blockID, packObjectPrefix) {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	g := p.registerUnpackedBlockLockedNoFlush(blockID, dataLength)

	if time.Now().After(p.flushPackIndexesAfter) || len(g.currentPackIndex.Items) > maxNonPackedBlocksPerPackIndex {
		if err := p.finishPackAndMaybeFlushIndexes(g); err != nil {
			return err
		}
	}

	return nil
}

func (p *packManager) registerUnpackedBlockLockedNoFlush(blockID string, dataLength int64) *packInfo {
	g := p.ensurePackGroupLocked(unpackedObjectsPackGroup)

	// See if we already have this block ID in an unpacked pack group.
	ndx, ok := p.blockToIndex[blockID]
	if ok && ndx.PackGroup == unpackedObjectsPackGroup {
		return g
	}

	g.currentPackIndex.Items[blockID] = fmt.Sprintf("0+%v", dataLength)
	return g

}
func (p *packManager) AddToPack(packGroup string, blockID string, data []byte) (ObjectID, error) {
	if strings.HasPrefix(blockID, packObjectPrefix) {
		return NullObjectID, fmt.Errorf("pack objects can't be packed: %v", blockID)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.ensurePackIndexesLoaded()

	// See if we already have this block ID in some pack.
	if _, ok := p.blockToIndex[blockID]; ok {
		return ObjectID{StorageBlock: blockID}, nil
	}

	g := p.ensurePackGroupLocked(packGroup)

	offset := g.currentPackData.Len()
	shouldFinish := false
	for _, d := range data {
		if g.splitter.add(d) {
			shouldFinish = true
		}
	}
	g.currentPackData.Write(data)
	g.currentPackIndex.Items[blockID] = fmt.Sprintf("%v+%v", int64(offset), int64(len(data)))
	p.blockToIndex[blockID] = g.currentPackIndex

	if shouldFinish {
		if err := p.finishPackAndMaybeFlushIndexes(g); err != nil {
			return NullObjectID, err
		}
	}

	return ObjectID{StorageBlock: blockID}, nil
}

func (p *packManager) finishPackAndMaybeFlushIndexes(g *packInfo) error {
	if err := p.finishPackLocked(g); err != nil {
		return err
	}

	if time.Now().After(p.flushPackIndexesAfter) {
		if err := p.flushPackIndexesLocked(); err != nil {
			return err
		}
	}

	return nil
}

func (p *packManager) ensurePackGroupLocked(packGroup string) *packInfo {
	g := p.openPackGroups[packGroup]
	if g == nil {
		g = &packInfo{
			splitter: p.objectManager.newSplitter(),
		}
		p.openPackGroups[packGroup] = g
	}

	if g.currentPackIndex == nil {
		g.currentPackIndex = &packIndex{
			Items:      make(map[string]string),
			PackGroup:  packGroup,
			CreateTime: time.Now().UTC(),
		}
		g.currentPackID = p.newPackID()
		g.currentPackData.Reset()
	}

	return g
}

func (p *packManager) flushPackIndexesLocked() error {
	if len(p.pendingPackIndexes) > 0 {
		log.Printf("saving %v pack indexes", len(p.pendingPackIndexes))
		if _, err := p.writePackIndexes(p.pendingPackIndexes); err != nil {
			return err
		}
	}

	p.flushPackIndexesAfter = time.Now().Add(flushPackIndexTimeout)
	p.pendingPackIndexes = make(packIndexes)
	return nil
}

func (p *packManager) writePackIndexes(ndx packIndexes) (string, error) {
	var buf bytes.Buffer

	zw := gzip.NewWriter(&buf)
	if err := json.NewEncoder(zw).Encode(ndx); err != nil {
		return "", fmt.Errorf("can't encode pack index: %v", err)
	}
	zw.Close()

	oid, err := p.objectManager.hashEncryptAndWrite("", &buf, packObjectPrefix, true)
	if err != nil {
		return "", fmt.Errorf("can't save pack index object: %v", err)
	}

	if oid.StorageBlock == "" {
		return "", fmt.Errorf("unexpected empty storage block: %v", oid)
	}

	return oid.StorageBlock, nil
}

func (p *packManager) finishAllOpenPacksLocked() error {
	for _, g := range p.openPackGroups {
		if err := p.finishPackLocked(g); err != nil {
			return err
		}
	}

	return nil
}

func (p *packManager) finishPackLocked(g *packInfo) error {
	if g.currentPackIndex == nil {
		return nil
	}

	if g.currentPackData.Len() > 0 {
		dataLength := int64(g.currentPackData.Len())
		oid, err := p.objectManager.hashEncryptAndWrite(unpackedObjectsPackGroup, &g.currentPackData, "", true)
		g.currentPackData.Reset()

		if err != nil {
			return fmt.Errorf("can't save pack data: %v", err)
		}

		if oid.StorageBlock == "" {
			return fmt.Errorf("storage block is empty: %v", oid)
		}

		p.registerUnpackedBlockLockedNoFlush(oid.StorageBlock, dataLength)

		g.currentPackIndex.PackBlockID = oid.StorageBlock
	}

	p.pendingPackIndexes[g.currentPackID] = g.currentPackIndex
	g.currentPackIndex = nil

	return nil
}

func (p *packManager) loadMergedPackIndexLocked(olderThan *time.Time) (map[string]*packIndex, []string, error) {
	ch, cancel := p.objectManager.storage.ListBlocks(packObjectPrefix)
	defer cancel()

	t0 := time.Now()

	var wg sync.WaitGroup

	errors := make(chan error, parallelFetches)
	var mu sync.Mutex

	packIndexData := map[string][]byte{}
	totalSize := 0
	var blockIDs []string
	for i := 0; i < parallelFetches; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for b := range ch {
				if b.Error != nil {
					errors <- b.Error
					return
				}

				if olderThan != nil && b.TimeStamp.After(*olderThan) {
					return
				}

				r, err := p.objectManager.Open(ObjectID{StorageBlock: b.BlockID})
				if err != nil {
					errors <- err
					return
				}

				data, err := ioutil.ReadAll(r)
				if err != nil {
					errors <- err
					return
				}

				mu.Lock()
				packIndexData[b.BlockID] = data
				blockIDs = append(blockIDs, b.BlockID)
				totalSize += len(data)
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	close(errors)

	// Propagate async errors, if any.
	for err := range errors {
		return nil, nil, err
	}

	if false {
		log.Printf("loaded %v pack indexes (%v bytes) in %v", len(packIndexData), totalSize, time.Since(t0))
	}

	var indexes []packIndexes

	for blockID, content := range packIndexData {
		var r io.Reader = bytes.NewReader(content)
		zr, err := gzip.NewReader(r)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to read pack index from %q: %v", blockID, err)
		}

		pi, err := loadPackIndexes(zr)
		if err != nil {
			return nil, nil, err
		}

		indexes = append(indexes, pi)
	}

	topLevelBlocks := map[string]bool{}

	for _, pi := range indexes {
		for _, ndx := range pi {
			if ndx.PackGroup == unpackedObjectsPackGroup {
				for blockID := range ndx.Items {
					topLevelBlocks[blockID] = true
				}
			}
		}
	}

	for _, pi := range indexes {
		for _, ndx := range pi {
			if ndx.PackGroup != unpackedObjectsPackGroup {
				if !topLevelBlocks[ndx.PackBlockID] {
					log.Printf("warning: pack block %q (%v) not found", ndx.PackBlockID, ndx.CreateTime)
				}
			}
		}
	}

	merged := make(packIndexes)
	for _, pi := range indexes {
		merged.merge(pi)
	}

	return merged, blockIDs, nil
}

func (p *packManager) ensurePackIndexesLoaded() (map[string]*packIndex, error) {
	pi := p.blockToIndex
	if pi != nil {
		return pi, nil
	}

	merged, _, err := p.loadMergedPackIndexLocked(nil)
	if err != nil {
		return nil, err
	}

	pi = make(map[string]*packIndex)
	for _, pck := range merged {
		for blockID := range pck.Items {
			pi[blockID] = pck
		}
	}

	p.blockToIndex = pi
	// log.Printf("loaded pack index with %v entries", len(p.blockToIndex))

	return pi, nil
}

func (p *packManager) Compact(cutoffTime time.Time) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	merged, blockIDs, err := p.loadMergedPackIndexLocked(&cutoffTime)
	if err != nil {
		return err
	}

	if len(blockIDs) <= 1 {
		log.Printf("skipping index compaction - the number of segments %v is too low", len(blockIDs))
		return nil
	}

	log.Printf("writing %v merged indexes", len(merged))

	compactedBlock, err := p.writePackIndexes(merged)
	if err != nil {
		return err
	}

	ch := makeStringChannel(blockIDs)
	var wg sync.WaitGroup

	for i := 0; i < parallelDeletes; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for blockID := range ch {
				if blockID == compactedBlock {
					log.Printf("warning: sanity check failed, not deleting freshly-written compacted index: %q", compactedBlock)
					continue
				}
				if err := p.objectManager.storage.DeleteBlock(blockID); err != nil {
					log.Printf("warning: unable to delete %q: %v", blockID, err)
				}
			}
		}(i)
	}
	wg.Wait()
	return nil
}

func makeStringChannel(s []string) <-chan string {
	ch := make(chan string)
	go func() {
		defer close(ch)

		for _, v := range s {
			ch <- v
		}
	}()
	return ch
}

func (p *packManager) newPackID() string {
	id := make([]byte, 8)
	rand.Read(id)
	return fmt.Sprintf("%x-%x", time.Now().UnixNano(), id)
}

func (p *packManager) Flush() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.finishAllOpenPacksLocked(); err != nil {
		return err
	}

	if err := p.flushPackIndexesLocked(); err != nil {
		return err
	}

	return nil
}

func newPackManager(om *ObjectManager) *packManager {
	return &packManager{
		objectManager:         om,
		openPackGroups:        make(map[string]*packInfo),
		flushPackIndexesAfter: time.Now().Add(flushPackIndexTimeout),
		pendingPackIndexes:    make(packIndexes),
	}
}
