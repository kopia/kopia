package repo

import (
	"bytes"
	"compress/gzip"
	"crypto/rand"
	"encoding/hex"
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

const flushPackIndexTimeout = 10 * time.Second
const packObjectPrefix = "P"

type packInfo struct {
	currentPackData  bytes.Buffer
	currentPackIndex *packIndex
	currentPackID    string
}

type blockLocation struct {
	packIndex   int
	objectIndex int
}

type packManager struct {
	objectManager *ObjectManager
	storage       blob.Storage

	mu           sync.RWMutex
	blockToIndex map[string]*packIndex

	pendingPackIndexes    packIndexes
	flushPackIndexesAfter time.Time

	packGroups map[string]*packInfo
}

func (p *packManager) enabled() bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.pendingPackIndexes != nil
}

func (p *packManager) blockIDToPackSection(blockID string) (ObjectIDSection, bool, error) {
	if strings.HasPrefix(blockID, packObjectPrefix) {
		return ObjectIDSection{}, false, nil
	}

	pi, err := p.ensurePackIndexesLoaded()
	if err != nil {
		return ObjectIDSection{}, false, fmt.Errorf("can't load pack index: %v", err)
	}

	ndx := pi[blockID]
	if ndx == nil {
		return ObjectIDSection{}, false, nil
	}

	blk := ndx.Items[blockID]
	if blk == "" {
		return ObjectIDSection{}, false, nil
	}

	if plus := strings.IndexByte(blk, '+'); plus > 0 {
		if start, err := strconv.ParseInt(blk[0:plus], 10, 64); err == nil {
			if length, err := strconv.ParseInt(blk[plus+1:], 10, 64); err == nil {
				if base, err := ParseObjectID(ndx.PackObject); err == nil {
					return ObjectIDSection{
						Base:   base,
						Start:  start,
						Length: length,
					}, true, nil
				}
			}
		}
	}

	return ObjectIDSection{}, false, fmt.Errorf("invalid pack index for %q", blockID)
}

func (p *packManager) begin() error {
	p.ensurePackIndexesLoaded()
	p.flushPackIndexesAfter = time.Now().Add(flushPackIndexTimeout)
	p.pendingPackIndexes = make(packIndexes)
	return nil
}

func (p *packManager) AddToPack(packGroup string, blockID string, data []byte) (ObjectID, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// See if we already have this block ID in some pack.
	if _, ok := p.blockToIndex[blockID]; ok {
		return ObjectID{StorageBlock: blockID}, nil
	}

	g := p.packGroups[packGroup]
	if g == nil {
		g = &packInfo{}
		p.packGroups[packGroup] = g
	}

	if g.currentPackIndex == nil {
		g.currentPackIndex = &packIndex{
			Items:      make(map[string]string),
			PackGroup:  packGroup,
			CreateTime: time.Now().UTC(),
		}
		g.currentPackID = p.newPackID()
		p.pendingPackIndexes[g.currentPackID] = g.currentPackIndex
		g.currentPackData.Reset()
	}

	offset := g.currentPackData.Len()
	g.currentPackData.Write(data)
	g.currentPackIndex.Items[blockID] = fmt.Sprintf("%v+%v", int64(offset), int64(len(data)))

	if g.currentPackData.Len() >= p.objectManager.format.MaxPackFileLength {
		if err := p.finishCurrentPackLocked(); err != nil {
			return NullObjectID, err
		}
	}

	if time.Now().After(p.flushPackIndexesAfter) {
		if err := p.finishCurrentPackLocked(); err != nil {
			return NullObjectID, err
		}
		if err := p.flushPackIndexesLocked(); err != nil {
			return NullObjectID, err
		}
	}

	p.blockToIndex[blockID] = g.currentPackIndex
	return ObjectID{StorageBlock: blockID}, nil
}

func (p *packManager) finishPacking() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.finishCurrentPackLocked(); err != nil {
		return err
	}

	if err := p.flushPackIndexesLocked(); err != nil {
		return err
	}

	p.pendingPackIndexes = nil
	return nil
}

func (p *packManager) flushPackIndexesLocked() error {
	if len(p.pendingPackIndexes) > 0 {
		log.Printf("saving %v pack indexes", len(p.pendingPackIndexes))
		if err := p.writePackIndexes(p.pendingPackIndexes); err != nil {
			return err
		}
	}

	p.flushPackIndexesAfter = time.Now().Add(flushPackIndexTimeout)
	p.pendingPackIndexes = make(packIndexes)
	return nil
}

func (p *packManager) writePackIndexes(ndx packIndexes) error {
	w := p.objectManager.NewWriter(WriterOptions{
		disablePacking:  true,
		Description:     "pack index",
		BlockNamePrefix: packObjectPrefix,
		splitter:        newNeverSplitter(),
	})
	defer w.Close()

	zw := gzip.NewWriter(w)
	if err := json.NewEncoder(zw).Encode(p.pendingPackIndexes); err != nil {
		return fmt.Errorf("can't encode pack index: %v", err)
	}
	zw.Close()

	if _, err := w.Result(); err != nil {
		return fmt.Errorf("can't save pack index object: %v", err)
	}

	return nil
}
func (p *packManager) finishCurrentPackLocked() error {
	for _, g := range p.packGroups {
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
	w := p.objectManager.NewWriter(WriterOptions{
		Description:    fmt.Sprintf("pack:%v", g.currentPackID),
		splitter:       newNeverSplitter(),
		disablePacking: true,
	})
	defer w.Close()

	if _, err := g.currentPackData.WriteTo(w); err != nil {
		return fmt.Errorf("unable to write pack: %v", err)
	}
	g.currentPackData.Reset()
	oid, err := w.Result()

	if err != nil {
		return fmt.Errorf("can't save pack data: %v", err)
	}

	g.currentPackIndex.PackObject = oid.String()
	g.currentPackIndex = nil

	return nil
}

func (p *packManager) loadMergedPackIndex(olderThan *time.Time) (map[string]*packIndex, []string, error) {
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

	merged := make(packIndexes)
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
		merged.merge(pi)
	}

	return merged, blockIDs, nil
}

func (p *packManager) ensurePackIndexesLoaded() (map[string]*packIndex, error) {
	p.mu.RLock()
	pi := p.blockToIndex
	p.mu.RUnlock()
	if pi != nil {
		return pi, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	merged, _, err := p.loadMergedPackIndex(nil)
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
	merged, blockIDs, err := p.loadMergedPackIndex(&cutoffTime)
	if err != nil {
		return err
	}

	if len(blockIDs) < parallelFetches {
		return nil
	}

	if err := p.writePackIndexes(merged); err != nil {
		return err
	}

	ch := makeStringChannel(blockIDs)
	var wg sync.WaitGroup

	for i := 0; i < parallelDeletes; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for blockID := range ch {
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
	return hex.EncodeToString(id)
}

func (p *packManager) Flush() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.finishCurrentPackLocked()
}
