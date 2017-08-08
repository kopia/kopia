package repo

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/kopia/kopia/blob"
)

type packManager struct {
	metadataManager *MetadataManager
	objectManager   *ObjectManager
	storage         blob.Storage

	mu          sync.RWMutex
	packIndexes packIndexes

	blockIDToPackedObjectID map[string]ObjectID
	currentPackData         bytes.Buffer
	currentPackIndexes      packIndexes
	currentPackIndex        *packIndex
	currentPackID           string
}

func (p *packManager) enabled() bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.blockIDToPackedObjectID != nil
}

func (p *packManager) begin() error {
	m, err := p.metadataManager.ListMetadataContents(packIDPrefix, -1)
	if err != nil {
		return err
	}

	merged, err := loadMergedPackIndex(m)
	if err != nil {
		return err
	}

	p.currentPackIndexes = make(packIndexes)
	p.blockIDToPackedObjectID = make(map[string]ObjectID)
	for packID, pck := range merged {
		for blockID := range pck.Items {
			p.blockIDToPackedObjectID[blockID] = ObjectID{
				PackID:       packID,
				StorageBlock: blockID,
			}
		}
	}

	return nil
}

func (p *packManager) AddToPack(blockID string, data []byte) (ObjectID, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// See if we already have this block ID in some pack.
	if oid, ok := p.blockIDToPackedObjectID[blockID]; ok {
		return oid, nil
	}

	//log.Printf("%q not found in  %v", blockID, p.blockIDToPackedObjectID)

	if p.currentPackIndex == nil {
		p.currentPackIndex = &packIndex{
			Items: make(map[string]string),
		}
		p.currentPackID = p.newPackID()
		p.currentPackIndexes[p.currentPackID] = p.currentPackIndex
		p.currentPackData.Reset()
	}

	offset := p.currentPackData.Len()
	p.currentPackData.Write(data)
	p.currentPackIndex.Items[blockID] = fmt.Sprintf("%v+%v", int64(offset), int64(len(data)))

	if p.currentPackData.Len() >= p.objectManager.format.MaxPackFileLength {
		if err := p.finishCurrentPackLocked(); err != nil {
			return NullObjectID, err
		}
	}

	packedID := ObjectID{StorageBlock: blockID, PackID: p.currentPackID}
	p.blockIDToPackedObjectID[blockID] = packedID
	return packedID, nil
}

func (p *packManager) finishPacking() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.finishCurrentPackLocked(); err != nil {
		return err
	}

	pi := p.currentPackIndexes
	if p.packIndexes != nil {
		p.packIndexes.merge(pi)
	}

	p.currentPackIndexes = nil
	p.blockIDToPackedObjectID = nil
	return nil
}

func (p *packManager) finishCurrentPackLocked() error {
	if p.currentPackIndex == nil {
		return nil
	}
	w := p.objectManager.NewWriter(WriterOptions{
		splitter:       newNeverSplitter(),
		disablePacking: true,
	})
	defer w.Close()

	if _, err := p.currentPackData.WriteTo(w); err != nil {
		return fmt.Errorf("unable to write pack: %v", err)
	}
	p.currentPackData.Reset()
	oid, err := w.Result(true)

	if err != nil {
		return fmt.Errorf("can't save pack data: %v", err)
	}

	p.currentPackIndex.PackObject = oid.String()
	p.currentPackIndex = nil

	var jb bytes.Buffer
	if err := json.NewEncoder(&jb).Encode(p.currentPackIndexes); err != nil {
		return fmt.Errorf("can't encode pack index: %v", err)
	}

	// save pack index
	uniqueID := make([]byte, 8)
	rand.Read(uniqueID)
	ts := math.MaxInt64 - time.Now().UnixNano()
	itemID := fmt.Sprintf("%v%v.%016x.%x", packIDPrefix, p.currentPackID, ts, uniqueID)
	if err := p.metadataManager.PutMetadata(itemID, jb.Bytes()); err != nil {
		return fmt.Errorf("can't save pack index %q: %v", itemID, err)
	}

	return nil
}

func (p *packManager) ensurePackIndexesLoaded() (packIndexes, error) {
	p.mu.RLock()
	pi := p.packIndexes
	p.mu.RUnlock()
	if pi != nil {
		return pi, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	m, err := p.metadataManager.ListMetadataContents(packIDPrefix, -1)
	if err != nil {
		return nil, fmt.Errorf("can't load pack manifests: %v", err)
	}

	pi, err = loadMergedPackIndex(m)
	if err != nil {
		return nil, fmt.Errorf("can't parse pack indexes: %v", err)
	}

	p.packIndexes = pi

	return pi, nil
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

func (r *Repository) initPackManager() {
	r.packMgr = &packManager{
		objectManager:   r.ObjectManager,
		metadataManager: r.MetadataManager,
	}
}
