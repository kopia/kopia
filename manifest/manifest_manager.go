package manifest

import (
	"bytes"
	"compress/gzip"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/kopia/kopia/block"
	"github.com/rs/zerolog/log"
)

// ErrNotFound is returned when the metadata item is not found.
var ErrNotFound = errors.New("not found")

const manifestBlockPrefix = "m"
const autoCompactionBlockCount = 16

// Manager organizes JSON manifests of various kinds, including snapshot manifests
type Manager struct {
	mu             sync.Mutex
	b              *block.Manager
	entries        map[string]*manifestEntry
	blockIDs       []string
	pendingEntries []*manifestEntry
}

// Put serializes the provided payload to JSON and persists it. Returns unique handle that represents the object.
func (m *Manager) Put(labels map[string]string, payload interface{}) (string, error) {
	if labels["type"] == "" {
		return "", fmt.Errorf("'type' label is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	random := make([]byte, 16)
	rand.Read(random)

	b, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}

	e := &manifestEntry{
		ID:      hex.EncodeToString(random),
		ModTime: time.Now().UTC(),
		Labels:  copyLabels(labels),
		Content: b,
	}

	m.pendingEntries = append(m.pendingEntries, e)
	m.entries[e.ID] = e

	return e.ID, nil
}

// GetMetadata returns metadata about provided manifest item or ErrNotFound if the item can't be found.
func (m *Manager) GetMetadata(id string) (*EntryMetadata, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	e := m.entries[id]
	if e == nil {
		return nil, ErrNotFound
	}

	return &EntryMetadata{
		ID:      id,
		ModTime: e.ModTime,
		Length:  len(e.Content),
		Labels:  copyLabels(e.Labels),
	}, nil
}

// Get retrieves the contents of the provided manifest item by deserializing it as JSON to provided object.
// If the manifest is not found, returns ErrNotFound.
func (m *Manager) Get(id string, data interface{}) error {
	b, err := m.GetRaw(id)
	if err != nil {
		return err
	}

	if err := json.Unmarshal(b, data); err != nil {
		return fmt.Errorf("unable to unmashal %q: %v", id, err)
	}

	return nil
}

// GetRaw returns raw contents of the provided manifest (JSON bytes) or ErrNotFound if not found.
func (m *Manager) GetRaw(id string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	e := m.entries[id]
	if e == nil {
		return nil, ErrNotFound
	}

	return e.Content, nil
}

// Find returns the list of EntryMetadata for manifest entries matching all provided labels.
func (m *Manager) Find(labels map[string]string) []*EntryMetadata {
	m.mu.Lock()
	defer m.mu.Unlock()

	var matches []*EntryMetadata
	for _, e := range m.entries {
		if matchesLabels(e.Labels, labels) {
			matches = append(matches, cloneEntryMetadata(e))
		}
	}

	sort.Slice(matches, func(i, j int) bool {
		return matches[i].ModTime.Before(matches[j].ModTime)
	})
	return matches
}

func cloneEntryMetadata(e *manifestEntry) *EntryMetadata {
	return &EntryMetadata{
		ID:      e.ID,
		Labels:  copyLabels(e.Labels),
		Length:  len(e.Content),
		ModTime: e.ModTime,
	}
}

// matchesLabels returns true when all entries in 'b' are found in the 'a'.
func matchesLabels(a, b map[string]string) bool {
	for k, v := range b {
		if a[k] != v {
			return false
		}
	}

	return true
}

// Flush persists changes to manifest manager.
func (m *Manager) Flush() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	blockID, err := m.flushPendingEntriesLocked()
	if err != nil {
		return err
	}

	if blockID == "" {
		return nil
	}

	m.blockIDs = append(m.blockIDs, blockID)
	return nil
}

func (m *Manager) flushPendingEntriesLocked() (string, error) {
	if len(m.pendingEntries) == 0 {
		return "", nil
	}

	man := manifest{
		Entries: m.pendingEntries,
	}

	var buf bytes.Buffer

	gz := gzip.NewWriter(&buf)
	if err := json.NewEncoder(gz).Encode(man); err != nil {
		return "", fmt.Errorf("unable to marshal: %v", err)
	}
	gz.Flush()
	gz.Close()

	blockID, err := m.b.WriteBlock(buf.Bytes(), manifestBlockPrefix)
	if err != nil {
		return "", err
	}

	m.pendingEntries = nil
	return blockID, nil
}

// Delete marks the specified manifest ID for deletion.
func (m *Manager) Delete(id string) {
	if m.entries[id] == nil {
		return
	}

	delete(m.entries, id)
	m.pendingEntries = append(m.pendingEntries, &manifestEntry{
		ID:      id,
		ModTime: time.Now().UTC(),
		Deleted: true,
	})
}

func (m *Manager) load() error {
	if err := m.Flush(); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.entries = map[string]*manifestEntry{}

	log.Debug().Msg("listing manifest blocks")
	blocks, err := m.b.ListBlocks(manifestBlockPrefix)
	if err != nil {
		return fmt.Errorf("unable to list manifest blocks: %v", err)
	}

	log.Printf("found %v manifest blocks", len(blocks))

	if err := m.loadManifestBlocks(blocks); err != nil {
		return fmt.Errorf("unable to load manifest blocks: %v", err)
	}

	if len(blocks) > autoCompactionBlockCount {
		log.Debug().Int("blocks", len(blocks)).Msg("performing automatic compaction")
		if err := m.compactLocked(); err != nil {
			return fmt.Errorf("unable to compact manifest blocks: %v", err)
		}
		m.b.Flush()
	}

	return nil
}

func (m *Manager) loadManifestBlocks(blocks []block.Info) error {
	t0 := time.Now()

	errors := make(chan error, len(blocks))
	manifests := make(chan manifest, len(blocks))
	blockIDs := make(chan string, len(blocks))
	var wg sync.WaitGroup

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for blk := range blockIDs {
				t0 := time.Now()
				man, err := m.loadManifestBlock(blk)
				log.Debug().Dur("duration", time.Since(t0)).Str("blk", blk).Int("worker", workerID).Msg("manifest block loaded")
				if err != nil {
					errors <- err
				} else {
					manifests <- man
				}
			}
		}(i)
	}

	// feed block IDs for goroutines
	for _, b := range blocks {
		blockIDs <- b.BlockID
	}
	close(blockIDs)

	// wait for workers to complete
	wg.Wait()
	close(errors)
	close(manifests)
	log.Debug().Dur("duration_ms", time.Since(t0)).Msgf("finished loading manifest blocks.")
	// if there was any error, forward it
	if err := <-errors; err != nil {
		return err
	}

	for _, b := range blocks {
		m.blockIDs = append(m.blockIDs, b.BlockID)
	}

	for man := range manifests {
		for _, e := range man.Entries {
			m.mergeEntry(e)
		}
	}

	// after merging, remove blocks marked as deleted.
	for k, e := range m.entries {
		if e.Deleted {
			delete(m.entries, k)
		}
	}

	return nil
}

func (m *Manager) loadManifestBlock(blockID string) (manifest, error) {
	man := manifest{}
	blk, err := m.b.GetBlock(blockID)
	if err != nil {
		return man, fmt.Errorf("unable to read block %q: %v", blockID, err)
	}

	if len(blk) > 2 && blk[0] == '{' {
		if err := json.Unmarshal(blk, &man); err != nil {
			return man, fmt.Errorf("unable to parse block %q: %v", blockID, err)
		}
	} else {
		gz, err := gzip.NewReader(bytes.NewReader(blk))
		if err != nil {
			return man, fmt.Errorf("unable to unpack block %q: %v", blockID, err)
		}

		if err := json.NewDecoder(gz).Decode(&man); err != nil {
			return man, fmt.Errorf("unable to parse block %q: %v", blockID, err)
		}
	}

	return man, nil
}

// Compact performs compaction of manifest blocks.
func (m *Manager) Compact() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.compactLocked()
}

func (m *Manager) compactLocked() error {
	log.Printf("compactLocked: pendingEntries=%v blockIDs=%v", len(m.pendingEntries), len(m.blockIDs))

	if len(m.blockIDs) == 1 && len(m.pendingEntries) == 0 {
		return nil
	}

	for _, e := range m.entries {
		m.pendingEntries = append(m.pendingEntries, e)
	}

	blockID, err := m.flushPendingEntriesLocked()
	if err != nil {
		return err
	}

	// add the newly-created block to the list, could be duplicate
	m.blockIDs = append(m.blockIDs, blockID)

	for _, b := range m.blockIDs {
		if b == blockID {
			// do not delete block that was just written.
			continue
		}

		if err := m.b.DeleteBlock(b); err != nil {
			return fmt.Errorf("unable to delete block %q: %v", b, err)
		}
	}

	// all previous blocks were deleted, now we have a new block
	m.blockIDs = []string{blockID}
	return nil
}

func (m *Manager) mergeEntry(e *manifestEntry) error {
	prev := m.entries[e.ID]
	if prev == nil {
		m.entries[e.ID] = e
		return nil
	}

	if e.ModTime.After(prev.ModTime) {
		m.entries[e.ID] = e
	}

	return nil
}

func copyLabels(m map[string]string) map[string]string {
	r := map[string]string{}
	for k, v := range m {
		r[k] = v
	}
	return r
}

// NewManager returns new manifest manager for the provided block manager.
func NewManager(b *block.Manager) (*Manager, error) {
	m := &Manager{
		b:       b,
		entries: map[string]*manifestEntry{},
	}

	if err := m.load(); err != nil {
		return nil, err
	}

	return m, nil
}
