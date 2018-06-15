package manifest

import (
	"bytes"
	"compress/gzip"
	"context"
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
	mu sync.Mutex
	b  *block.Manager

	pendingEntries map[string]*manifestEntry

	committedEntries  map[string]*manifestEntry
	committedBlockIDs map[string]bool
}

// Put serializes the provided payload to JSON and persists it. Returns unique handle that represents the object.
func (m *Manager) Put(labels map[string]string, payload interface{}) (string, error) {
	if labels["type"] == "" {
		return "", fmt.Errorf("'type' label is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return "", fmt.Errorf("can't initialize randomness: %v", err)
	}

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

	m.pendingEntries[e.ID] = e

	return e.ID, nil
}

// GetMetadata returns metadata about provided manifest item or ErrNotFound if the item can't be found.
func (m *Manager) GetMetadata(id string) (*EntryMetadata, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	e := m.pendingEntries[id]
	if e == nil {
		e = m.committedEntries[id]
	}

	if e == nil || e.Deleted {
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

	e := m.pendingEntries[id]
	if e == nil {
		e = m.committedEntries[id]
	}
	if e == nil || e.Deleted {
		return nil, ErrNotFound
	}

	return e.Content, nil
}

// Find returns the list of EntryMetadata for manifest entries matching all provided labels.
func (m *Manager) Find(labels map[string]string) []*EntryMetadata {
	m.mu.Lock()
	defer m.mu.Unlock()

	var matches []*EntryMetadata
	for _, e := range m.pendingEntries {
		if matchesLabels(e.Labels, labels) {
			matches = append(matches, cloneEntryMetadata(e))
		}
	}
	for _, e := range m.committedEntries {
		if m.pendingEntries[e.ID] != nil {
			// ignore committed that are also in pending
			continue
		}

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
func (m *Manager) Flush(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, err := m.flushPendingEntriesLocked(ctx)
	return err
}

func (m *Manager) flushPendingEntriesLocked(ctx context.Context) (string, error) {
	if len(m.pendingEntries) == 0 {
		return "", nil
	}

	man := manifest{}

	for _, e := range m.pendingEntries {
		man.Entries = append(man.Entries, e)
	}

	var buf bytes.Buffer

	gz := gzip.NewWriter(&buf)
	if err := json.NewEncoder(gz).Encode(man); err != nil {
		return "", fmt.Errorf("unable to marshal: %v", err)
	}
	if err := gz.Flush(); err != nil {
		return "", fmt.Errorf("unable to flush: %v", err)
	}
	if err := gz.Close(); err != nil {
		return "", fmt.Errorf("unable to close: %v", err)
	}

	blockID, err := m.b.WriteBlock(ctx, buf.Bytes(), manifestBlockPrefix)
	if err != nil {
		return "", err
	}

	for _, e := range m.pendingEntries {
		m.committedEntries[e.ID] = e
		delete(m.pendingEntries, e.ID)
	}

	m.committedBlockIDs[blockID] = true

	return blockID, nil
}

// Delete marks the specified manifest ID for deletion.
func (m *Manager) Delete(id string) {
	if m.pendingEntries[id] == nil && m.committedEntries[id] == nil {
		return
	}

	m.pendingEntries[id] = &manifestEntry{
		ID:      id,
		ModTime: time.Now().UTC(),
		Deleted: true,
	}
}

// Refresh updates the committed blocks from the underlying storage.
func (m *Manager) Refresh(ctx context.Context) error {
	return m.loadCommittedBlocks(ctx)
}

func (m *Manager) loadCommittedBlocks(ctx context.Context) error {
	log.Debug().Msg("listing manifest blocks")
	blocks, err := m.b.ListBlocks(manifestBlockPrefix)
	if err != nil {
		return fmt.Errorf("unable to list manifest blocks: %v", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.committedEntries = map[string]*manifestEntry{}
	m.committedBlockIDs = map[string]bool{}

	log.Printf("found %v manifest blocks", len(blocks))

	if err := m.loadManifestBlocks(ctx, blocks); err != nil {
		return fmt.Errorf("unable to load manifest blocks: %v", err)
	}

	if err := m.maybeCompactLocked(ctx); err != nil {
		return fmt.Errorf("error auto-compacting blocks")
	}

	return nil
}

func (m *Manager) loadManifestBlocks(ctx context.Context, blockIDs []string) error {
	t0 := time.Now()

	for _, b := range blockIDs {
		m.committedBlockIDs[b] = true
	}

	manifests, err := m.loadBlocksInParallel(ctx, blockIDs)
	if err != nil {
		return err
	}

	for _, man := range manifests {
		for _, e := range man.Entries {
			m.mergeEntry(e)
		}
	}

	// after merging, remove blocks marked as deleted.
	for k, e := range m.committedEntries {
		if e.Deleted {
			delete(m.committedEntries, k)
		}
	}

	log.Debug().Dur("duration_ms", time.Since(t0)).Msgf("finished loading manifest blocks.")

	return nil
}

func (m *Manager) loadBlocksInParallel(ctx context.Context, blockIDs []string) ([]manifest, error) {
	log.Printf("loading blocks %v", blockIDs)
	errors := make(chan error, len(blockIDs))
	manifests := make(chan manifest, len(blockIDs))
	ch := make(chan string, len(blockIDs))
	var wg sync.WaitGroup

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for blk := range ch {
				t1 := time.Now()
				man, err := m.loadManifestBlock(ctx, blk)
				log.Debug().Dur("duration", time.Since(t1)).Str("blk", blk).Int("worker", workerID).Err(err).Msg("manifest block loaded")
				if err != nil {
					errors <- err
				} else {
					manifests <- man
				}
			}
		}(i)
	}

	// feed block IDs for goroutines
	for _, b := range blockIDs {
		ch <- b
	}
	close(ch)

	// wait for workers to complete
	wg.Wait()
	close(errors)
	close(manifests)

	// if there was any error, forward it
	if err := <-errors; err != nil {
		return nil, err
	}

	var man []manifest
	for m := range manifests {
		man = append(man, m)
	}

	return man, nil
}

func (m *Manager) loadManifestBlock(ctx context.Context, blockID string) (manifest, error) {
	man := manifest{}
	blk, err := m.b.GetBlock(ctx, blockID)
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
func (m *Manager) Compact(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.compactLocked(ctx)
}

func (m *Manager) maybeCompactLocked(ctx context.Context) error {
	if len(m.committedBlockIDs) < autoCompactionBlockCount {
		return nil
	}

	log.Debug().Int("blocks", len(m.committedBlockIDs)).Msg("performing automatic compaction")
	if err := m.compactLocked(ctx); err != nil {
		return fmt.Errorf("unable to compact manifest blocks: %v", err)
	}

	if err := m.b.Flush(ctx); err != nil {
		return fmt.Errorf("unable to flush blocks after auto-compaction: %v", err)
	}

	return nil
}

func (m *Manager) compactLocked(ctx context.Context) error {
	log.Printf("compactLocked: pendingEntries=%v blockIDs=%v", len(m.pendingEntries), len(m.committedBlockIDs))

	if len(m.committedBlockIDs) == 1 && len(m.pendingEntries) == 0 {
		return nil
	}

	// compaction needs to be atomic (deletes and rewrite should show up in one index block or not show up at all)
	// that's why we want to prevent index flushes while we're d.
	m.b.DisableIndexFlush()
	defer m.b.EnableIndexFlush()

	for _, e := range m.committedEntries {
		m.pendingEntries[e.ID] = e
	}

	blockID, err := m.flushPendingEntriesLocked(ctx)
	if err != nil {
		return err
	}

	// add the newly-created block to the list, could be duplicate
	for b := range m.committedBlockIDs {
		if b == blockID {
			// do not delete block that was just written.
			continue
		}

		if err := m.b.DeleteBlock(b); err != nil {
			return fmt.Errorf("unable to delete block %q: %v", b, err)
		}

		delete(m.committedBlockIDs, b)
	}

	return nil
}

func (m *Manager) mergeEntry(e *manifestEntry) {
	prev := m.committedEntries[e.ID]
	if prev == nil {
		m.committedEntries[e.ID] = e
		return
	}

	if e.ModTime.After(prev.ModTime) {
		m.committedEntries[e.ID] = e
	}
}

func copyLabels(m map[string]string) map[string]string {
	r := map[string]string{}
	for k, v := range m {
		r[k] = v
	}
	return r
}

// NewManager returns new manifest manager for the provided block manager.
func NewManager(ctx context.Context, b *block.Manager) (*Manager, error) {
	m := &Manager{
		b:                 b,
		pendingEntries:    map[string]*manifestEntry{},
		committedEntries:  map[string]*manifestEntry{},
		committedBlockIDs: map[string]bool{},
	}

	if err := m.loadCommittedBlocks(ctx); err != nil {
		return nil, err
	}

	return m, nil
}
