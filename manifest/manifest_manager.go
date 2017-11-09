package manifest

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/kopia/kopia/block"
)

// ErrNotFound is returned when the metadata item is not found.
var ErrNotFound = errors.New("not found")

const manifestGroupID = "manifests"

// Manager organizes JSON manifests of various kinds, including snapshot manifests
type Manager struct {
	mu             sync.Mutex
	b              *block.Manager
	entries        map[string]*Entry
	pendingEntries []*Entry
}

func (m *Manager) Add(labels map[string]string, payload interface{}) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	random := make([]byte, 16)
	rand.Read(random)

	b, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}

	e := &Entry{
		ID:      hex.EncodeToString(random),
		ModTime: time.Now().UTC(),
		Labels:  copyLabels(labels),
		Content: b,
	}

	m.pendingEntries = append(m.pendingEntries, e)
	m.entries[e.ID] = e

	return e.ID, nil
}

func (m *Manager) Get(id string, data interface{}) (map[string]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	e := m.entries[id]
	if e == nil {
		return nil, ErrNotFound
	}

	if err := json.Unmarshal(e.Content, data); err != nil {
		return nil, fmt.Errorf("unable to unmashal %q: %v", id, err)
	}

	return copyLabels(e.Labels), nil
}

func (m *Manager) Find(labels map[string]string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	var matches []string
	for id, e := range m.entries {
		if matchesLabels(e.Labels, labels) {
			matches = append(matches, id)
		}
	}

	sort.Slice(matches, func(i, j int) bool {
		return m.entries[matches[i]].ModTime.Before(m.entries[matches[j]].ModTime)
	})
	return matches
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

func (m *Manager) Flush() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.pendingEntries) == 0 {
		return nil
	}

	man := Manifest{
		Entries: m.pendingEntries,
	}

	b, err := json.Marshal(man)
	if err != nil {
		return fmt.Errorf("unable to marshal: %v", err)
	}

	if _, err := m.b.WriteBlock(manifestGroupID, b); err != nil {
		return err
	}

	if err := m.b.Flush(); err != nil {
		return err
	}

	m.pendingEntries = nil
	return nil
}

func (m *Manager) Delete(id string) {
	if m.entries[id] == nil {
		return
	}

	delete(m.entries, id)
	m.pendingEntries = append(m.pendingEntries, &Entry{
		ID:      id,
		ModTime: time.Now().UTC(),
		Deleted: true,
	})
}

func (m *Manager) Load() error {
	if err := m.Flush(); err != nil {
		return err
	}

	m.entries = map[string]*Entry{}

	for _, i := range m.b.ListGroupBlocks(manifestGroupID) {
		blk, err := m.b.GetBlock(i.BlockID)
		if err != nil {
			return fmt.Errorf("unable to read block %q: %v", i.BlockID, err)
		}

		var man Manifest
		if err := json.Unmarshal(blk, &man); err != nil {
			return fmt.Errorf("unable to parse block %q: %v", i.BlockID, err)
		}

		for _, e := range man.Entries {
			m.mergeEntry(e)
		}
	}

	return nil
}

func (m *Manager) mergeEntry(e *Entry) error {
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

type Entry struct {
	ID      string            `json:"id"`
	Labels  map[string]string `json:"labels"`
	ModTime time.Time         `json:"modified"`
	Deleted bool              `json:"deleted,omitempty"`
	Content json.RawMessage   `json:"data"`
}

type Manifest struct {
	Entries []*Entry `json:"entries"`
}

func NewManager(b *block.Manager) (*Manager, error) {
	m := &Manager{
		b:       b,
		entries: map[string]*Entry{},
	}

	if err := m.Load(); err != nil {
		return nil, err
	}

	return m, nil
}
