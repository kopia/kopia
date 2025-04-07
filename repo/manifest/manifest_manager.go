// Package manifest implements support for managing JSON-based manifests in repository.
package manifest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"sort"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/metrics"
	"github.com/kopia/kopia/repo/compression"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/logging"
)

const (
	manifestLoadParallelism = 8
	manifestIDLength        = 16
)

var log = logging.Module("kopia/manifest") // +checklocksignore

// ErrNotFound is returned when the metadata item is not found.
var ErrNotFound = errors.New("not found")

// ContentPrefix is the prefix of the content id for manifests.
const (
	ContentPrefix                     = "m"
	autoCompactionContentCountDefault = 16
)

// TypeLabelKey is the label key for manifest type.
const TypeLabelKey = "type"

type contentManager interface {
	Revision() int64
	GetContent(ctx context.Context, contentID content.ID) ([]byte, error)
	WriteContent(ctx context.Context, data gather.Bytes, prefix content.IDPrefix, comp compression.HeaderID) (content.ID, error)
	DeleteContent(ctx context.Context, contentID content.ID) error
	IterateContents(ctx context.Context, options content.IterateOptions, callback content.IterateCallback) error
	DisableIndexFlush(ctx context.Context)
	EnableIndexFlush(ctx context.Context)
	Flush(ctx context.Context) error
	IsReadOnly() bool
}

// ID is a unique identifier of a single manifest.
type ID string

// Manager organizes JSON manifests of various kinds, including snapshot manifests.
type Manager struct {
	mu sync.Mutex
	b  contentManager

	// +checklocks:mu
	pendingEntries map[ID]*manifestEntry

	committed *committedManifestManager

	timeNow func() time.Time // Time provider
}

// Put serializes the provided payload to JSON and persists it. Returns unique identifier that represents the manifest.
func (m *Manager) Put(ctx context.Context, labels map[string]string, payload interface{}) (ID, error) {
	if labels[TypeLabelKey] == "" {
		return "", errors.New("'type' label is required")
	}

	random := make([]byte, manifestIDLength)
	if _, err := rand.Read(random); err != nil {
		return "", errors.Wrap(err, "can't initialize randomness")
	}

	b, err := json.Marshal(payload)
	if err != nil {
		return "", errors.Wrap(err, "marshal error")
	}

	e := &manifestEntry{
		ID:      ID(hex.EncodeToString(random)),
		ModTime: m.timeNow().UTC(),
		Labels:  copyLabels(labels),
		Content: b,
	}

	m.mu.Lock()
	m.pendingEntries[e.ID] = e
	m.mu.Unlock()

	return e.ID, nil
}

// GetMetadata returns metadata about provided manifest item or ErrNotFound if the item can't be found.
func (m *Manager) GetMetadata(ctx context.Context, id ID) (*EntryMetadata, error) {
	e, err := m.getPendingOrCommitted(ctx, id)
	if err != nil {
		return nil, err
	}

	return cloneEntryMetadata(e), nil
}

// Get retrieves the contents of the provided manifest item by deserializing it as JSON to provided object.
// If the manifest is not found, returns ErrNotFound.
func (m *Manager) Get(ctx context.Context, id ID, data interface{}) (*EntryMetadata, error) {
	e, err := m.getPendingOrCommitted(ctx, id)
	if err != nil {
		return nil, err
	}

	if data != nil {
		if err := json.Unmarshal([]byte(e.Content), data); err != nil {
			return nil, errors.Wrapf(err, "unable to unmarshal %q", id)
		}
	}

	return cloneEntryMetadata(e), nil
}

func (m *Manager) getPendingOrCommitted(ctx context.Context, id ID) (*manifestEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	e := m.pendingEntries[id]
	if e == nil {
		var err error

		e, err = m.committed.getCommittedEntryOrNil(ctx, id)
		if err != nil {
			return nil, err
		}
	}

	if e == nil || e.Deleted {
		return nil, errors.Wrapf(ErrNotFound, "manifest %v", id)
	}

	return e, nil
}

func findEntriesMatchingLabels(m map[ID]*manifestEntry, labels map[string]string) map[ID]*manifestEntry {
	matches := map[ID]*manifestEntry{}

	for id, e := range m {
		if matchesLabels(e.Labels, labels) {
			matches[id] = e
		}
	}

	return matches
}

// Find returns the list of EntryMetadata for manifest entries matching all provided labels.
func (m *Manager) Find(ctx context.Context, labels map[string]string) ([]*EntryMetadata, error) {
	committedMatches, err := m.committed.findCommittedEntries(ctx, labels)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	var matches []*EntryMetadata

	for _, e := range findEntriesMatchingLabels(m.pendingEntries, labels) {
		matches = append(matches, cloneEntryMetadata(e))
	}

	for _, e := range committedMatches {
		if m.pendingEntries[e.ID] != nil {
			// ignore committed that are also in pending
			continue
		}

		matches = append(matches, cloneEntryMetadata(e))
	}

	sort.Slice(matches, func(i, j int) bool {
		return matches[i].ModTime.Before(matches[j].ModTime)
	})

	return matches, nil
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
		if av, ok := a[k]; !ok || av != v {
			return false
		}
	}

	return true
}

// Flush persists changes to manifest manager.
func (m *Manager) Flush(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, err := m.committed.commitEntries(ctx, m.pendingEntries)
	if err == nil {
		m.pendingEntries = map[ID]*manifestEntry{}
	}

	return err
}

func mustSucceed(e error) {
	if e != nil {
		panic("unexpected failure: " + e.Error())
	}
}

// Delete marks the specified manifest ID for deletion.
func (m *Manager) Delete(ctx context.Context, id ID) error {
	com, err := m.committed.getCommittedEntryOrNil(ctx, id)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.pendingEntries[id] == nil && com == nil {
		return nil
	}

	m.pendingEntries[id] = &manifestEntry{
		ID:      id,
		ModTime: m.timeNow().UTC(),
		Deleted: true,
	}

	return nil
}

// Compact performs compaction of manifest contents.
func (m *Manager) Compact(ctx context.Context) error {
	return m.committed.compact(ctx)
}

// IDsToStrings converts the IDs to strings.
func IDsToStrings(input []ID) []string {
	var result []string

	for _, v := range input {
		result = append(result, string(v))
	}

	return result
}

// IDsFromStrings converts the IDs to strings.
func IDsFromStrings(input []string) []ID {
	var result []ID

	for _, v := range input {
		result = append(result, ID(v))
	}

	return result
}

func copyLabels(m map[string]string) map[string]string {
	r := map[string]string{}
	for k, v := range m {
		r[k] = v
	}

	return r
}

// ManagerOptions are optional parameters for Manager creation.
type ManagerOptions struct {
	TimeNow                 func() time.Time // Time provider
	AutoCompactionThreshold int
}

// NewManager returns new manifest manager for the provided content manager.
func NewManager(ctx context.Context, b contentManager, options ManagerOptions, mr *metrics.Registry) (*Manager, error) {
	_ = mr

	timeNow := options.TimeNow
	if timeNow == nil {
		timeNow = clock.Now
	}

	autoCompactionThreshold := options.AutoCompactionThreshold
	if autoCompactionThreshold == 0 {
		autoCompactionThreshold = autoCompactionContentCountDefault
	}

	m := &Manager{
		b:              b,
		pendingEntries: map[ID]*manifestEntry{},
		timeNow:        timeNow,
		committed:      newCommittedManager(b, autoCompactionThreshold),
	}

	return m, nil
}
