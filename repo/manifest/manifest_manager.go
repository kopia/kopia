// Package manifest implements support for managing JSON-based manifests in repository.
package manifest

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"sort"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/logging"
)

const manifestLoadParallelism = 8

var log = logging.GetContextLoggerFunc("kopia/manifest")

// ErrNotFound is returned when the metadata item is not found.
var ErrNotFound = errors.New("not found")

// ContentPrefix is the prefix of the content id for manifests
const ContentPrefix = "m"
const autoCompactionContentCount = 16

// TypeLabelKey is the label key for manifest type
const TypeLabelKey = "type"

type contentManager interface {
	GetContent(ctx context.Context, contentID content.ID) ([]byte, error)
	WriteContent(ctx context.Context, data []byte, prefix content.ID) (content.ID, error)
	DeleteContent(ctx context.Context, contentID content.ID) error
	IterateContents(ctx context.Context, options content.IterateOptions, callback content.IterateCallback) error
	DisableIndexFlush(ctx context.Context)
	EnableIndexFlush(ctx context.Context)
	Flush(ctx context.Context) error
}

// ID is a unique identifier of a single manifest.
type ID string

// Manager organizes JSON manifests of various kinds, including snapshot manifests
type Manager struct {
	mu sync.Mutex
	b  contentManager

	initialized    bool
	pendingEntries map[ID]*manifestEntry

	committedEntries    map[ID]*manifestEntry
	committedContentIDs map[content.ID]bool

	timeNow func() time.Time // Time provider
}

// Put serializes the provided payload to JSON and persists it. Returns unique identifier that represents the manifest.
func (m *Manager) Put(ctx context.Context, labels map[string]string, payload interface{}) (ID, error) {
	if labels[TypeLabelKey] == "" {
		return "", errors.Errorf("'type' label is required")
	}

	if err := m.ensureInitialized(ctx); err != nil {
		return "", err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	random := make([]byte, 16)
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

	m.pendingEntries[e.ID] = e

	return e.ID, nil
}

// GetMetadata returns metadata about provided manifest item or ErrNotFound if the item can't be found.
func (m *Manager) GetMetadata(ctx context.Context, id ID) (*EntryMetadata, error) {
	if err := m.ensureInitialized(ctx); err != nil {
		return nil, err
	}

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
func (m *Manager) Get(ctx context.Context, id ID, data interface{}) error {
	if err := m.ensureInitialized(ctx); err != nil {
		return err
	}

	b, err := m.GetRaw(ctx, id)
	if err != nil {
		return err
	}

	if err := json.Unmarshal(b, data); err != nil {
		return errors.Wrapf(err, "unable to unmashal %q", id)
	}

	return nil
}

// GetRaw returns raw contents of the provided manifest (JSON bytes) or ErrNotFound if not found.
func (m *Manager) GetRaw(ctx context.Context, id ID) ([]byte, error) {
	if err := m.ensureInitialized(ctx); err != nil {
		return nil, err
	}

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
func (m *Manager) Find(ctx context.Context, labels map[string]string) ([]*EntryMetadata, error) {
	if err := m.ensureInitialized(ctx); err != nil {
		return nil, err
	}

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

func (m *Manager) flushPendingEntriesLocked(ctx context.Context) (content.ID, error) {
	if len(m.pendingEntries) == 0 {
		return "", nil
	}

	man := manifest{}

	for _, e := range m.pendingEntries {
		man.Entries = append(man.Entries, e)
	}

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	mustSucceed(json.NewEncoder(gz).Encode(man))
	mustSucceed(gz.Flush())
	mustSucceed(gz.Close())

	contentID, err := m.b.WriteContent(ctx, buf.Bytes(), ContentPrefix)
	if err != nil {
		return "", err
	}

	for _, e := range m.pendingEntries {
		m.committedEntries[e.ID] = e
		delete(m.pendingEntries, e.ID)
	}

	m.committedContentIDs[contentID] = true

	return contentID, nil
}

func mustSucceed(e error) {
	if e != nil {
		panic("unexpected failure: " + e.Error())
	}
}

// Delete marks the specified manifest ID for deletion.
func (m *Manager) Delete(ctx context.Context, id ID) error {
	if err := m.ensureInitialized(ctx); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.pendingEntries[id] == nil && m.committedEntries[id] == nil {
		return nil
	}

	m.pendingEntries[id] = &manifestEntry{
		ID:      id,
		ModTime: m.timeNow().UTC(),
		Deleted: true,
	}

	return nil
}

// Refresh updates the committed contents from the underlying storage.
func (m *Manager) Refresh(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.loadCommittedContentsLocked(ctx)
}

func (m *Manager) loadCommittedContentsLocked(ctx context.Context) error {
	log(ctx).Debugf("listing manifest contents")

	var (
		mu        sync.Mutex
		manifests map[content.ID]manifest
	)

	for {
		manifests = map[content.ID]manifest{}

		err := m.b.IterateContents(ctx, content.IterateOptions{
			Prefix:   ContentPrefix,
			Parallel: manifestLoadParallelism,
		}, func(ci content.Info) error {
			man, err := m.loadManifestContent(ctx, ci.ID)
			if err != nil {
				return err
			}
			mu.Lock()
			manifests[ci.ID] = man
			mu.Unlock()
			return nil
		})
		if err == nil {
			// success
			break
		}

		if err == content.ErrContentNotFound {
			// try again, lost a race with another manifest manager which just did compaction
			continue
		}

		return errors.Wrap(err, "unable to load manifest contents")
	}

	m.loadManifestContentsLocked(manifests)

	if err := m.maybeCompactLocked(ctx); err != nil {
		return errors.Errorf("error auto-compacting contents")
	}

	return nil
}

func (m *Manager) loadManifestContentsLocked(manifests map[content.ID]manifest) {
	m.committedEntries = map[ID]*manifestEntry{}
	m.committedContentIDs = map[content.ID]bool{}

	for contentID := range manifests {
		m.committedContentIDs[contentID] = true
	}

	for _, man := range manifests {
		for _, e := range man.Entries {
			m.mergeEntry(e)
		}
	}

	// after merging, remove contents marked as deleted.
	for k, e := range m.committedEntries {
		if e.Deleted {
			delete(m.committedEntries, k)
		}
	}
}

func (m *Manager) loadManifestContent(ctx context.Context, contentID content.ID) (manifest, error) {
	man := manifest{}

	blk, err := m.b.GetContent(ctx, contentID)
	if err != nil {
		// do not wrap the error here, we want to propagate original ErrNotFound
		// which causes a retry if we lose list/delete race.
		return man, err
	}

	gz, err := gzip.NewReader(bytes.NewReader(blk))
	if err != nil {
		return man, errors.Wrapf(err, "unable to unpack manifest data %q", contentID)
	}

	if err := json.NewDecoder(gz).Decode(&man); err != nil {
		return man, errors.Wrapf(err, "unable to parse manifest %q", contentID)
	}

	return man, nil
}

// Compact performs compaction of manifest contents.
func (m *Manager) Compact(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.compactLocked(ctx)
}

func (m *Manager) maybeCompactLocked(ctx context.Context) error {
	if len(m.committedContentIDs) < autoCompactionContentCount {
		return nil
	}

	log(ctx).Debugf("performing automatic compaction of %v contents", len(m.committedContentIDs))

	if err := m.compactLocked(ctx); err != nil {
		return errors.Wrap(err, "unable to compact manifest contents")
	}

	if err := m.b.Flush(ctx); err != nil {
		return errors.Wrap(err, "unable to flush contents after auto-compaction")
	}

	return nil
}

func (m *Manager) compactLocked(ctx context.Context) error {
	log(ctx).Debugf("compactLocked: pendingEntries=%v contentIDs=%v", len(m.pendingEntries), len(m.committedContentIDs))

	if len(m.committedContentIDs) == 1 && len(m.pendingEntries) == 0 {
		return nil
	}

	// compaction needs to be atomic (deletes and rewrite should show up in one index blob or not show up at all)
	// that's why we want to prevent index flushes while we're d.
	m.b.DisableIndexFlush(ctx)
	defer m.b.EnableIndexFlush(ctx)

	for _, e := range m.committedEntries {
		m.pendingEntries[e.ID] = e
	}

	contentID, err := m.flushPendingEntriesLocked(ctx)
	if err != nil {
		return err
	}

	// add the newly-created content to the list, could be duplicate
	for b := range m.committedContentIDs {
		if b == contentID {
			// do not delete content that was just written.
			continue
		}

		if err := m.b.DeleteContent(ctx, b); err != nil {
			return errors.Wrapf(err, "unable to delete content %q", b)
		}

		delete(m.committedContentIDs, b)
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

func (m *Manager) ensureInitialized(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.initialized {
		return nil
	}

	if err := m.loadCommittedContentsLocked(ctx); err != nil {
		return err
	}

	m.initialized = true

	return nil
}

func copyLabels(m map[string]string) map[string]string {
	r := map[string]string{}
	for k, v := range m {
		r[k] = v
	}

	return r
}

// ManagerOptions are optional parameters for Manager creation
type ManagerOptions struct {
	TimeNow func() time.Time // Time provider
}

// NewManager returns new manifest manager for the provided content manager.
func NewManager(ctx context.Context, b contentManager, options ManagerOptions) (*Manager, error) {
	timeNow := options.TimeNow
	if timeNow == nil {
		timeNow = time.Now // allow:no-inject-time
	}

	m := &Manager{
		b:                   b,
		pendingEntries:      map[ID]*manifestEntry{},
		committedEntries:    map[ID]*manifestEntry{},
		committedContentIDs: map[content.ID]bool{},
		timeNow:             timeNow,
	}

	return m, nil
}
