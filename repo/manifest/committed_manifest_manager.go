package manifest

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"sync"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/content"
)

// committedManifestManager manages committed manifest entries stored in 'm' contents.
type committedManifestManager struct {
	b contentManager

	cmmu                sync.Mutex
	lastRevision        int64
	locked              bool
	committedEntries    map[ID]*manifestEntry
	committedContentIDs map[content.ID]bool
}

func (m *committedManifestManager) getCommittedEntryOrNil(ctx context.Context, id ID) (*manifestEntry, error) {
	if err := m.ensureInitialized(ctx); err != nil {
		return nil, err
	}

	m.lock()
	defer m.unlock()

	return m.committedEntries[id], nil
}

func (m *committedManifestManager) findCommittedEntries(ctx context.Context, labels map[string]string) (map[ID]*manifestEntry, error) {
	if err := m.ensureInitialized(ctx); err != nil {
		return nil, err
	}

	m.lock()
	defer m.unlock()

	return findEntriesMatchingLabels(m.committedEntries, labels), nil
}

func (m *committedManifestManager) commitEntries(ctx context.Context, entries map[ID]*manifestEntry) (map[content.ID]bool, error) {
	m.lock()
	defer m.unlock()

	return m.writeEntriesLocked(ctx, entries)
}

// writeEntriesLocked writes entries in the provided map as manifest contents
// and removes all entries from the map when complete and returns the set of content IDs written
// (typically one).
//
// NOTE: this function is used in two cases - to write pending entries (where the caller acquires
// the lock via commitEntries()) and to compact existing committed entries during compaction
// where the lock is already being held.
func (m *committedManifestManager) writeEntriesLocked(ctx context.Context, entries map[ID]*manifestEntry) (map[content.ID]bool, error) {
	if len(entries) == 0 {
		return nil, nil
	}

	man := manifest{}

	for _, e := range entries {
		man.Entries = append(man.Entries, e)
	}

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	mustSucceed(json.NewEncoder(gz).Encode(man))
	mustSucceed(gz.Flush())
	mustSucceed(gz.Close())

	contentID, err := m.b.WriteContent(ctx, buf.Bytes(), ContentPrefix)
	if err != nil {
		return nil, errors.Wrap(err, "unable to write content")
	}

	for _, e := range entries {
		m.committedEntries[e.ID] = e
		delete(entries, e.ID)
	}

	m.committedContentIDs[contentID] = true

	return map[content.ID]bool{contentID: true}, nil
}

func (m *committedManifestManager) loadCommittedContentsLocked(ctx context.Context) error {
	m.verifyLocked()

	log(ctx).Debugf("listing manifest contents")

	var (
		mu        sync.Mutex
		manifests map[content.ID]manifest
	)

	for {
		manifests = map[content.ID]manifest{}

		err := m.b.IterateContents(ctx, content.IterateOptions{
			Range:    content.PrefixRange(ContentPrefix),
			Parallel: manifestLoadParallelism,
		}, func(ci content.Info) error {
			man, err := loadManifestContent(ctx, m.b, ci.ID)
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

		if errors.Is(err, content.ErrContentNotFound) {
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

func (m *committedManifestManager) loadManifestContentsLocked(manifests map[content.ID]manifest) {
	m.committedEntries = map[ID]*manifestEntry{}
	m.committedContentIDs = map[content.ID]bool{}

	for contentID := range manifests {
		m.committedContentIDs[contentID] = true
	}

	for _, man := range manifests {
		for _, e := range man.Entries {
			m.mergeEntryLocked(e)
		}
	}

	// after merging, remove contents marked as deleted.
	for k, e := range m.committedEntries {
		if e.Deleted {
			delete(m.committedEntries, k)
		}
	}
}

func (m *committedManifestManager) compact(ctx context.Context) error {
	m.lock()
	defer m.unlock()

	return m.compactLocked(ctx)
}

func (m *committedManifestManager) maybeCompactLocked(ctx context.Context) error {
	m.verifyLocked()

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

func (m *committedManifestManager) compactLocked(ctx context.Context) error {
	m.verifyLocked()

	log(ctx).Debugf("compactLocked: contentIDs=%v", len(m.committedContentIDs))

	if len(m.committedContentIDs) == 1 {
		return nil
	}

	// compaction needs to be atomic (deletes and rewrite should show up in one index blob or not show up at all)
	// that's why we want to prevent index flushes while we're d.
	m.b.DisableIndexFlush(ctx)
	defer m.b.EnableIndexFlush(ctx)

	tmp := map[ID]*manifestEntry{}
	for k, v := range m.committedEntries {
		tmp[k] = v
	}

	written, err := m.writeEntriesLocked(ctx, tmp)
	if err != nil {
		return err
	}

	// add the newly-created content to the list, could be duplicate
	for b := range m.committedContentIDs {
		if written[b] {
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

func (m *committedManifestManager) mergeEntryLocked(e *manifestEntry) {
	m.verifyLocked()

	prev := m.committedEntries[e.ID]
	if prev == nil {
		m.committedEntries[e.ID] = e
		return
	}

	if e.ModTime.After(prev.ModTime) {
		m.committedEntries[e.ID] = e
	}
}

func (m *committedManifestManager) ensureInitialized(ctx context.Context) error {
	m.lock()
	defer m.unlock()

	rev := m.b.Revision()
	if m.lastRevision == rev {
		return nil
	}

	if err := m.loadCommittedContentsLocked(ctx); err != nil {
		return err
	}

	m.lastRevision = rev

	// it is possible that the content manager revision has changed while we were reading it,
	// that's ok - we read __some__ consistent set of data and next time we will invalidate again.

	return nil
}

func (m *committedManifestManager) lock() {
	m.cmmu.Lock()
	m.locked = true
}

func (m *committedManifestManager) unlock() {
	m.locked = false
	m.cmmu.Unlock()
}

func (m *committedManifestManager) verifyLocked() {
	if !m.locked {
		panic("not locked")
	}
}

func loadManifestContent(ctx context.Context, b contentManager, contentID content.ID) (manifest, error) {
	man := manifest{}

	blk, err := b.GetContent(ctx, contentID)
	if err != nil {
		return man, errors.Wrap(err, "error loading manifest content")
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

func newCommittedManager(b contentManager) *committedManifestManager {
	return &committedManifestManager{
		b:                   b,
		committedEntries:    map[ID]*manifestEntry{},
		committedContentIDs: map[content.ID]bool{},
	}
}
