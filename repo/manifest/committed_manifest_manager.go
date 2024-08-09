package manifest

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/content/index"
)

// committedManifestManager manages committed manifest entries stored in 'm' contents.
type committedManifestManager struct {
	b contentManager

	debugID string // +checklocksignore

	cmmu sync.Mutex
	// +checklocks:cmmu
	lastRevision int64
	// +checklocks:cmmu
	locked bool
	// +checklocks:cmmu
	committedEntries map[ID]*manifestEntry
	// +checklocks:cmmu
	committedContentIDs map[content.ID]bool

	// autoCompactionThreshold controls the threshold after which the manager auto-compacts
	// manifest contents
	// +checklocks:cmmu
	autoCompactionThreshold int
}

func (m *committedManifestManager) getCommittedEntryOrNil(ctx context.Context, id ID) (*manifestEntry, error) {
	m.lock()
	defer m.unlock()

	if err := m.ensureInitializedLocked(ctx); err != nil {
		return nil, err
	}

	return m.committedEntries[id], nil
}

// +checklocks:m.cmmu
func (m *committedManifestManager) dump(ctx context.Context, prefix string) {
	if m.debugID == "" {
		return
	}

	var keys []string

	for k := range m.committedEntries {
		keys = append(keys, string(k))
	}

	sort.Strings(keys)

	log(ctx).Debugf(prefix+"["+m.debugID+"] committed keys %v: %v rev=%v", len(keys), keys, m.lastRevision)
}

func (m *committedManifestManager) findCommittedEntries(ctx context.Context, labels map[string]string) (map[ID]*manifestEntry, error) {
	m.lock()
	defer m.unlock()

	if err := m.ensureInitializedLocked(ctx); err != nil {
		return nil, err
	}

	return findEntriesMatchingLabels(m.committedEntries, labels), nil
}

func (m *committedManifestManager) commitEntries(ctx context.Context, entries map[ID]*manifestEntry) (map[content.ID]bool, error) {
	if len(entries) == 0 {
		return nil, nil
	}

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
// +checklocks:m.cmmu
func (m *committedManifestManager) writeEntriesLocked(ctx context.Context, entries map[ID]*manifestEntry) (map[content.ID]bool, error) {
	if len(entries) == 0 {
		return nil, nil
	}

	man := manifest{}

	for _, e := range entries {
		man.Entries = append(man.Entries, e)
	}

	var buf gather.WriteBuffer
	defer buf.Close()

	gz := gzip.NewWriter(&buf)
	mustSucceed(json.NewEncoder(gz).Encode(man))
	mustSucceed(gz.Flush())
	mustSucceed(gz.Close())

	contentID, err := m.b.WriteContent(ctx, buf.Bytes(), ContentPrefix, content.NoCompression)
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

// +checklocks:m.cmmu
func (m *committedManifestManager) loadCommittedContentsLocked(ctx context.Context) error {
	m.verifyLocked()

	var (
		mu sync.Mutex

		// Temporary set of mappings that we can swap with what's currently in the
		// manager once we're done. This ensures we don't clobber existing data if
		// we fail to load manifests.
		committedEntries    = map[ID]*manifestEntry{}
		committedContentIDs = map[content.ID]bool{}
	)

	for {
		err := m.b.IterateContents(ctx, content.IterateOptions{
			Range:    index.PrefixRange(ContentPrefix),
			Parallel: manifestLoadParallelism,
		}, func(ci content.Info) error {
			mu.Lock()
			committedContentIDs[ci.ContentID] = true
			mu.Unlock()

			err := forEachManifestEntry(
				ctx,
				m.b,
				ci.ContentID,
				func(e *manifestEntry) bool {
					mu.Lock()
					defer mu.Unlock()

					prev := committedEntries[e.ID]
					if prev == nil || e.ModTime.After(prev.ModTime) {
						committedEntries[e.ID] = e
					}

					// Continue iteration.
					return true
				},
			)
			if err != nil {
				// this can be used to allow corrupted repositories to still open and see the
				// (incomplete) list of manifests.
				if os.Getenv("KOPIA_IGNORE_MALFORMED_MANIFEST_CONTENTS") != "" {
					log(ctx).Warnf("ignoring malformed manifest content %v: %v", ci.ContentID, err)

					return nil
				}

				return err
			}

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

	// Remove deleted manifests from the new list. We have to do this last because
	// removing entries from the list while loading would lose mod time info
	// that's used to determine if a tombstone is newer than a populated entry.
	for id, e := range committedEntries {
		if e.Deleted {
			delete(committedEntries, id)
		}
	}

	m.committedEntries = committedEntries
	m.committedContentIDs = committedContentIDs

	if err := m.maybeCompactLocked(ctx); err != nil {
		return errors.Wrap(err, "error auto-compacting contents")
	}

	return nil
}

func (m *committedManifestManager) compact(ctx context.Context) error {
	m.lock()
	defer m.unlock()

	return m.compactLocked(ctx)
}

// +checklocks:m.cmmu
func (m *committedManifestManager) maybeCompactLocked(ctx context.Context) error {
	m.verifyLocked()

	// Don't attempt to compact manifests if the repo was opened in read only mode
	// since we'll just end up failing.
	if m.b.IsReadOnly() || len(m.committedContentIDs) < m.autoCompactionThreshold {
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

// +checklocks:m.cmmu
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

// +checklocks:m.cmmu
func (m *committedManifestManager) ensureInitializedLocked(ctx context.Context) error {
	rev := m.b.Revision()
	if m.lastRevision == rev {
		if m.debugID != "" {
			log(ctx).Debugf("%v up-to-date rev=%v last=%v", m.debugID, rev, m.lastRevision)
		}

		return nil
	}

	if err := m.loadCommittedContentsLocked(ctx); err != nil {
		return err
	}

	m.lastRevision = rev

	m.dump(ctx, "after ensureInitialized: ")
	// it is possible that the content manager revision has changed while we were reading it,
	// that's ok - we read __some__ consistent set of data and next time we will invalidate again.

	return nil
}

// +checklocksacquire:m.cmmu
func (m *committedManifestManager) lock() {
	m.cmmu.Lock()
	m.locked = true
}

// +checklocksrelease:m.cmmu
func (m *committedManifestManager) unlock() {
	m.locked = false
	m.cmmu.Unlock()
}

// +checklocks:m.cmmu
func (m *committedManifestManager) verifyLocked() {
	if !m.locked {
		panic("not locked")
	}
}

// forEachManifestEntry loads the content piece with manifests and calls the
// callback for each manifestEntry. If the callback returns false then it stops
// loading manifests and returns. In cases where the callback returns false,
// content with invalid formatting may not return parse errors.
func forEachManifestEntry(
	ctx context.Context,
	b contentManager,
	contentID content.ID,
	callback func(e *manifestEntry) bool,
) error {
	blk, err := b.GetContent(ctx, contentID)
	if err != nil {
		return errors.Wrapf(err, "error loading manifest content %q", contentID)
	}

	gz, err := gzip.NewReader(bytes.NewReader(blk))
	if err != nil {
		return errors.Wrapf(err, "unable to unpack manifest data %q", contentID)
	}

	// Will be GC-ed even if we don't close it?
	//nolint:errcheck
	defer gz.Close()

	err = forEachDeserializedEntry(gz, callback)
	if err != nil {
		return errors.Wrapf(err, "unable to iterate manifests in content %q", contentID)
	}

	return nil
}

func newCommittedManager(b contentManager, autoCompactionThreshold int) *committedManifestManager {
	debugID := ""
	if os.Getenv("KOPIA_DEBUG_MANIFEST_MANAGER") != "" {
		debugID = fmt.Sprintf("%x", rand.Int63()) //nolint:gosec
	}

	return &committedManifestManager{
		b:                       b,
		debugID:                 debugID,
		committedEntries:        map[ID]*manifestEntry{},
		committedContentIDs:     map[content.ID]bool{},
		autoCompactionThreshold: autoCompactionThreshold,
	}
}
