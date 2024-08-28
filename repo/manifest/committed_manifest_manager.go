package manifest

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sort"
	"sync"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/content/index"
)

const contentChunkLimit = 10000

var errPersistentVersionMismatch = errors.New("unsupported persisted manifest version")

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
	committedEntries map[ID]*inMemManifestEntry
	// +checklocks:cmmu
	committedContentIDs map[content.ID]bool

	// autoCompactionThreshold controls the threshold after which the manager auto-compacts
	// manifest contents
	// +checklocks:cmmu
	autoCompactionThreshold int

	// formatVersion is the serialization version that these manifests have.
	// Version 0 stored all manifest content inline with metadata. Version 1 adds
	// a level of indirection to store manifest content as separate content blobs
	// and has the metadata point to the content blob.
	formatVersion int
}

func (m *committedManifestManager) getCommittedEntryOrNil(ctx context.Context, id ID) (*inMemManifestEntry, error) {
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

func (m *committedManifestManager) findCommittedEntries(ctx context.Context, labels map[string]string) (map[ID]*inMemManifestEntry, error) {
	m.lock()
	defer m.unlock()

	if err := m.ensureInitializedLocked(ctx); err != nil {
		return nil, err
	}

	return findEntriesMatchingLabels(m.committedEntries, labels), nil
}

func (m *committedManifestManager) commitEntries(ctx context.Context, entries map[ID]*inMemManifestEntry) (map[content.ID]bool, error) {
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
func (m *committedManifestManager) writeEntriesLocked(ctx context.Context, entries map[ID]*inMemManifestEntry) (map[content.ID]bool, error) {
	if len(entries) == 0 {
		return nil, nil
	}

	switch m.formatVersion {
	case 0:
		res, err := m.writeEntriesLockedV0(ctx, entries)
		if err != nil {
			return nil, errors.Wrap(err, "writing manifests in v0 format")
		}

		return res, nil

	case 1:
		res, err := m.writeEntriesLockedV1(ctx, entries)
		if err != nil {
			return nil, errors.Wrap(err, "writing manifests in v1 format")
		}

		return res, nil
	}

	return nil, errors.Errorf("unsupported format version: %d", m.formatVersion)
}

// +checklocks:m.cmmu
func (m *committedManifestManager) writeContentLocked(
	ctx context.Context,
	prefix content.IDPrefix,
	data interface{},
	buf *gather.WriteBuffer,
) (content.ID, error) {
	gz := gzip.NewWriter(buf)
	mustSucceed(json.NewEncoder(gz).Encode(data))
	mustSucceed(gz.Flush())
	mustSucceed(gz.Close())

	contentID, err := m.b.WriteContent(ctx, buf.Bytes(), prefix, content.NoCompression)
	if err != nil {
		return content.EmptyID, errors.Wrap(err, "unable to write content")
	}

	return contentID, nil
}

// +checklocks:m.cmmu
func (m *committedManifestManager) writeEntriesLockedV0(
	ctx context.Context,
	entries map[ID]*inMemManifestEntry,
) (map[content.ID]bool, error) {
	man := manifest{
		Version: 0,
	}

	for _, e := range entries {
		// Don't allow downgrading. We also can't just propagate this entry because
		// the Version field is associated with groups of manifestEntries instead of
		// individual entries.
		if e.formatVersion != 0 {
			return nil, errors.Wrapf(
				errPersistentVersionMismatch,
				"manifest version %d, manager version %d",
				e.formatVersion,
				m.formatVersion,
			)
		}

		man.Entries = append(man.Entries, e.manifestEntry)
	}

	buf := &gather.WriteBuffer{}
	defer buf.Close()

	contentID, err := m.writeContentLocked(ctx, ContentPrefix, man, buf)
	if err != nil {
		return nil, err
	}

	for _, e := range entries {
		e.formatVersion = 0
		m.committedEntries[e.ID] = e
		delete(entries, e.ID)
	}

	m.committedContentIDs[contentID] = true

	return map[content.ID]bool{contentID: true}, nil
}

// +checklocks:m.cmmu
func (m *committedManifestManager) writeContentChunkLocked(
	ctx context.Context,
	entries []*inMemManifestEntry,
	buf *gather.WriteBuffer,
) error {
	contents := contentSet{
		Version: 1,
	}

	for _, e := range entries {
		contents.Contents = append(
			contents.Contents,
			&manifestContent{
				ID:      e.ID,
				Content: e.Content,
			},
		)
	}

	contentID, err := m.writeContentLocked(
		ctx,
		IndirectContentPrefix,
		contents,
		buf,
	)
	if err != nil {
		return errors.Wrapf(err, "writing manifest content chunk")
	}

	contentIDBytes, err := json.Marshal(contentID.String())
	if err != nil {
		return errors.Wrap(err, "serializing content ID")
	}

	for _, e := range entries {
		e.Content = contentIDBytes
	}

	return nil
}

// +checklocks:m.cmmu
func (m *committedManifestManager) writeEntriesLockedV1(
	ctx context.Context,
	entries map[ID]*inMemManifestEntry,
) (map[content.ID]bool, error) {
	var (
		staged []*inMemManifestEntry
		buf    = &gather.WriteBuffer{}
	)

	defer buf.Close()

	// Write all manifest contents to the content manager and store the generated
	// content IDs in the manifestEntries.
	for _, e := range entries {
		// Deleted manifests don't need to be written out since they're just
		// tombstones that exist until the next manifest index compaction.
		//
		// Manifests from version 0, regardless of whether they've been previously
		// persisted or not, will need written out. That will ensure migration
		// occurs as well as ensuring new entries are persisted since they piggyback
		// on the v0 specifier.
		if !e.Deleted && e.formatVersion == 0 {
			e.Size = int32(len(e.Content))

			staged = append(staged, e)

			// TODO(ashmrtn): Pick some cutoff metric. This is a number out of a hat
			// based on an easily available metric, the number of contents we have in
			// this group.
			//
			// If every snapshot manifest is ~1KB of data, then 10k of them is ~10MB
			// of uncompressed content.
			if len(staged) >= contentChunkLimit {
				if err := m.writeContentChunkLocked(ctx, staged, buf); err != nil {
					return nil, err
				}

				buf.Reset()

				staged = nil
			}
		}
	}

	// Write out any remaining manifest contents that may not have met the limit
	// above.
	if len(staged) > 0 {
		if err := m.writeContentChunkLocked(ctx, staged, buf); err != nil {
			return nil, errors.Wrap(err, "writing final manifest content chunk")
		}
	}

	buf.Reset()

	man := manifest{
		Entries: make([]*manifestEntry, 0, len(entries)),
		Version: 1,
	}

	// Each manifestEntry that did have content written out should already have a
	// content ID associated with it since we're working with pointers.
	for _, e := range entries {
		man.Entries = append(man.Entries, e.manifestEntry)
	}

	contentID, err := m.writeContentLocked(ctx, ContentPrefix, man, buf)
	if err != nil {
		return nil, err
	}

	for _, e := range entries {
		e.formatVersion = 1
		m.committedEntries[e.ID] = e
		delete(entries, e.ID)
	}

	m.committedContentIDs[contentID] = true

	return map[content.ID]bool{contentID: true}, nil
}

func (m *committedManifestManager) getV1Manifest(
	ctx context.Context,
	e *inMemManifestEntry,
) (json.RawMessage, error) {
	contentID, err := e.contentID()
	if err != nil {
		return json.RawMessage{},
			errors.Wrap(err, "fetching manifest from indirect chunk")
	}

	gz, err := getContentReader(ctx, m.b, contentID)
	if err != nil {
		return json.RawMessage{},
			errors.Wrap(err, "loading indirect manifest content chunk data")
	}

	// Will be GC-ed even if we don't close it?
	//nolint:errcheck
	defer gz.Close()

	readData, err := io.ReadAll(gz)
	if err != nil {
		return json.RawMessage{},
			errors.Wrapf(
				err,
				"reading data from manifest content chunk %q",
				contentID,
			)
	}

	contents := &contentSet{}

	err = json.Unmarshal(readData, contents)
	if err != nil {
		return json.RawMessage{},
			errors.Wrapf(
				err,
				"deserializing data from manifest content chunk %q",
				contentID,
			)
	}

	if contents.Version != 1 {
		return json.RawMessage{},
			errors.Errorf(
				"unexpected manifest content chunk version %d",
				contents.Version,
			)
	}

	for _, entry := range contents.Contents {
		if entry.ID == e.ID {
			return entry.Content, nil
		}
	}

	return json.RawMessage{},
		errors.Errorf(
			"unable to find manifest %q in manifest content chunk %q",
			e.ID,
			contentID,
		)
}

// +checklocks:m.cmmu
func (m *committedManifestManager) loadCommittedContentsLocked(ctx context.Context) error {
	m.verifyLocked()

	var (
		mu        sync.Mutex
		manifests map[content.ID]manifest
	)

	for {
		manifests = map[content.ID]manifest{}

		err := m.b.IterateContents(ctx, content.IterateOptions{
			Range:    index.PrefixRange(ContentPrefix),
			Parallel: manifestLoadParallelism,
		}, func(ci content.Info) error {
			man, err := loadManifestContent(ctx, m.b, ci.ContentID)
			if err != nil {
				// this can be used to allow corrupterd repositories to still open and see the
				// (incomplete) list of manifests.
				if os.Getenv("KOPIA_IGNORE_MALFORMED_MANIFEST_CONTENTS") != "" {
					log(ctx).Warnf("ignoring malformed manifest content %v: %v", ci.ContentID, err)

					return nil
				}

				return err
			}

			mu.Lock()
			manifests[ci.ContentID] = man
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
		return errors.Wrap(err, "error auto-compacting contents")
	}

	return nil
}

// +checklocks:m.cmmu
func (m *committedManifestManager) loadManifestContentsLocked(manifests map[content.ID]manifest) {
	m.committedEntries = map[ID]*inMemManifestEntry{}
	m.committedContentIDs = map[content.ID]bool{}

	for contentID := range manifests {
		m.committedContentIDs[contentID] = true
	}

	for _, man := range manifests {
		for _, e := range man.Entries {
			m.mergeEntryLocked(e, man.Version)
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

	tmp := map[ID]*inMemManifestEntry{}
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
func (m *committedManifestManager) mergeEntryLocked(
	e *manifestEntry,
	formatVersion int,
) {
	m.verifyLocked()

	prev := m.committedEntries[e.ID]
	if prev == nil {
		m.committedEntries[e.ID] = &inMemManifestEntry{
			manifestEntry: e,
			formatVersion: formatVersion,
		}

		return
	}

	if e.ModTime.After(prev.ModTime) {
		m.committedEntries[e.ID] = &inMemManifestEntry{
			manifestEntry: e,
			formatVersion: formatVersion,
		}
	}
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

func getContentReader(
	ctx context.Context,
	b contentManager,
	contentID content.ID,
) (io.ReadCloser, error) {
	blk, err := b.GetContent(ctx, contentID)
	if err != nil {
		return nil, errors.Wrapf(err, "error loading content piece %q", contentID)
	}

	gz, err := gzip.NewReader(bytes.NewReader(blk))
	if err != nil {
		return nil, errors.Wrapf(err, "unable to unpack content data %q", contentID)
	}

	return gz, nil
}

func loadManifestContent(ctx context.Context, b contentManager, contentID content.ID) (manifest, error) {
	man := manifest{}

	gz, err := getContentReader(ctx, b, contentID)
	if err != nil {
		return man, errors.Wrap(err, "loading manifest data")
	}

	// Will be GC-ed even if we don't close it?
	//nolint:errcheck
	defer gz.Close()

	man, err = decodeManifestArray(gz)

	return man, errors.Wrapf(err, "unable to parse manifest %q", contentID)
}

func newCommittedManager(
	b contentManager,
	autoCompactionThreshold int,
	formatVersion int,
) *committedManifestManager {
	debugID := ""
	if os.Getenv("KOPIA_DEBUG_MANIFEST_MANAGER") != "" {
		debugID = fmt.Sprintf("%x", rand.Int63()) //nolint:gosec
	}

	return &committedManifestManager{
		b:                       b,
		debugID:                 debugID,
		committedEntries:        map[ID]*inMemManifestEntry{},
		committedContentIDs:     map[content.ID]bool{},
		autoCompactionThreshold: autoCompactionThreshold,
		formatVersion:           formatVersion,
	}
}
