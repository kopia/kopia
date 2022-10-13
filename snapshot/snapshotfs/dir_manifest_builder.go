package snapshotfs

import (
	"sort"
	"sync"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/snapshot"
)

// DirManifestBuilder builds directory manifests.
type DirManifestBuilder struct {
	mu sync.Mutex

	// +checklocks:mu
	summary fs.DirectorySummary
	// +checklocks:mu
	entries []*snapshot.DirEntry
}

// Clone clones the current state of dirManifestBuilder.
func (b *DirManifestBuilder) Clone() *DirManifestBuilder {
	b.mu.Lock()
	defer b.mu.Unlock()

	return &DirManifestBuilder{
		summary: b.summary.Clone(),
		entries: append([]*snapshot.DirEntry(nil), b.entries...),
	}
}

// AddEntry adds a directory entry to the builder.
func (b *DirManifestBuilder) AddEntry(de *snapshot.DirEntry) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.entries = append(b.entries, de)

	if de.ModTime.After(b.summary.MaxModTime) {
		b.summary.MaxModTime = de.ModTime
	}

	//nolint:exhaustive
	switch de.Type {
	case snapshot.EntryTypeSymlink:
		b.summary.TotalSymlinkCount++

	case snapshot.EntryTypeFile:
		b.summary.TotalFileCount++
		b.summary.TotalFileSize += de.FileSize

	case snapshot.EntryTypeDirectory:
		if childSummary := de.DirSummary; childSummary != nil {
			b.summary.TotalFileCount += childSummary.TotalFileCount
			b.summary.TotalFileSize += childSummary.TotalFileSize
			b.summary.TotalDirCount += childSummary.TotalDirCount
			b.summary.FatalErrorCount += childSummary.FatalErrorCount
			b.summary.IgnoredErrorCount += childSummary.IgnoredErrorCount
			b.summary.FailedEntries = append(b.summary.FailedEntries, childSummary.FailedEntries...)

			if childSummary.MaxModTime.After(b.summary.MaxModTime) {
				b.summary.MaxModTime = childSummary.MaxModTime
			}
		}
	}
}

// AddFailedEntry adds a failed directory entry to the builder and increments enither ignored or fatal error count.
func (b *DirManifestBuilder) AddFailedEntry(relPath string, isIgnoredError bool, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if isIgnoredError {
		b.summary.IgnoredErrorCount++
	} else {
		b.summary.FatalErrorCount++
	}

	b.summary.FailedEntries = append(b.summary.FailedEntries, &fs.EntryWithError{
		EntryPath: relPath,
		Error:     err.Error(),
	})
}

// Build builds the directory manifest.
func (b *DirManifestBuilder) Build(dirModTime fs.UTCTimestamp, incompleteReason string) *snapshot.DirManifest {
	b.mu.Lock()
	defer b.mu.Unlock()

	s := b.summary
	s.TotalDirCount++

	entries := b.entries

	if len(entries) == 0 {
		s.MaxModTime = dirModTime
	}

	s.IncompleteReason = incompleteReason

	b.summary.FailedEntries = sortedTopFailures(b.summary.FailedEntries)

	// sort the result, directories first, then non-directories, ordered by name
	sort.Slice(b.entries, func(i, j int) bool {
		if leftDir, rightDir := isDir(entries[i]), isDir(entries[j]); leftDir != rightDir {
			// directories get sorted before non-directories
			return leftDir
		}

		return entries[i].Name < entries[j].Name
	})

	return &snapshot.DirManifest{
		StreamType: directoryStreamType,
		Summary:    &s,
		Entries:    entries,
	}
}

func sortedTopFailures(entries []*fs.EntryWithError) []*fs.EntryWithError {
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].EntryPath < entries[j].EntryPath
	})

	if len(entries) > fs.MaxFailedEntriesPerDirectorySummary {
		entries = entries[0:fs.MaxFailedEntriesPerDirectorySummary]
	}

	return entries
}
