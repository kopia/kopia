package notifydata

import (
	"time"

	"github.com/kopia/kopia/snapshot"
)

// ManifestWithError represents information about the snapshot manifest with optional error.
type ManifestWithError struct {
	snapshot.Manifest `json:"manifest"` // may not be filled out if there was an error, Manifst.Source is always set.

	Error string `json:"error"` // will be present if there was an error
}

// StartTimestamp returns the start time of the snapshot.
func (m *ManifestWithError) StartTimestamp() time.Time {
	return m.StartTime.ToTime().Truncate(time.Second)
}

// EndTimestamp returns the end time of the snapshot.
func (m *ManifestWithError) EndTimestamp() time.Time {
	return m.EndTime.ToTime().Truncate(time.Second)
}

// TotalSize returns the total size of the snapshot in bytes.
func (m *ManifestWithError) TotalSize() int64 {
	if m.RootEntry == nil {
		return 0
	}

	if m.RootEntry.DirSummary != nil {
		return m.RootEntry.DirSummary.TotalFileSize
	}

	return m.RootEntry.FileSize
}

// TotalFiles returns the total number of files in the snapshot.
func (m *ManifestWithError) TotalFiles() int64 {
	if m.RootEntry == nil {
		return 0
	}

	if m.RootEntry.DirSummary != nil {
		return m.RootEntry.DirSummary.TotalFileCount
	}

	return 1
}

// TotalDirs returns the total number of directories in the snapshot.
func (m *ManifestWithError) TotalDirs() int64 {
	if m.RootEntry == nil {
		return 0
	}

	if m.RootEntry.DirSummary != nil {
		return m.RootEntry.DirSummary.TotalDirCount
	}

	return 0
}

// Duration returns the duration of the snapshot.
func (m *ManifestWithError) Duration() time.Duration {
	return time.Duration(m.EndTime - m.StartTime)
}

// StatusCode returns the status code of the manifest.
func (m *ManifestWithError) StatusCode() string {
	if m.Error != "" {
		return "fatal"
	}

	if m.Manifest.IncompleteReason != "" {
		return "incomplete"
	}

	if m.Manifest.RootEntry != nil && m.Manifest.RootEntry.DirSummary != nil {
		if m.Manifest.RootEntry.DirSummary.FatalErrorCount > 0 {
			return "fatal"
		}

		if m.Manifest.RootEntry.DirSummary.IgnoredErrorCount > 0 {
			return "error"
		}
	}

	return "ok"
}

// MultiSnapshotStatus represents the status of multiple snapshots.
type MultiSnapshotStatus struct {
	Snapshots []*ManifestWithError `json:"snapshots"`
}
