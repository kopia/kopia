package notifydata_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/notification/notifydata"
	"github.com/kopia/kopia/snapshot"
)

func TestOverallStatus(t *testing.T) {
	tests := []struct {
		name      string
		snapshots []*notifydata.ManifestWithError
		expected  string
	}{
		{
			name: "one success",
			snapshots: []*notifydata.ManifestWithError{
				{Manifest: snapshot.Manifest{
					Source: snapshot.SourceInfo{
						Host:     "host",
						Path:     "/some/path",
						UserName: "user",
					},
				}},
			},
			expected: "Successfully created a snapshot of /some/path",
		},
		{
			name: "all success",
			snapshots: []*notifydata.ManifestWithError{
				{Manifest: snapshot.Manifest{}},
				{Manifest: snapshot.Manifest{}},
			},
			expected: "Successfully created 2 snapshots",
		},
		{
			name: "one fatal error",
			snapshots: []*notifydata.ManifestWithError{
				{Manifest: snapshot.Manifest{}, Error: "fatal error"},
				{Manifest: snapshot.Manifest{}},
			},
			expected: "Failed to create 1 of 2 snapshots",
		},
		{
			name: "one fatal error",
			snapshots: []*notifydata.ManifestWithError{
				{Manifest: snapshot.Manifest{
					Source: snapshot.SourceInfo{
						Host:     "host",
						Path:     "/some/path",
						UserName: "user",
					},
				}, Error: "fatal error"},
			},
			expected: "Failed to create a snapshot of /some/path",
		},
		{
			name: "multiple fatal errors",
			snapshots: []*notifydata.ManifestWithError{
				{Manifest: snapshot.Manifest{}, Error: "fatal error"},
				{Manifest: snapshot.Manifest{}, Error: "fatal error"},
			},
			expected: "Failed to create 2 of 2 snapshots",
		},
		{
			name: "one error",
			snapshots: []*notifydata.ManifestWithError{
				{Manifest: snapshot.Manifest{RootEntry: &snapshot.DirEntry{DirSummary: &fs.DirectorySummary{IgnoredErrorCount: 1}}}},
				{Manifest: snapshot.Manifest{}},
			},
			expected: "Successfully created 2 snapshots",
		},
		{
			name: "one fatal error and two errors",
			snapshots: []*notifydata.ManifestWithError{
				{Manifest: snapshot.Manifest{}, Error: "fatal error"},
				{Manifest: snapshot.Manifest{}},
				{Manifest: snapshot.Manifest{RootEntry: &snapshot.DirEntry{DirSummary: &fs.DirectorySummary{IgnoredErrorCount: 1}}}},
				{Manifest: snapshot.Manifest{RootEntry: &snapshot.DirEntry{DirSummary: &fs.DirectorySummary{IgnoredErrorCount: 1}}}},
			},
			expected: "Failed to create 1 of 4 snapshots",
		},
		{
			name: "one fatal error and one errors",
			snapshots: []*notifydata.ManifestWithError{
				{Manifest: snapshot.Manifest{}, Error: "fatal error"},
				{Manifest: snapshot.Manifest{}},
				{Manifest: snapshot.Manifest{RootEntry: &snapshot.DirEntry{DirSummary: &fs.DirectorySummary{IgnoredErrorCount: 1}}}},
			},
			expected: "Failed to create 1 of 3 snapshots",
		},
		{
			name: "multiple errors",
			snapshots: []*notifydata.ManifestWithError{
				{Manifest: snapshot.Manifest{RootEntry: &snapshot.DirEntry{DirSummary: &fs.DirectorySummary{IgnoredErrorCount: 1}}}},
				{Manifest: snapshot.Manifest{RootEntry: &snapshot.DirEntry{DirSummary: &fs.DirectorySummary{IgnoredErrorCount: 1}}}},
			},
			expected: "Successfully created 2 snapshots",
		},
		{
			name: "incomplete snapshot",
			snapshots: []*notifydata.ManifestWithError{
				{Manifest: snapshot.Manifest{IncompleteReason: "incomplete"}},
				{Manifest: snapshot.Manifest{}},
			},
			expected: "Successfully created 2 snapshots",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mss := notifydata.MultiSnapshotStatus{Snapshots: tt.snapshots}
			require.Equal(t, tt.expected, mss.OverallStatus())
		})
	}
}

func TestStatusCode(t *testing.T) {
	tests := []struct {
		name     string
		manifest notifydata.ManifestWithError
		expected string
	}{
		{
			name: "fatal error",
			manifest: notifydata.ManifestWithError{
				Error: "fatal error",
			},
			expected: notifydata.StatusCodeFatal,
		},
		{
			name: "incomplete snapshot",
			manifest: notifydata.ManifestWithError{
				Manifest: snapshot.Manifest{
					IncompleteReason: "incomplete",
				},
			},
			expected: notifydata.StatusCodeIncomplete,
		},
		{
			name: "fatal error in dir summary",
			manifest: notifydata.ManifestWithError{
				Manifest: snapshot.Manifest{
					RootEntry: &snapshot.DirEntry{
						DirSummary: &fs.DirectorySummary{
							FatalErrorCount: 1,
						},
					},
				},
			},
			expected: notifydata.StatusCodeFatal,
		},
		{
			name: "ignored error in dir summary",
			manifest: notifydata.ManifestWithError{
				Manifest: snapshot.Manifest{
					RootEntry: &snapshot.DirEntry{
						DirSummary: &fs.DirectorySummary{
							IgnoredErrorCount: 1,
						},
					},
				},
			},
			expected: notifydata.StatusCodeWarnings,
		},
		{
			name: "success",
			manifest: notifydata.ManifestWithError{
				Manifest: snapshot.Manifest{},
			},
			expected: notifydata.StatusCodeSuccess,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, tt.manifest.StatusCode())
		})
	}
}

func TestManifestWithErrorMethods(t *testing.T) {
	startTime := clock.Now().Add(-1*time.Minute - 330*time.Millisecond)
	endTime := clock.Now()

	dirSummary := &fs.DirectorySummary{
		TotalFileSize:  1000,
		TotalFileCount: 10,
		TotalDirCount:  5,
	}

	tests := []struct {
		name     string
		manifest notifydata.ManifestWithError
		expected struct {
			startTimestamp time.Time
			endTimestamp   time.Time
			totalSize      int64
			totalFiles     int64
			totalDirs      int64
			duration       time.Duration
		}
	}{
		{
			name: "complete manifest",
			manifest: notifydata.ManifestWithError{
				Manifest: snapshot.Manifest{
					StartTime: fs.UTCTimestamp(startTime.UnixNano()),
					EndTime:   fs.UTCTimestamp(endTime.UnixNano()),
					RootEntry: &snapshot.DirEntry{
						DirSummary: dirSummary,
					},
				},
			},
			expected: struct {
				startTimestamp time.Time
				endTimestamp   time.Time
				totalSize      int64
				totalFiles     int64
				totalDirs      int64
				duration       time.Duration
			}{
				startTimestamp: startTime.UTC().Truncate(time.Second),
				endTimestamp:   endTime.UTC().Truncate(time.Second),
				totalSize:      1000,
				totalFiles:     10,
				totalDirs:      5,
				duration:       endTime.Sub(startTime).Truncate(100 * time.Millisecond),
			},
		},
		{
			name: "empty manifest",
			manifest: notifydata.ManifestWithError{
				Manifest: snapshot.Manifest{},
			},
			expected: struct {
				startTimestamp time.Time
				endTimestamp   time.Time
				totalSize      int64
				totalFiles     int64
				totalDirs      int64
				duration       time.Duration
			}{
				startTimestamp: time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC),
				endTimestamp:   time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC),
				totalSize:      0,
				totalFiles:     0,
				totalDirs:      0,
				duration:       0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected.startTimestamp, tt.manifest.StartTimestamp())
			require.Equal(t, tt.expected.endTimestamp, tt.manifest.EndTimestamp())
			require.Equal(t, tt.expected.totalSize, tt.manifest.TotalSize())
			require.Equal(t, tt.expected.totalFiles, tt.manifest.TotalFiles())
			require.Equal(t, tt.expected.totalDirs, tt.manifest.TotalDirs())
			require.Equal(t, tt.expected.duration, tt.manifest.Duration())
		})
	}
}

func TestTotalSizeDelta(t *testing.T) {
	tests := []struct {
		name     string
		manifest notifydata.ManifestWithError
		expected int64
	}{
		{
			name: "no previous manifest",
			manifest: notifydata.ManifestWithError{
				Manifest: snapshot.Manifest{
					RootEntry: &snapshot.DirEntry{
						DirSummary: &fs.DirectorySummary{
							TotalFileSize: 1000,
						},
					},
				},
			},
			expected: 0,
		},
		{
			name: "no root entry in current manifest",
			manifest: notifydata.ManifestWithError{
				Previous: &snapshot.Manifest{
					RootEntry: &snapshot.DirEntry{
						DirSummary: &fs.DirectorySummary{
							TotalFileSize: 1000,
						},
					},
				},
			},
			expected: 0,
		},
		{
			name: "no dir summary in current manifest",
			manifest: notifydata.ManifestWithError{
				Manifest: snapshot.Manifest{
					RootEntry: &snapshot.DirEntry{
						FileSize: 500,
					},
				},
				Previous: &snapshot.Manifest{
					RootEntry: &snapshot.DirEntry{
						DirSummary: &fs.DirectorySummary{
							TotalFileSize: 1000,
						},
					},
				},
			},
			expected: 500,
		},
		{
			name: "dir summary in both manifests",
			manifest: notifydata.ManifestWithError{
				Manifest: snapshot.Manifest{
					RootEntry: &snapshot.DirEntry{
						DirSummary: &fs.DirectorySummary{
							TotalFileSize: 1500,
						},
					},
				},
				Previous: &snapshot.Manifest{
					RootEntry: &snapshot.DirEntry{
						DirSummary: &fs.DirectorySummary{
							TotalFileSize: 1000,
						},
					},
				},
			},
			expected: 500,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, tt.manifest.TotalSizeDelta())
		})
	}
}

func TestTotalFilesDelta(t *testing.T) {
	tests := []struct {
		name     string
		manifest notifydata.ManifestWithError
		expected int64
	}{
		{
			name: "no previous manifest",
			manifest: notifydata.ManifestWithError{
				Manifest: snapshot.Manifest{
					RootEntry: &snapshot.DirEntry{
						DirSummary: &fs.DirectorySummary{
							TotalFileCount: 10,
						},
					},
				},
			},
			expected: 0,
		},
		{
			name: "no root entry in current manifest",
			manifest: notifydata.ManifestWithError{
				Previous: &snapshot.Manifest{
					RootEntry: &snapshot.DirEntry{
						DirSummary: &fs.DirectorySummary{
							TotalFileCount: 10,
						},
					},
				},
			},
			expected: 0,
		},
		{
			name: "no dir summary in current manifest",
			manifest: notifydata.ManifestWithError{
				Manifest: snapshot.Manifest{
					RootEntry: &snapshot.DirEntry{},
				},
				Previous: &snapshot.Manifest{
					RootEntry: &snapshot.DirEntry{
						DirSummary: &fs.DirectorySummary{
							TotalFileCount: 10,
						},
					},
				},
			},
			expected: 1,
		},
		{
			name: "dir summary in both manifests",
			manifest: notifydata.ManifestWithError{
				Manifest: snapshot.Manifest{
					RootEntry: &snapshot.DirEntry{
						DirSummary: &fs.DirectorySummary{
							TotalFileCount: 15,
						},
					},
				},
				Previous: &snapshot.Manifest{
					RootEntry: &snapshot.DirEntry{
						DirSummary: &fs.DirectorySummary{
							TotalFileCount: 10,
						},
					},
				},
			},
			expected: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, tt.manifest.TotalFilesDelta())
		})
	}
}

func TestTotalDirsDelta(t *testing.T) {
	tests := []struct {
		name     string
		manifest notifydata.ManifestWithError
		expected int64
	}{
		{
			name: "no previous manifest",
			manifest: notifydata.ManifestWithError{
				Manifest: snapshot.Manifest{
					RootEntry: &snapshot.DirEntry{
						DirSummary: &fs.DirectorySummary{
							TotalDirCount: 5,
						},
					},
				},
			},
			expected: 0,
		},
		{
			name: "no root entry in current manifest",
			manifest: notifydata.ManifestWithError{
				Previous: &snapshot.Manifest{
					RootEntry: &snapshot.DirEntry{
						DirSummary: &fs.DirectorySummary{
							TotalDirCount: 5,
						},
					},
				},
			},
			expected: 0,
		},
		{
			name: "no dir summary in current manifest",
			manifest: notifydata.ManifestWithError{
				Manifest: snapshot.Manifest{
					RootEntry: &snapshot.DirEntry{},
				},
				Previous: &snapshot.Manifest{
					RootEntry: &snapshot.DirEntry{
						DirSummary: &fs.DirectorySummary{
							TotalDirCount: 5,
						},
					},
				},
			},
			expected: 0,
		},
		{
			name: "dir summary in both manifests",
			manifest: notifydata.ManifestWithError{
				Manifest: snapshot.Manifest{
					RootEntry: &snapshot.DirEntry{
						DirSummary: &fs.DirectorySummary{
							TotalDirCount: 10,
						},
					},
				},
				Previous: &snapshot.Manifest{
					RootEntry: &snapshot.DirEntry{
						DirSummary: &fs.DirectorySummary{
							TotalDirCount: 5,
						},
					},
				},
			},
			expected: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, tt.manifest.TotalDirsDelta())
		})
	}
}

func TestTotalFiles(t *testing.T) {
	tests := []struct {
		name     string
		manifest notifydata.ManifestWithError
		expected int64
	}{
		{
			name: "no root entry",
			manifest: notifydata.ManifestWithError{
				Manifest: snapshot.Manifest{},
			},
			expected: 0,
		},
		{
			name: "root entry with dir summary",
			manifest: notifydata.ManifestWithError{
				Manifest: snapshot.Manifest{
					RootEntry: &snapshot.DirEntry{
						DirSummary: &fs.DirectorySummary{
							TotalFileCount: 10,
						},
					},
				},
			},
			expected: 10,
		},
		{
			name: "root entry without dir summary",
			manifest: notifydata.ManifestWithError{
				Manifest: snapshot.Manifest{
					RootEntry: &snapshot.DirEntry{},
				},
			},
			expected: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, tt.manifest.TotalFiles())
		})
	}
}

func TestTotalDirs(t *testing.T) {
	tests := []struct {
		name     string
		manifest notifydata.ManifestWithError
		expected int64
	}{
		{
			name: "no root entry",
			manifest: notifydata.ManifestWithError{
				Manifest: snapshot.Manifest{},
			},
			expected: 0,
		},
		{
			name: "root entry with dir summary",
			manifest: notifydata.ManifestWithError{
				Manifest: snapshot.Manifest{
					RootEntry: &snapshot.DirEntry{
						DirSummary: &fs.DirectorySummary{
							TotalDirCount: 5,
						},
					},
				},
			},
			expected: 5,
		},
		{
			name: "root entry without dir summary",
			manifest: notifydata.ManifestWithError{
				Manifest: snapshot.Manifest{
					RootEntry: &snapshot.DirEntry{},
				},
			},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, tt.manifest.TotalDirs())
		})
	}
}

func TestTotalSize(t *testing.T) {
	tests := []struct {
		name     string
		manifest notifydata.ManifestWithError
		expected int64
	}{
		{
			name: "no root entry",
			manifest: notifydata.ManifestWithError{
				Manifest: snapshot.Manifest{},
			},
			expected: 0,
		},
		{
			name: "root entry with dir summary",
			manifest: notifydata.ManifestWithError{
				Manifest: snapshot.Manifest{
					RootEntry: &snapshot.DirEntry{
						DirSummary: &fs.DirectorySummary{
							TotalFileSize: 1000,
						},
					},
				},
			},
			expected: 1000,
		},
		{
			name: "root entry without dir summary",
			manifest: notifydata.ManifestWithError{
				Manifest: snapshot.Manifest{
					RootEntry: &snapshot.DirEntry{
						FileSize: 500,
					},
				},
			},
			expected: 500,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, tt.manifest.TotalSize())
		})
	}
}
