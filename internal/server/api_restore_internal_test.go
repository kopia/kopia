package server

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/snapshot/restore"
)

func TestRestoreProgressInfo(t *testing.T) {
	tests := []struct {
		name     string
		stats    restore.Stats
		contains []string
	}{
		{
			name: "bytes",
			stats: restore.Stats{
				RestoredFileCount:     1,
				EnqueuedFileCount:     2,
				RestoredTotalFileSize: 250,
				EnqueuedTotalFileSize: 1000,
			},
			contains: []string{"Processed 1 of 2 items", "25.0%"},
		},
		{
			name: "zero-byte-items",
			stats: restore.Stats{
				RestoredFileCount: 1,
				EnqueuedFileCount: 4,
			},
			contains: []string{"Processed 1 of 4 items", "25.0%"},
		},
		{
			name: "skipped",
			stats: restore.Stats{
				SkippedCount:          2,
				EnqueuedFileCount:     4,
				SkippedTotalFileSize:  500,
				EnqueuedTotalFileSize: 1000,
			},
			contains: []string{"Processed 2 of 4 items", "50.0%", "skipped 2"},
		},
		{
			name: "root-entry",
			stats: restore.Stats{
				RestoredFileCount:     1,
				RestoredTotalFileSize: 1000,
			},
			contains: []string{"Processed 1 of 1 items", "100.0%"},
		},
		{
			name: "clamps-percentage-and-reports-errors",
			stats: restore.Stats{
				RestoredFileCount:     2,
				EnqueuedFileCount:     1,
				RestoredTotalFileSize: 2000,
				EnqueuedTotalFileSize: 1000,
				IgnoredErrorCount:     1,
			},
			contains: []string{"Processed 2 of 2 items", "100.0%", "ignored 1 errors"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := newRestoreTaskProgress().progressInfo(tc.stats)
			for _, want := range tc.contains {
				require.Contains(t, got, want)
			}
		})
	}
}
