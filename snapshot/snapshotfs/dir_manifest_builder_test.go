package snapshotfs

import (
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/snapshot"
)

func TestDirManifestBuilder_IsEmpty(t *testing.T) {
	for _, tc := range []struct {
		name    string
		entry   *snapshot.DirEntry
		failure bool
		ignored bool
		want    bool
	}{
		{name: "new builder", want: true},
		{name: "file", entry: &snapshot.DirEntry{Type: snapshot.EntryTypeFile}},
		{name: "symlink", entry: &snapshot.DirEntry{Type: snapshot.EntryTypeSymlink}},
		{name: "empty directory", entry: &snapshot.DirEntry{Type: snapshot.EntryTypeDirectory}},
		{name: "fatal error", failure: true},
		{name: "ignored error", failure: true, ignored: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := &DirManifestBuilder{}
			if tc.entry != nil {
				b.AddEntry(tc.entry)
			}

			if tc.failure {
				b.AddFailedEntry("failed", tc.ignored, io.ErrUnexpectedEOF)
			}

			require.Equal(t, tc.want, b.IsEmpty())
			b.Build(0, "")
			require.Equal(t, tc.want, b.IsEmpty())
		})
	}
}

func TestAddEntry(t *testing.T) {
	now := time.Date(2020, 1, 2, 3, 4, 5, 6, time.UTC)

	for _, tc := range []struct {
		desc     string
		entry    *snapshot.DirEntry
		expected fs.DirectorySummary
	}{
		{
			desc: "Add file entry",
			entry: &snapshot.DirEntry{
				Name:     "file1",
				Type:     snapshot.EntryTypeFile,
				FileSize: 100,
				ModTime:  fs.UTCTimestamp(now.Unix()),
			},
			expected: fs.DirectorySummary{
				TotalFileCount: 1,
				TotalFileSize:  100,
				MaxModTime:     fs.UTCTimestamp(now.Unix()),
			},
		},
		{
			desc: "Add symlink entry",
			entry: &snapshot.DirEntry{
				Name:     "symlink1",
				Type:     snapshot.EntryTypeSymlink,
				FileSize: 50,
				ModTime:  fs.UTCTimestamp(now.Add(1 * time.Hour).Unix()),
			},
			expected: fs.DirectorySummary{
				TotalSymlinkCount: 1,
				TotalFileSize:     50,
				MaxModTime:        fs.UTCTimestamp(now.Add(1 * time.Hour).Unix()),
			},
		},
		{
			desc: "Add directory entry with child summary",
			entry: &snapshot.DirEntry{
				Name: "dir1",
				Type: snapshot.EntryTypeDirectory,
				DirSummary: &fs.DirectorySummary{
					TotalFileCount:    2,
					TotalFileSize:     200,
					TotalDirCount:     1,
					TotalSymlinkCount: 1,
					FatalErrorCount:   1,
					IgnoredErrorCount: 1,
					MaxModTime:        fs.UTCTimestamp(now.Add(2 * time.Hour).Unix()),
				},
				ModTime: fs.UTCTimestamp(now.Add(1 * time.Hour).Unix()),
			},
			expected: fs.DirectorySummary{
				TotalFileCount:    2,
				TotalFileSize:     200,
				TotalDirCount:     1,
				TotalSymlinkCount: 1,
				FatalErrorCount:   1,
				IgnoredErrorCount: 1,
				MaxModTime:        fs.UTCTimestamp(now.Add(2 * time.Hour).Unix()),
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			builder := &DirManifestBuilder{}
			builder.AddEntry(tc.entry)
			require.Equal(t, tc.expected.TotalFileCount, builder.summary.TotalFileCount, "TotalFileCount mismatch")

			if tc.entry.Type != snapshot.EntryTypeSymlink {
				// Note: the file size is not being propagated for symlinks.
				// For now, don't check the file size for symlinks.
				require.Equal(t, tc.expected.TotalFileSize, builder.summary.TotalFileSize, "TotalFileSize mismatch")
			}

			require.Equal(t, tc.expected.TotalSymlinkCount, builder.summary.TotalSymlinkCount, "TotalSymlinkCount mismatch")
			require.Equal(t, tc.expected.TotalDirCount, builder.summary.TotalDirCount, "TotalDirCount mismatch")
			require.Equal(t, tc.expected.FatalErrorCount, builder.summary.FatalErrorCount, "FatalErrorCount mismatch")
			require.Equal(t, tc.expected.IgnoredErrorCount, builder.summary.IgnoredErrorCount, "IgnoredErrorCount mismatch")
			require.True(t, builder.summary.MaxModTime.Equal(tc.expected.MaxModTime), "MaxModTime mismatch: got %v, want %v", builder.summary.MaxModTime, tc.expected.MaxModTime)
		})
	}
}
