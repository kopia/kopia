// Package clitestutil contains utilities for
package clitestutil

import (
	"strings"
	"testing"
	"time"
)

// SourceInfo represents a single source (user@host:/path) with its snapshots.
type SourceInfo struct {
	User      string
	Host      string
	Path      string
	Snapshots []SnapshotInfo
}

// SnapshotInfo represents a single snapshot information.
type SnapshotInfo struct {
	ObjectID   string
	SnapshotID string
	Time       time.Time
}

// MustParseSnapshots parsers the output of 'snapshot list'.
func MustParseSnapshots(t *testing.T, lines []string) []SourceInfo {
	t.Helper()

	var (
		result        []SourceInfo
		currentSource *SourceInfo
	)

	for _, l := range lines {
		if l == "" {
			continue
		}

		if strings.HasPrefix(l, "  ") {
			if currentSource == nil {
				t.Errorf("snapshot without a source: %q", l)
				return nil
			}

			currentSource.Snapshots = append(currentSource.Snapshots, mustParseSnaphotInfo(t, l[2:]))

			continue
		}

		s := mustParseSourceInfo(t, l)
		result = append(result, s)
		currentSource = &result[len(result)-1]
	}

	return result
}

func mustParseSnaphotInfo(t *testing.T, l string) SnapshotInfo {
	t.Helper()

	parts := strings.Split(l, " ")

	ts, err := time.Parse("2006-01-02 15:04:05 MST", strings.Join(parts[0:3], " "))
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	manifestField := parts[7]
	snapID := strings.TrimPrefix(manifestField, "manifest:")

	return SnapshotInfo{
		Time:       ts,
		ObjectID:   parts[3],
		SnapshotID: snapID,
	}
}

func mustParseSourceInfo(t *testing.T, l string) SourceInfo {
	t.Helper()

	p1 := strings.Index(l, "@")

	p2 := strings.Index(l, ":")

	if p1 >= 0 && p2 > p1 {
		return SourceInfo{User: l[0:p1], Host: l[p1+1 : p2], Path: l[p2+1:]}
	}

	t.Fatalf("can't parse source info: %q", l)

	return SourceInfo{}
}

// DirEntry represents directory entry.
type DirEntry struct {
	Name     string
	ObjectID string
}

func mustParseDirectoryEntries(lines []string) []DirEntry {
	var result []DirEntry

	for _, l := range lines {
		parts := strings.Fields(l)

		result = append(result, DirEntry{
			Name:     parts[6],
			ObjectID: parts[5],
		})
	}

	return result
}

type testEnv interface {
	RunAndExpectSuccess(t *testing.T, args ...string) []string
}

// ListSnapshotsAndExpectSuccess lists given snapshots and parses the output.
func ListSnapshotsAndExpectSuccess(t *testing.T, e testEnv, targets ...string) []SourceInfo {
	t.Helper()

	lines := e.RunAndExpectSuccess(t, append([]string{"snapshot", "list", "--all", "-l", "--manifest-id"}, targets...)...)

	return MustParseSnapshots(t, lines)
}

// ListDirectory lists a given directory and returns directory entries.
func ListDirectory(t *testing.T, e testEnv, targets ...string) []DirEntry {
	t.Helper()

	lines := e.RunAndExpectSuccess(t, append([]string{"ls", "-l"}, targets...)...)

	return mustParseDirectoryEntries(lines)
}

// ListDirectoryRecursive lists a given directory recursively and returns directory entries.
func ListDirectoryRecursive(t *testing.T, e testEnv, targets ...string) []DirEntry {
	t.Helper()

	lines := e.RunAndExpectSuccess(t, append([]string{"ls", "-lr"}, targets...)...)

	return mustParseDirectoryEntries(lines)
}
