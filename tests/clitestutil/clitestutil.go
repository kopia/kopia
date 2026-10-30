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
	ObjectID         string
	SnapshotID       string
	Time             time.Time
	Incomplete       bool
	IncompleteReason string
}

// MustParseSnapshots parsers the output of 'snapshot list'.
func MustParseSnapshots(tb testing.TB, lines []string) []SourceInfo {
	tb.Helper()

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
				tb.Errorf("snapshot without a source: %q", l)
				return nil
			}

			currentSource.Snapshots = append(currentSource.Snapshots, mustParseSnapshotInfo(tb, l[2:]))

			continue
		}

		s := mustParseSourceInfo(tb, l)
		result = append(result, s)
		currentSource = &result[len(result)-1]
	}

	return result
}

func mustParseSnapshotInfo(tb testing.TB, l string) SnapshotInfo {
	tb.Helper()

	incomplete := strings.Contains(l, "incomplete")

	parts := strings.Split(l, " ")

	ts, err := time.Parse("2006-01-02 15:04:05 MST", strings.Join(parts[0:3], " "))
	if err != nil {
		tb.Fatalf("err: %v", err)
	}

	var manifestField string

	if incomplete {
		manifestField = parts[8]
	} else {
		manifestField = parts[7]
	}

	snapID := strings.TrimPrefix(manifestField, "manifest:")

	incompleteReason := ""
	if incomplete {
		incompleteReason = strings.Split(parts[4], ":")[1]
	}

	return SnapshotInfo{
		Time:             ts,
		ObjectID:         parts[3],
		SnapshotID:       snapID,
		Incomplete:       incomplete,
		IncompleteReason: incompleteReason,
	}
}

func mustParseSourceInfo(tb testing.TB, l string) SourceInfo {
	tb.Helper()

	p1 := strings.Index(l, "@")
	p2 := strings.Index(l, ":")

	if p1 >= 0 && p2 > p1 {
		return SourceInfo{User: l[0:p1], Host: l[p1+1 : p2], Path: l[p2+1:]}
	}

	tb.Fatalf("can't parse source info: %q", l)

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
	RunAndExpectSuccess(t testing.TB, args ...string) []string
}

// ListSnapshotsAndExpectSuccess lists given snapshots and parses the output.
func ListSnapshotsAndExpectSuccess(tb testing.TB, e testEnv, targets ...string) []SourceInfo {
	tb.Helper()

	lines := e.RunAndExpectSuccess(tb, append([]string{"snapshot", "list", "-l", "--manifest-id"}, targets...)...)

	return MustParseSnapshots(tb, lines)
}

// ListDirectory lists a given directory and returns directory entries.
func ListDirectory(tb testing.TB, e testEnv, targets ...string) []DirEntry {
	tb.Helper()

	lines := e.RunAndExpectSuccess(tb, append([]string{"ls", "-l"}, targets...)...)

	return mustParseDirectoryEntries(lines)
}

// ListDirectoryRecursive lists a given directory recursively and returns directory entries.
func ListDirectoryRecursive(tb testing.TB, e testEnv, targets ...string) []DirEntry {
	tb.Helper()

	lines := e.RunAndExpectSuccess(tb, append([]string{"ls", "-lr"}, targets...)...)

	return mustParseDirectoryEntries(lines)
}
