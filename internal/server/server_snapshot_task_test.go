package server

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/uitask"
	"github.com/kopia/kopia/notification/notifydata"
	"github.com/kopia/kopia/snapshot"
)

func runTestSnapshotTask(t *testing.T, inner func(result *notifydata.ManifestWithError) error) (*notifydata.ManifestWithError, error) {
	t.Helper()

	s := &Server{maxParallelSnapshots: 1, taskmgr: uitask.NewManager(false)}
	s.parallelSnapshotsChanged = sync.NewCond(&s.parallelSnapshotsMutex)

	var got *notifydata.ManifestWithError

	src := snapshot.SourceInfo{Host: "host", UserName: "user", Path: "/path"}

	err := s.runSnapshotTask(testlogging.Context(t), src, func(_ context.Context, _ uitask.Controller, result *notifydata.ManifestWithError) error {
		got = result
		return inner(result)
	})

	return got, err
}

func TestRunSnapshotTask_FailedSnapshotReportsItsTimes(t *testing.T) {
	got, err := runTestSnapshotTask(t, func(*notifydata.ManifestWithError) error {
		return errors.New("upload error")
	})

	require.Error(t, err)
	require.Equal(t, notifydata.StatusCodeFatal, got.StatusCode())
	require.WithinDuration(t, clock.Now(), got.StartTimestamp(), time.Minute)
	require.GreaterOrEqual(t, got.Duration(), time.Duration(0))
	require.Less(t, got.Duration(), time.Minute)
}

func TestRunSnapshotTask_KeepsTimesFromUploadedManifest(t *testing.T) {
	want := snapshot.Manifest{StartTime: 100, EndTime: 200}

	got, err := runTestSnapshotTask(t, func(result *notifydata.ManifestWithError) error {
		result.Manifest = want
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, want.StartTime, got.Manifest.StartTime)
	require.Equal(t, want.EndTime, got.Manifest.EndTime)
}
