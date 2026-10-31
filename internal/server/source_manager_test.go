package server

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/snapshot/policy"
)

func TestSourceManagerBackoffAfterFailedScheduledSnapshot(t *testing.T) {
	sm := &sourceManager{
		pol: policy.SchedulingPolicy{
			IntervalSeconds: int64(time.Hour.Seconds()),
		},
		snapshotRequests: make(chan struct{}, 1),
	}

	initialSnapshotTime := clock.Now().Add(time.Minute)
	sm.nextSnapshotTime = &initialSnapshotTime
	sm.scheduleSnapshotNow()

	_, ok := sm.getNextSnapshotTime()
	require.False(t, ok, "queued snapshot should clear the next snapshot time")

	beforeBackoff := clock.Now()

	sm.backoffBeforeNextSnapshot()

	nextSnapshotTime, ok := sm.getNextSnapshotTime()
	require.True(t, ok, "failed scheduled snapshot should be retried")
	require.WithinDuration(t, beforeBackoff.Add(failedSnapshotRetryInterval), nextSnapshotTime, time.Second)
}

func TestSourceManagerBackoffAfterFailedManualSnapshot(t *testing.T) {
	sm := &sourceManager{
		pol: policy.SchedulingPolicy{
			Manual: true,
		},
		snapshotRequests: make(chan struct{}, 1),
	}

	initialSnapshotTime := clock.Now().Add(time.Minute)
	sm.nextSnapshotTime = &initialSnapshotTime
	sm.scheduleSnapshotNow()
	sm.backoffBeforeNextSnapshot()

	_, ok := sm.getNextSnapshotTime()
	require.False(t, ok, "manual snapshot should not be retried")
}
