package format_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/repo/format"
)

func TestUpgradeLockIntentUpdatesWithAdvanceNotice(t *testing.T) {
	oldLock := format.UpgradeLockIntent{
		OwnerID:                "upgrade-owner",
		CreationTime:           clock.Now(),
		AdvanceNoticeDuration:  time.Hour,
		IODrainTimeout:         15 * time.Minute,
		StatusPollInterval:     60 * time.Second,
		Message:                "upgrading from format version 2 -> 3",
		MaxPermittedClockDrift: 5 * time.Second,
	}

	// verify that we can increment the lock's advance notice
	newLock := oldLock.Clone()
	newLock.AdvanceNoticeDuration += 2 * time.Hour
	mergedLock, err := oldLock.Update(newLock)
	require.NoError(t, err)
	require.Equal(t, newLock.AdvanceNoticeDuration, mergedLock.AdvanceNoticeDuration)

	// verify that we cannot make an update to the lock that prepones the
	// upgrade time
	newLock = oldLock.Clone()
	newLock.AdvanceNoticeDuration -= 1 * time.Minute
	mergedLock, err = oldLock.Update(newLock)
	require.EqualError(t, err, "can only extend the upgrade-time on an existing lock")
	require.Nil(t, mergedLock)

	// verify that we cannot make an update to the lock that is prepared by a
	// different owner
	newLock = oldLock.Clone()
	newLock.AdvanceNoticeDuration += 1 * time.Minute
	newLock.OwnerID += "-different"
	mergedLock, err = oldLock.Update(newLock)
	require.EqualError(t, err,
		fmt.Sprintf("upgrade owner-id mismatch %q != %q, you are not the owner of the upgrade lock",
			newLock.OwnerID, oldLock.OwnerID))
	require.Nil(t, mergedLock)

	// verify that we cannot unset the advance notice
	newLock = oldLock.Clone()
	newLock.AdvanceNoticeDuration = 0
	mergedLock, err = oldLock.Update(newLock)
	require.EqualError(t, err, "cannot unset advance notice an on existing lock")
	require.Nil(t, mergedLock)
}

func TestUpgradeLockIntentUpdatesWithoutAdvanceNotice(t *testing.T) {
	oldLock := format.UpgradeLockIntent{
		OwnerID:                "upgrade-owner",
		CreationTime:           clock.Now(),
		AdvanceNoticeDuration:  0, /* no advance notice */
		IODrainTimeout:         15 * time.Minute,
		StatusPollInterval:     60 * time.Second,
		Message:                "upgrading from format version 2 -> 3",
		MaxPermittedClockDrift: 5 * time.Second,
	}

	// verify that we cannot set an advance notice on an existing lock which does
	// not have it set
	newLock := oldLock.Clone()
	newLock.AdvanceNoticeDuration = 2 * time.Hour // Set a new advance notice
	mergedLock, err := oldLock.Update(newLock)
	require.EqualError(t, err, "cannot set an advance notice an on existing lock")
	require.Nil(t, mergedLock)
}

func TestUpgradeLockIntentValidation(t *testing.T) {
	var l format.UpgradeLockIntent

	require.EqualError(t, l.Validate(), "no owner-id set, it is required to set a unique owner-id")
	l.OwnerID = "new-owner"

	require.EqualError(t, l.Validate(), "upgrade lock intent creation time is not set")
	l.CreationTime = clock.Now()

	require.EqualError(t, l.Validate(), "io-drain-timeout is required to be set for the upgrade lock")
	l.IODrainTimeout = 15 * time.Minute

	l.StatusPollInterval = l.IODrainTimeout * 2
	require.EqualError(t, l.Validate(), "status-poll-interval must be less than or equal to the io-drain-timeout")
	l.StatusPollInterval = l.IODrainTimeout

	require.EqualError(t, l.Validate(), "please set an upgrade message for visibility")
	l.Message = "upgrading from format version 2 -> 3"

	require.EqualError(t, l.Validate(), "max-permitted-clock-drift is not set")
	l.MaxPermittedClockDrift = 5 * time.Second

	require.NoError(t, l.Validate())

	l.AdvanceNoticeDuration = -1 * time.Hour
	require.EqualError(t, l.Validate(), fmt.Sprintf("the advanced notice duration %s cannot be negative", l.AdvanceNoticeDuration))

	// set too low advance notice
	l.AdvanceNoticeDuration = 1 * time.Minute
	require.EqualError(t, l.Validate(), fmt.Sprintf("the advanced notice duration %s must be more than the total drain interval %s",
		l.AdvanceNoticeDuration, l.MaxPermittedClockDrift+2*l.IODrainTimeout))

	l.AdvanceNoticeDuration = 1 * time.Hour

	require.NoError(t, l.Validate())
}

func TestUpgradeLockIntentImmediateLock(t *testing.T) {
	now := clock.Now()

	var l *format.UpgradeLockIntent

	// checking lock status on nil lock
	locked, writersDrained := l.IsLocked(now)
	require.False(t, locked)
	require.False(t, writersDrained)

	// negative drain-timeout will lead to a panic
	require.PanicsWithValue(t,
		"writers have drained but we are not locked, this is not possible until the upgrade-lock intent is invalid",
		func() {
			tmp := format.UpgradeLockIntent{
				OwnerID:                "",
				CreationTime:           now,
				AdvanceNoticeDuration:  1 * time.Hour,
				IODrainTimeout:         -1 * time.Hour,
				StatusPollInterval:     0,
				Message:                "upgrading from format version 2 -> 3",
				MaxPermittedClockDrift: 0,
			}
			tmp.IsLocked(now.Add(2 * time.Hour))
		})

	l = &format.UpgradeLockIntent{
		OwnerID:                "upgrade-owner",
		CreationTime:           now,
		AdvanceNoticeDuration:  0, /* no advance notice */
		IODrainTimeout:         15 * time.Minute,
		StatusPollInterval:     60 * time.Second,
		Message:                "upgrading from format version 2 -> 3",
		MaxPermittedClockDrift: 5 * time.Second,
	}

	// Verify that the lock intent has been placed but is not fully established
	// (writers drained) at the time of taking the lock
	locked, writersDrained = l.IsLocked(now)
	require.True(t, locked)
	require.False(t, writersDrained)

	// Verify that the lock intent is not fully established
	// (writers drained) after the drain timeout has expired
	locked, writersDrained = l.IsLocked(now.Add(l.IODrainTimeout))
	require.True(t, locked)
	require.False(t, writersDrained)

	// Verify that the lock intent is not fully established
	// (writers drained) after twice the drain timeout has expired
	locked, writersDrained = l.IsLocked(now.Add(2 * l.IODrainTimeout))
	require.True(t, locked)
	require.False(t, writersDrained)

	// Verify that the lock intent is fully established
	// (writers drained) after twice the drain timeout + clock drift has expired
	locked, writersDrained = l.IsLocked(now.Add(l.MaxPermittedClockDrift + 2*l.IODrainTimeout))
	require.True(t, locked)
	require.True(t, writersDrained)

	// Verify that the lock intent is fully established
	// (writers drained) at the time of upgrade
	locked, writersDrained = l.IsLocked(l.UpgradeTime())
	require.True(t, locked)
	require.True(t, writersDrained)
}

func TestUpgradeLockIntentSufficientAdvanceLock(t *testing.T) {
	now := clock.Now()
	l := format.UpgradeLockIntent{
		OwnerID:                "upgrade-owner",
		CreationTime:           now,
		AdvanceNoticeDuration:  6 * time.Hour,
		IODrainTimeout:         15 * time.Minute,
		StatusPollInterval:     60 * time.Second,
		Message:                "upgrading from format version 2 -> 3",
		MaxPermittedClockDrift: 5 * time.Second,
	}

	// Verify that the lock intent has been placed but is not locked at all,
	// at the time of taking the lock with advance notice
	locked, writersDrained := l.IsLocked(now)
	require.False(t, locked)
	require.False(t, writersDrained)

	// Verify that the lock intent has been placed but is not locked at all,
	// even at the next drain timeout mark
	locked, writersDrained = l.IsLocked(now.Add(l.IODrainTimeout))
	require.False(t, locked)
	require.False(t, writersDrained)

	// Verify that the lock intent has been placed but is not locked at all,
	// even at twice drain timeout mark
	locked, writersDrained = l.IsLocked(now.Add(2 * l.IODrainTimeout))
	require.False(t, locked)
	require.False(t, writersDrained)

	// Verify that the lock intent has been placed but is not locked at all,
	// even at twice the drain timeout mark + clock drift
	locked, writersDrained = l.IsLocked(now.Add(l.MaxPermittedClockDrift + 2*l.IODrainTimeout))
	require.False(t, locked)
	require.False(t, writersDrained)

	// Verify that the lock intent is held (but is not fully established) at
	// (advance notice - drain timeout).
	locked, writersDrained = l.IsLocked(now.Add(l.AdvanceNoticeDuration - l.MaxPermittedClockDrift - 2*l.IODrainTimeout))
	require.True(t, locked)
	require.False(t, writersDrained)

	// Verify that the intent is held and is fully established
	// (writers drained) at the advance notice time
	locked, writersDrained = l.IsLocked(now.Add(l.AdvanceNoticeDuration))
	require.True(t, locked)
	require.True(t, writersDrained)

	// Verify that we can still push the advance notice after being
	// fully-locked
	newLock := l.Clone()
	newLock.AdvanceNoticeDuration += 3 * time.Hour
	mergedLock, err := l.Update(newLock)
	require.NoError(t, err)

	// According to the old lock timings we'd now get unlocked again
	locked, writersDrained = mergedLock.IsLocked(now.Add(l.AdvanceNoticeDuration))
	require.False(t, locked)
	require.False(t, writersDrained)

	// According to the new lock timings we'd get fully-locked again at the new
	// advance notice
	locked, writersDrained = mergedLock.IsLocked(now.Add(mergedLock.AdvanceNoticeDuration))
	require.True(t, locked)
	require.True(t, writersDrained)
}

func TestUpgradeLockIntentInSufficientAdvanceLock(t *testing.T) {
	now := clock.Now()
	l := format.UpgradeLockIntent{
		OwnerID:                "upgrade-owner",
		CreationTime:           now,
		AdvanceNoticeDuration:  20 * time.Minute, /* insufficient time to drain the writers */
		IODrainTimeout:         15 * time.Minute,
		StatusPollInterval:     60 * time.Second,
		Message:                "upgrading from format version 2 -> 3",
		MaxPermittedClockDrift: 5 * time.Second,
	}

	// Verify that the lock intent has been placed and is held right at the
	// creation time because there is insufficient time to drain from the
	// advance notice.
	locked, writersDrained := l.IsLocked(now)
	require.True(t, locked)
	require.False(t, writersDrained)

	// Verify that the lock intent has been placed but is not fully locked at
	// the next drain timeout mark
	locked, writersDrained = l.IsLocked(now.Add(l.IODrainTimeout))
	require.True(t, locked)
	require.False(t, writersDrained)

	// Verify that the lock intent has been placed but is not fully locked at
	// double the drain timeout mark
	locked, writersDrained = l.IsLocked(now.Add(2 * l.IODrainTimeout))
	require.True(t, locked)
	require.False(t, writersDrained)

	// Verify that the lock intent has been placed and is fully established at
	// double the drain timeout + clock drift mark [full drain time]
	locked, writersDrained = l.IsLocked(now.Add(l.MaxPermittedClockDrift + 2*l.IODrainTimeout))
	require.True(t, locked)
	require.True(t, writersDrained)
}

func TestUpgradeLockIntentUpgradeTime(t *testing.T) {
	now := clock.Now()

	var l format.UpgradeLockIntent

	// checking time on nil lock
	require.Equal(t, time.Time{}, l.UpgradeTime())

	l = format.UpgradeLockIntent{
		OwnerID:                "upgrade-owner",
		CreationTime:           now,
		AdvanceNoticeDuration:  20 * time.Minute, /* insufficient time to drain the writers */
		IODrainTimeout:         15 * time.Minute,
		StatusPollInterval:     60 * time.Second,
		Message:                "upgrading from format version 2 -> 3",
		MaxPermittedClockDrift: 5 * time.Second,
	}
	require.Equal(t, now.Add(l.MaxPermittedClockDrift+2*l.IODrainTimeout), l.UpgradeTime())

	l = format.UpgradeLockIntent{
		OwnerID:                "upgrade-owner",
		CreationTime:           now,
		AdvanceNoticeDuration:  20 * time.Hour, /* sufficient time to drain the writers */
		IODrainTimeout:         15 * time.Minute,
		StatusPollInterval:     60 * time.Second,
		Message:                "upgrading from format version 2 -> 3",
		MaxPermittedClockDrift: 5 * time.Second,
	}
	require.Equal(t, now.Add(l.AdvanceNoticeDuration), l.UpgradeTime())

	l = format.UpgradeLockIntent{
		OwnerID:                "upgrade-owner",
		CreationTime:           now,
		AdvanceNoticeDuration:  0, /* immediate lock */
		IODrainTimeout:         15 * time.Minute,
		StatusPollInterval:     60 * time.Second,
		Message:                "upgrading from format version 2 -> 3",
		MaxPermittedClockDrift: 5 * time.Second,
	}
	require.Equal(t, now.Add(l.MaxPermittedClockDrift+2*l.IODrainTimeout), l.UpgradeTime())
}

func TestUpgradeLockIntentClone(t *testing.T) {
	l := &format.UpgradeLockIntent{
		OwnerID:                "upgrade-owner",
		CreationTime:           clock.Now(),
		AdvanceNoticeDuration:  20 * time.Minute,
		IODrainTimeout:         15 * time.Minute,
		StatusPollInterval:     60 * time.Second,
		Message:                "upgrading from format version 2 -> 3",
		MaxPermittedClockDrift: 5 * time.Second,
	}
	require.EqualValues(t, l, l.Clone())
}
