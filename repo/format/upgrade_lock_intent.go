package format

import (
	"time"

	"github.com/pkg/errors"
)

// UpgradeLockIntent represents the intent to lock a kopia repository for upgrade
// related maintenance activity. This signals a request for exclusive access to
// the repository. The lock object is set on the Kopia repository format blob
// 'kopia.repository' and must be respected by all clients accessing the
// repository.
type UpgradeLockIntent struct {
	OwnerID                string        `json:"ownerID,omitempty"`
	CreationTime           time.Time     `json:"creationTime,omitempty"`
	AdvanceNoticeDuration  time.Duration `json:"advanceNoticeDuration,omitempty"`
	IODrainTimeout         time.Duration `json:"ioDrainTimeout,omitempty"`
	StatusPollInterval     time.Duration `json:"statusPollInterval,omitempty"`
	Message                string        `json:"message,omitempty"`
	MaxPermittedClockDrift time.Duration `json:"maxPermittedClockDrift,omitempty"`
}

// Update upgrades an existing lock intent. This method controls what mutations
// are allowed on an upgrade lock once it has been placed on the repository.
func (l *UpgradeLockIntent) Update(other *UpgradeLockIntent) (*UpgradeLockIntent, error) {
	if l.OwnerID != other.OwnerID {
		return nil, errors.Errorf("upgrade owner-id mismatch %q != %q, you are not the owner of the upgrade lock",
			other.OwnerID, l.OwnerID)
	}

	switch {
	case l.AdvanceNoticeDuration == 0:
		if other.AdvanceNoticeDuration != 0 {
			return nil, errors.New("cannot set an advance notice an on existing lock")
		}
	case other.AdvanceNoticeDuration == 0:
		// TODO(small): see if we can do this
		return nil, errors.New("cannot unset advance notice an on existing lock")
	case other.UpgradeTime().Before(l.UpgradeTime()):
		// TODO(small): see if we can jump backwards as well
		return nil, errors.New("can only extend the upgrade-time on an existing lock")
	}

	newL := l.Clone()
	// currently the only allowed update is the notice time
	newL.AdvanceNoticeDuration = other.AdvanceNoticeDuration

	return newL, nil
}

// Clone creates a copy of the UpgradeLock instance.
func (l *UpgradeLockIntent) Clone() *UpgradeLockIntent {
	if l == nil {
		return nil
	}

	clone := *l

	return &clone
}

// Validate verifies the parameters of an upgrade lock.
func (l *UpgradeLockIntent) Validate() error {
	if l.OwnerID == "" {
		return errors.New("no owner-id set, it is required to set a unique owner-id")
	}

	if l.CreationTime.IsZero() {
		return errors.New("upgrade lock intent creation time is not set")
	}

	if l.IODrainTimeout <= 0 {
		return errors.New("io-drain-timeout is required to be set for the upgrade lock")
	}

	if l.StatusPollInterval > l.IODrainTimeout {
		return errors.New("status-poll-interval must be less than or equal to the io-drain-timeout")
	}

	if l.Message == "" {
		return errors.New("please set an upgrade message for visibility")
	}

	if l.MaxPermittedClockDrift <= 0 {
		return errors.New("max-permitted-clock-drift is not set")
	}

	if l.AdvanceNoticeDuration != 0 {
		if l.AdvanceNoticeDuration < 0 {
			return errors.Errorf("the advanced notice duration %s cannot be negative", l.AdvanceNoticeDuration)
		}

		totalDrainInterval := l.totalDrainInterval()
		if l.AdvanceNoticeDuration <= totalDrainInterval {
			return errors.Errorf("the advanced notice duration %s must be more than the total drain interval %s",
				l.AdvanceNoticeDuration, totalDrainInterval)
		}
	}

	return nil
}

// UpgradeTime returns the absolute time in future by when the upgrade lock
// will be fully established, i.e. all non-upgrading-owner kopia accessors
// would be drained.
func (l *UpgradeLockIntent) UpgradeTime() time.Time {
	if l == nil {
		return time.Time{}
	}

	var (
		upgradeTime        time.Time
		totalDrainInterval = l.totalDrainInterval()
	)

	if l.AdvanceNoticeDuration > totalDrainInterval {
		upgradeTime = l.CreationTime.Add(l.AdvanceNoticeDuration)
	} else {
		upgradeTime = l.CreationTime.Add(totalDrainInterval)
	}

	return upgradeTime
}

func (l *UpgradeLockIntent) totalDrainInterval() time.Duration {
	return l.MaxPermittedClockDrift + 2*l.IODrainTimeout
}

// IsLocked indicates whether a lock intent has been placed and whether all
// other repository accessors have been drained.
func (l *UpgradeLockIntent) IsLocked(now time.Time) (locked, writersDrained bool) {
	if l == nil {
		return false, false
	}

	totalDrainInterval := l.totalDrainInterval()
	locked = l.AdvanceNoticeDuration < totalDrainInterval /* insufficient or no advance notice means immediate lock */ ||
		!now.Before(l.CreationTime.Add(l.AdvanceNoticeDuration-totalDrainInterval)) // are we approaching the notice window ?
	writersDrained = !now.Before(l.UpgradeTime())

	if writersDrained && !locked {
		panic("writers have drained but we are not locked, this is not possible until the upgrade-lock intent is invalid")
	}

	return locked, writersDrained
}
