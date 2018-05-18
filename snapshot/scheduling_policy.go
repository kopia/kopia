package snapshot

import "time"

// SchedulingPolicy describes policy for scheduling snapshots.
type SchedulingPolicy struct {
	MaxFrequency *time.Duration `json:"frequency"`
}

func mergeSchedulingPolicy(dst, src *SchedulingPolicy) {
	if dst.MaxFrequency == nil {
		dst.MaxFrequency = src.MaxFrequency
	}
}

var defaultSchedulingPolicy = &SchedulingPolicy{}
