package metrics

import "time"

// DurationDistributionValue returns a function that aggregates on given duration distribution value from a snapshot.
func DurationDistributionValue(name string) DurationDistributionValueAggregator {
	return DurationDistributionValueAggregator{name}
}

// DurationDistributionValueAggregator handles aggregation of counter values.
type DurationDistributionValueAggregator struct {
	name string
}

// FromSnapshot extracts counter value from a snapshot.
func (c DurationDistributionValueAggregator) FromSnapshot(s *Snapshot) (*DistributionState[time.Duration], bool) {
	v, ok := s.DurationDistributions[c.name]

	return v, ok
}

// Aggregate aggregates counter values.
func (c DurationDistributionValueAggregator) Aggregate(previousAggregate, incoming *DistributionState[time.Duration], ratio float64) *DistributionState[time.Duration] {
	if previousAggregate == nil {
		previousAggregate = &DistributionState[time.Duration]{}
	}

	previousAggregate.mergeScaledFrom(incoming, ratio)

	return previousAggregate
}

var _ SnapshotValueAggregator[*DistributionState[time.Duration]] = DurationDistributionValueAggregator{}
