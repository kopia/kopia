package metrics

// SizeDistributionValue returns a function that aggregates on given duration distribution value from a snapshot.
func SizeDistributionValue(name string) SizeDistributionValueAggregator {
	return SizeDistributionValueAggregator{name}
}

// SizeDistributionValueAggregator handles aggregation of counter values.
type SizeDistributionValueAggregator struct {
	name string
}

// FromSnapshot extracts counter value from a snapshot.
func (c SizeDistributionValueAggregator) FromSnapshot(s *Snapshot) (*DistributionState[int64], bool) {
	v, ok := s.SizeDistributions[c.name]

	return v, ok
}

// Aggregate aggregates counter values.
func (c SizeDistributionValueAggregator) Aggregate(previousAggregate, incoming *DistributionState[int64], ratio float64) *DistributionState[int64] {
	if previousAggregate == nil {
		previousAggregate = &DistributionState[int64]{}
	}

	previousAggregate.mergeScaledFrom(incoming, ratio)

	return previousAggregate
}

var _ SnapshotValueAggregator[*DistributionState[int64]] = SizeDistributionValueAggregator{}
