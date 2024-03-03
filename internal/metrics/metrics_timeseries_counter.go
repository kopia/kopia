package metrics

// CounterValue returns a function that extracts given counter value from a snapshot.
func CounterValue(name string) TimeseriesAggregator {
	return TimeseriesAggregator{name}
}

// TimeseriesAggregator handles aggregation of counter values.
type TimeseriesAggregator struct {
	name string
}

// FromSnapshot extracts counter value from a snapshot.
func (c TimeseriesAggregator) FromSnapshot(s *Snapshot) (int64, bool) {
	v, ok := s.Counters[c.name]

	return v, ok
}

// Aggregate aggregates counter values.
func (c TimeseriesAggregator) Aggregate(agg, incoming int64, ratio float64) int64 {
	return agg + int64(float64(incoming)*ratio)
}
