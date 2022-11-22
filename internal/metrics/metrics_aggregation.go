package metrics

// AggregateSnapshots computes aggregate of the provided snapshots.
func AggregateSnapshots(snapshots []Snapshot) Snapshot {
	result := createSnapshot()

	for _, s := range snapshots {
		result.mergeFrom(s)
	}

	return result
}
