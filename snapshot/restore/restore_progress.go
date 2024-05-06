package restore

// Progress is invoked by copier to report status of snapshot restoration.
type Progress interface {
	SetCounters(
		enqueuedCount, restoredCount, skippedCount, ignoredErrors int32,
		enqueuedBytes, restoredBytes, skippedBytes int64,
	)
	Flush()
}
