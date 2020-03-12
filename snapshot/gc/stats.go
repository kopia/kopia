package gc

// Stats contains statistics about a GC run
type Stats struct {
	// Keep int64 fields first to ensure they get aligned to at least 64-bit
	// boundaries which is required for atomic access on ARM and x86-32.
	// Also results in a smaller struct size
	UnusedBytes, InUseBytes, SystemBytes, TooRecentBytes int64
	UnusedCount, InUseCount, SystemCount, TooRecentCount uint32
}
