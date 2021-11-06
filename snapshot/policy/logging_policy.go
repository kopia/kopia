package policy

// DirLoggingPolicy represents the policy for logging directory information when snapshotting.
type DirLoggingPolicy struct {
	Snapshotted *LogDetail `json:"snapshotted,omitempty"`
	Ignored     *LogDetail `json:"ignored,omitempty"`
}

// Merge merges the provided directory logging policy.
func (p *DirLoggingPolicy) Merge(src DirLoggingPolicy) {
	mergeLogLevel(&p.Snapshotted, src.Snapshotted)
	mergeLogLevel(&p.Ignored, src.Ignored)
}

// EntryLoggingPolicy represents the policy for logging entry information when snapshotting.
type EntryLoggingPolicy struct {
	Snapshotted *LogDetail `json:"snapshotted,omitempty"`
	Ignored     *LogDetail `json:"ignored,omitempty"`
	CacheHit    *LogDetail `json:"cacheHit,omitempty"`
	CacheMiss   *LogDetail `json:"cacheMiss,omitempty"`
}

// Merge merges the provided entry logging policy.
func (p *EntryLoggingPolicy) Merge(src EntryLoggingPolicy) {
	mergeLogLevel(&p.Snapshotted, src.Snapshotted)
	mergeLogLevel(&p.Ignored, src.Ignored)
	mergeLogLevel(&p.CacheHit, src.CacheHit)
	mergeLogLevel(&p.CacheMiss, src.CacheMiss)
}

// LoggingPolicy describes policy for emitting logs during snapshots.
type LoggingPolicy struct {
	Directories DirLoggingPolicy   `json:"directories,omitempty"`
	Entries     EntryLoggingPolicy `json:"entries,omitempty"`
}

// Merge applies default values from the provided policy.
func (p *LoggingPolicy) Merge(src LoggingPolicy) {
	p.Directories.Merge(src.Directories)
	p.Entries.Merge(src.Entries)
}

// defaultLoggingPolicy is the default logs policy.
var defaultLoggingPolicy = LoggingPolicy{
	Directories: DirLoggingPolicy{
		Snapshotted: NewLogDetail(LogDetailNormal),
		Ignored:     NewLogDetail(LogDetailNormal),
	},
	Entries: EntryLoggingPolicy{
		Snapshotted: NewLogDetail(LogDetailNone),
		Ignored:     NewLogDetail(LogDetailNormal),
		CacheHit:    NewLogDetail(LogDetailNone),
		CacheMiss:   NewLogDetail(LogDetailNone),
	},
}
