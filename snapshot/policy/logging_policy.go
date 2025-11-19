package policy

import "github.com/kopia/kopia/snapshot"

// DirLoggingPolicy represents the policy for logging directory information when snapshotting.
type DirLoggingPolicy struct {
	Snapshotted *LogDetail `json:"snapshotted,omitzero"`
	Ignored     *LogDetail `json:"ignored,omitzero"`
}

// DirLoggingPolicyDefinition specifies which policy definition provided the value of a particular field.
type DirLoggingPolicyDefinition struct {
	Snapshotted snapshot.SourceInfo `json:"snapshotted,omitzero"`
	Ignored     snapshot.SourceInfo `json:"ignored,omitzero"`
}

// Merge merges the provided directory logging policy.
func (p *DirLoggingPolicy) Merge(src DirLoggingPolicy, def *DirLoggingPolicyDefinition, si snapshot.SourceInfo) {
	mergeLogLevel(&p.Snapshotted, src.Snapshotted, &def.Snapshotted, si)
	mergeLogLevel(&p.Ignored, src.Ignored, &def.Ignored, si)
}

// EntryLoggingPolicy represents the policy for logging entry information when snapshotting.
type EntryLoggingPolicy struct {
	Snapshotted *LogDetail `json:"snapshotted,omitzero"`
	Ignored     *LogDetail `json:"ignored,omitzero"`
	CacheHit    *LogDetail `json:"cacheHit,omitzero"`
	CacheMiss   *LogDetail `json:"cacheMiss,omitzero"`
}

// EntryLoggingPolicyDefinition specifies which policy definition provided the value of a particular field.
type EntryLoggingPolicyDefinition struct {
	Snapshotted snapshot.SourceInfo `json:"snapshotted,omitzero"`
	Ignored     snapshot.SourceInfo `json:"ignored,omitzero"`
	CacheHit    snapshot.SourceInfo `json:"cacheHit,omitzero"`
	CacheMiss   snapshot.SourceInfo `json:"cacheMiss,omitzero"`
}

// Merge merges the provided entry logging policy.
func (p *EntryLoggingPolicy) Merge(src EntryLoggingPolicy, def *EntryLoggingPolicyDefinition, si snapshot.SourceInfo) {
	mergeLogLevel(&p.Snapshotted, src.Snapshotted, &def.Snapshotted, si)
	mergeLogLevel(&p.Ignored, src.Ignored, &def.Ignored, si)
	mergeLogLevel(&p.CacheHit, src.CacheHit, &def.CacheHit, si)
	mergeLogLevel(&p.CacheMiss, src.CacheMiss, &def.CacheMiss, si)
}

// LoggingPolicy describes policy for emitting logs during snapshots.
type LoggingPolicy struct {
	Directories DirLoggingPolicy   `json:"directories,omitzero"`
	Entries     EntryLoggingPolicy `json:"entries,omitzero"`
}

// LoggingPolicyDefinition specifies which policy definition provided the value of a particular field.
type LoggingPolicyDefinition struct {
	Directories DirLoggingPolicyDefinition   `json:"directories,omitzero"`
	Entries     EntryLoggingPolicyDefinition `json:"entries,omitzero"`
}

// Merge applies default values from the provided policy.
func (p *LoggingPolicy) Merge(src LoggingPolicy, def *LoggingPolicyDefinition, si snapshot.SourceInfo) {
	p.Directories.Merge(src.Directories, &def.Directories, si)
	p.Entries.Merge(src.Entries, &def.Entries, si)
}
