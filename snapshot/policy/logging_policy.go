package policy

import "github.com/kopia/kopia/snapshot"

// DirLoggingPolicy represents the policy for logging directory information when snapshotting.
type DirLoggingPolicy struct {
	Snapshotted *LogDetail `json:"snapshotted,omitempty"`
	Ignored     *LogDetail `json:"ignored,omitempty"`
}

// DirLoggingPolicyDefinition specifies which policy definition provided the value of a particular field.
type DirLoggingPolicyDefinition struct {
	Snapshotted snapshot.SourceInfo `json:"snapshotted"`
	Ignored     snapshot.SourceInfo `json:"ignored"`
}

// Merge merges the provided directory logging policy.
func (p *DirLoggingPolicy) Merge(src DirLoggingPolicy, def *DirLoggingPolicyDefinition, si snapshot.SourceInfo) {
	mergeLogLevel(&p.Snapshotted, src.Snapshotted, &def.Snapshotted, si)
	mergeLogLevel(&p.Ignored, src.Ignored, &def.Ignored, si)
}

// EntryLoggingPolicy represents the policy for logging entry information when snapshotting.
type EntryLoggingPolicy struct {
	Snapshotted *LogDetail `json:"snapshotted,omitempty"`
	Ignored     *LogDetail `json:"ignored,omitempty"`
	CacheHit    *LogDetail `json:"cacheHit,omitempty"`
	CacheMiss   *LogDetail `json:"cacheMiss,omitempty"`
}

// EntryLoggingPolicyDefinition specifies which policy definition provided the value of a particular field.
type EntryLoggingPolicyDefinition struct {
	Snapshotted snapshot.SourceInfo `json:"snapshotted"`
	Ignored     snapshot.SourceInfo `json:"ignored"`
	CacheHit    snapshot.SourceInfo `json:"cacheHit"`
	CacheMiss   snapshot.SourceInfo `json:"cacheMiss"`
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
	Directories DirLoggingPolicy   `json:"directories"`
	Entries     EntryLoggingPolicy `json:"entries"`
}

// LoggingPolicyDefinition specifies which policy definition provided the value of a particular field.
type LoggingPolicyDefinition struct {
	Directories DirLoggingPolicyDefinition   `json:"directories"`
	Entries     EntryLoggingPolicyDefinition `json:"entries"`
}

// Merge applies default values from the provided policy.
func (p *LoggingPolicy) Merge(src LoggingPolicy, def *LoggingPolicyDefinition, si snapshot.SourceInfo) {
	p.Directories.Merge(src.Directories, &def.Directories, si)
	p.Entries.Merge(src.Entries, &def.Entries, si)
}
