package policy

import "github.com/kopia/kopia/snapshot"

// OSSnapshotPolicy describes settings for OS-level snapshots.
type OSSnapshotPolicy struct {
	VolumeShadowCopy VolumeShadowCopyPolicy `json:"volumeShadowCopy,omitempty"`
}

// OSSnapshotPolicyDefinition specifies which policy definition provided the value of a particular field.
type OSSnapshotPolicyDefinition struct {
	VolumeShadowCopy VolumeShadowCopyPolicyDefinition `json:"volumeShadowCopy,omitempty"`
}

// Merge applies default values from the provided policy.
func (p *OSSnapshotPolicy) Merge(src OSSnapshotPolicy, def *OSSnapshotPolicyDefinition, si snapshot.SourceInfo) {
	p.VolumeShadowCopy.Merge(src.VolumeShadowCopy, &def.VolumeShadowCopy, si)
}

// VolumeShadowCopyPolicy describes settings for Windows Volume Shadow Copy
// snapshots.
type VolumeShadowCopyPolicy struct {
	Enable *OSSnapshotMode `json:"enable,omitempty"`
}

// VolumeShadowCopyPolicyDefinition specifies which policy definition provided
// the value of a particular field.
type VolumeShadowCopyPolicyDefinition struct {
	Enable snapshot.SourceInfo `json:"enable,omitempty"`
}

// Merge applies default values from the provided policy.
func (p *VolumeShadowCopyPolicy) Merge(src VolumeShadowCopyPolicy, def *VolumeShadowCopyPolicyDefinition, si snapshot.SourceInfo) {
	mergeOSSnapshotMode(&p.Enable, src.Enable, &def.Enable, si)
}

// OSSnapshotMode specifies whether OS-level snapshots are used for file systems
// that support them.
//
//nolint:recvcheck
type OSSnapshotMode byte

// OS-level snapshot modes.
const (
	OSSnapshotNever         OSSnapshotMode = iota // Disable OS-level snapshots
	OSSnapshotAlways                              // Fail if an OS-level snapshot cannot be created
	OSSnapshotWhenAvailable                       // Fall back to regular file access on error
)

// OS-level snapshot mode strings.
const (
	OSSnapshotNeverString         = "never"
	OSSnapshotAlwaysString        = "always"
	OSSnapshotWhenAvailableString = "when-available"
)

// NewOSSnapshotMode provides an OptionalBool pointer.
func NewOSSnapshotMode(m OSSnapshotMode) *OSSnapshotMode {
	return &m
}

// OrDefault returns the OS snapshot mode or the provided default.
func (m *OSSnapshotMode) OrDefault(def OSSnapshotMode) OSSnapshotMode {
	if m == nil {
		return def
	}

	return *m
}

func (m OSSnapshotMode) String() string {
	switch m {
	case OSSnapshotAlways:
		return OSSnapshotAlwaysString
	case OSSnapshotWhenAvailable:
		return OSSnapshotWhenAvailableString
	default:
		return OSSnapshotNeverString
	}
}

func mergeOSSnapshotMode(target **OSSnapshotMode, src *OSSnapshotMode, def *snapshot.SourceInfo, si snapshot.SourceInfo) {
	if *target == nil && src != nil {
		v := *src
		*target = &v
		*def = si
	}
}
