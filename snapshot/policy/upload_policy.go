package policy

import (
	"github.com/pkg/errors"

	"github.com/kopia/kopia/snapshot"
)

// UploadPolicy describes policy to apply when uploading snapshots.
type UploadPolicy struct {
	MaxParallelSnapshots    *OptionalInt   `json:"maxParallelSnapshots,omitempty"`
	MaxParallelFileReads    *OptionalInt   `json:"maxParallelFileReads,omitempty"`
	ParallelUploadAboveSize *OptionalInt64 `json:"parallelUploadAboveSize,omitempty"`

	// ReuseHardlinkContent causes files that are hardlinks of a file already
	// uploaded during the same snapshot to reuse its content without being read
	// again. Hardlinks are detected by device and inode number, which are only
	// reliable when the source is not modified during the snapshot.
	ReuseHardlinkContent *OptionalBool `json:"reuseHardlinkContent,omitempty"`
}

// UploadPolicyDefinition specifies which policy definition provided the value of a particular field.
type UploadPolicyDefinition struct {
	MaxParallelSnapshots    snapshot.SourceInfo `json:"maxParallelSnapshots,omitempty"`
	MaxParallelFileReads    snapshot.SourceInfo `json:"maxParallelFileReads,omitempty"`
	ParallelUploadAboveSize snapshot.SourceInfo `json:"parallelUploadAboveSize,omitempty"`
	ReuseHardlinkContent    snapshot.SourceInfo `json:"reuseHardlinkContent,omitempty"`
}

// Merge applies default values from the provided policy.
func (p *UploadPolicy) Merge(src UploadPolicy, def *UploadPolicyDefinition, si snapshot.SourceInfo) {
	mergeOptionalInt(&p.MaxParallelSnapshots, src.MaxParallelSnapshots, &def.MaxParallelSnapshots, si)
	mergeOptionalInt(&p.MaxParallelFileReads, src.MaxParallelFileReads, &def.MaxParallelFileReads, si)
	mergeOptionalInt64(&p.ParallelUploadAboveSize, src.ParallelUploadAboveSize, &def.ParallelUploadAboveSize, si)
	mergeOptionalBool(&p.ReuseHardlinkContent, src.ReuseHardlinkContent, &def.ReuseHardlinkContent, si)
}

// ValidateUploadPolicy returns an error if manual field is set along with Upload fields.
func ValidateUploadPolicy(si snapshot.SourceInfo, p UploadPolicy) error {
	if si.Path != "" && p.MaxParallelSnapshots != nil {
		return errors.New("max parallel snapshots cannot be specified for paths, only global, username@hostname or @hostname")
	}

	return nil
}
