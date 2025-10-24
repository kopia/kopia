package policy

import (
	"bytes"
	"encoding/json"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/snapshot"
)

// ErrPolicyNotFound is returned when the policy is not found.
var ErrPolicyNotFound = errors.New("policy not found")

// TargetWithPolicy wraps a policy with its target and ID.
type TargetWithPolicy struct {
	ID     string              `json:"id"`
	Target snapshot.SourceInfo `json:"target"`
	*Policy
}

// Policy describes snapshot policy for a single source.
type Policy struct {
	Labels                    map[string]string         `json:"-"`
	RetentionPolicy           RetentionPolicy           `json:"retention,omitzero"`
	FilesPolicy               FilesPolicy               `json:"files,omitzero"`
	ErrorHandlingPolicy       ErrorHandlingPolicy       `json:"errorHandling,omitzero"`
	SchedulingPolicy          SchedulingPolicy          `json:"scheduling,omitzero"`
	CompressionPolicy         CompressionPolicy         `json:"compression,omitzero"`
	MetadataCompressionPolicy MetadataCompressionPolicy `json:"metadataCompression,omitzero"`
	SplitterPolicy            SplitterPolicy            `json:"splitter,omitzero"`
	Actions                   ActionsPolicy             `json:"actions,omitzero"`
	OSSnapshotPolicy          OSSnapshotPolicy          `json:"osSnapshots,omitzero"`
	LoggingPolicy             LoggingPolicy             `json:"logging,omitzero"`
	UploadPolicy              UploadPolicy              `json:"upload,omitzero"`
	NoParent                  bool                      `json:"noParent,omitempty"`
}

// Definition corresponds 1:1 to Policy and each field specifies the snapshot.SourceInfo
// where a particular policy field was specified.
type Definition struct {
	RetentionPolicy           RetentionPolicyDefinition           `json:"retention,omitzero"`
	FilesPolicy               FilesPolicyDefinition               `json:"files,omitzero"`
	ErrorHandlingPolicy       ErrorHandlingPolicyDefinition       `json:"errorHandling,omitzero"`
	SchedulingPolicy          SchedulingPolicyDefinition          `json:"scheduling,omitzero"`
	CompressionPolicy         CompressionPolicyDefinition         `json:"compression,omitzero"`
	MetadataCompressionPolicy MetadataCompressionPolicyDefinition `json:"metadataCompression,omitzero"`
	SplitterPolicy            SplitterPolicyDefinition            `json:"splitter,omitzero"`
	Actions                   ActionsPolicyDefinition             `json:"actions,omitzero"`
	OSSnapshotPolicy          OSSnapshotPolicyDefinition          `json:"osSnapshots,omitzero"`
	LoggingPolicy             LoggingPolicyDefinition             `json:"logging,omitzero"`
	UploadPolicy              UploadPolicyDefinition              `json:"upload,omitzero"`
}

func (p *Policy) String() string {
	var buf bytes.Buffer

	e := json.NewEncoder(&buf)
	e.SetIndent("", "  ")

	if err := e.Encode(p); err != nil {
		return "unable to policy as JSON: " + err.Error()
	}

	return buf.String()
}

// ID returns globally unique identifier of the policy.
func (p *Policy) ID() string {
	return p.Labels["id"]
}

// Target returns the snapshot.SourceInfo describing username, host and path targeted by the policy.
func (p *Policy) Target() snapshot.SourceInfo {
	return snapshot.SourceInfo{
		Host:     p.Labels["hostname"],
		UserName: p.Labels["username"],
		Path:     p.Labels["path"],
	}
}

// ValidatePolicy returns error if the given policy is invalid.
// Currently, only SchedulingPolicy is validated.
func ValidatePolicy(si snapshot.SourceInfo, pol *Policy) error {
	if err := ValidateSchedulingPolicy(pol.SchedulingPolicy); err != nil {
		return errors.Wrap(err, "invalid scheduling policy")
	}

	if err := ValidateUploadPolicy(si, pol.UploadPolicy); err != nil {
		return errors.Wrap(err, "invalid upload policy")
	}

	return nil
}

// validatePolicyPath validates that the provided policy path is valid and the path exists.
func validatePolicyPath(p string) error {
	if isSlashOrBackslash(p[len(p)-1]) && !isRootPath(p) {
		return errors.New("path cannot end with a slash or a backslash")
	}

	return nil
}
