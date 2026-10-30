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
	RetentionPolicy           RetentionPolicy           `json:"retention,omitempty"`
	FilesPolicy               FilesPolicy               `json:"files,omitempty"`
	ErrorHandlingPolicy       ErrorHandlingPolicy       `json:"errorHandling,omitempty"`
	SchedulingPolicy          SchedulingPolicy          `json:"scheduling,omitempty"`
	CompressionPolicy         CompressionPolicy         `json:"compression,omitempty"`
	MetadataCompressionPolicy MetadataCompressionPolicy `json:"metadataCompression,omitempty"`
	SplitterPolicy            SplitterPolicy            `json:"splitter,omitempty"`
	Actions                   ActionsPolicy             `json:"actions,omitempty"`
	OSSnapshotPolicy          OSSnapshotPolicy          `json:"osSnapshots,omitempty"`
	LoggingPolicy             LoggingPolicy             `json:"logging,omitempty"`
	UploadPolicy              UploadPolicy              `json:"upload,omitempty"`
	NoParent                  bool                      `json:"noParent,omitempty"`
}

// Definition corresponds 1:1 to Policy and each field specifies the snapshot.SourceInfo
// where a particular policy field was specified.
type Definition struct {
	RetentionPolicy           RetentionPolicyDefinition           `json:"retention,omitempty"`
	FilesPolicy               FilesPolicyDefinition               `json:"files,omitempty"`
	ErrorHandlingPolicy       ErrorHandlingPolicyDefinition       `json:"errorHandling,omitempty"`
	SchedulingPolicy          SchedulingPolicyDefinition          `json:"scheduling,omitempty"`
	CompressionPolicy         CompressionPolicyDefinition         `json:"compression,omitempty"`
	MetadataCompressionPolicy MetadataCompressionPolicyDefinition `json:"metadataCompression,omitempty"`
	SplitterPolicy            SplitterPolicyDefinition            `json:"splitter,omitempty"`
	Actions                   ActionsPolicyDefinition             `json:"actions,omitempty"`
	OSSnapshotPolicy          OSSnapshotPolicyDefinition          `json:"osSnapshots,omitempty"`
	LoggingPolicy             LoggingPolicyDefinition             `json:"logging,omitempty"`
	UploadPolicy              UploadPolicyDefinition              `json:"upload,omitempty"`
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
