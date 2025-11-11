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
	RetentionPolicy           RetentionPolicy           `json:"retention"`
	FilesPolicy               FilesPolicy               `json:"files"`
	ErrorHandlingPolicy       ErrorHandlingPolicy       `json:"errorHandling"`
	SchedulingPolicy          SchedulingPolicy          `json:"scheduling"`
	CompressionPolicy         CompressionPolicy         `json:"compression"`
	MetadataCompressionPolicy MetadataCompressionPolicy `json:"metadataCompression"`
	SplitterPolicy            SplitterPolicy            `json:"splitter"`
	Actions                   ActionsPolicy             `json:"actions"`
	OSSnapshotPolicy          OSSnapshotPolicy          `json:"osSnapshots"`
	LoggingPolicy             LoggingPolicy             `json:"logging"`
	UploadPolicy              UploadPolicy              `json:"upload"`
	NoParent                  bool                      `json:"noParent,omitempty"`
}

// Definition corresponds 1:1 to Policy and each field specifies the snapshot.SourceInfo
// where a particular policy field was specified.
type Definition struct {
	RetentionPolicy           RetentionPolicyDefinition           `json:"retention"`
	FilesPolicy               FilesPolicyDefinition               `json:"files"`
	ErrorHandlingPolicy       ErrorHandlingPolicyDefinition       `json:"errorHandling"`
	SchedulingPolicy          SchedulingPolicyDefinition          `json:"scheduling"`
	CompressionPolicy         CompressionPolicyDefinition         `json:"compression"`
	MetadataCompressionPolicy MetadataCompressionPolicyDefinition `json:"metadataCompression"`
	SplitterPolicy            SplitterPolicyDefinition            `json:"splitter"`
	Actions                   ActionsPolicyDefinition             `json:"actions"`
	OSSnapshotPolicy          OSSnapshotPolicyDefinition          `json:"osSnapshots"`
	LoggingPolicy             LoggingPolicyDefinition             `json:"logging"`
	UploadPolicy              UploadPolicyDefinition              `json:"upload"`
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
