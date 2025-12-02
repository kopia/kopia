package policy

import (
	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/snapshot"
)

// SplitterPolicy specifies compression policy.
type SplitterPolicy struct {
	Algorithm string `json:"algorithm,omitempty"`
}

// SplitterPolicyDefinition specifies which policy definition provided the value of a particular field.
type SplitterPolicyDefinition struct {
	Algorithm snapshot.SourceInfo `json:"algorithm,omitempty"`
}

// SplitterForFile returns splitter algorithm.
func (p *SplitterPolicy) SplitterForFile(_ fs.Entry) string {
	return p.Algorithm
}

// Merge applies default values from the provided policy.
func (p *SplitterPolicy) Merge(src SplitterPolicy, def *SplitterPolicyDefinition, si snapshot.SourceInfo) {
	mergeString(&p.Algorithm, src.Algorithm, &def.Algorithm, si)
}
