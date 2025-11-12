package policy

import "github.com/kopia/kopia/snapshot"

// ActionsPolicy describes actions to be invoked when taking snapshots.
type ActionsPolicy struct {
	// command runs once before and after the folder it's attached to (not inherited).
	BeforeFolder *ActionCommand `json:"beforeFolder,omitempty"`
	AfterFolder  *ActionCommand `json:"afterFolder,omitempty"`

	// commands run once before and after each snapshot root (can be inherited).
	BeforeSnapshotRoot *ActionCommand `json:"beforeSnapshotRoot,omitempty"`
	AfterSnapshotRoot  *ActionCommand `json:"afterSnapshotRoot,omitempty"`
}

// ActionsPolicyDefinition specifies which policy definition provided the value of a particular field.
type ActionsPolicyDefinition struct {
	BeforeSnapshotRoot snapshot.SourceInfo `json:"beforeSnapshotRoot,omitempty"`
	AfterSnapshotRoot  snapshot.SourceInfo `json:"afterSnapshotRoot,omitempty"`
}

// ActionCommand configures a action command.
type ActionCommand struct {
	// command + args to run
	Command   string   `json:"path,omitempty"`
	Arguments []string `json:"args,omitempty"`

	// alternatively inline script to run using either Unix shell or cmd.exe on Windows.
	Script string `json:"script,omitempty"`

	TimeoutSeconds int    `json:"timeout,omitempty"`
	Mode           string `json:"mode,omitempty"` // essential,optional,async
}

// Merge applies default values from the provided policy.
func (p *ActionsPolicy) Merge(src ActionsPolicy, def *ActionsPolicyDefinition, si snapshot.SourceInfo) {
	mergeActionCommand(&p.BeforeSnapshotRoot, src.BeforeSnapshotRoot, &def.BeforeSnapshotRoot, si)
	mergeActionCommand(&p.AfterSnapshotRoot, src.AfterSnapshotRoot, &def.AfterSnapshotRoot, si)
}

// MergeNonInheritable copies non-inheritable properties from the provided actions policy.
func (p *ActionsPolicy) MergeNonInheritable(src ActionsPolicy) {
	p.BeforeFolder = src.BeforeFolder
	p.AfterFolder = src.AfterFolder
}
