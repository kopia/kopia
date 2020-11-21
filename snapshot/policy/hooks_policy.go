package policy

// HooksPolicy describes hooks to be invoked when taking snapshots.
type HooksPolicy struct {
	// command runs once before and after the folder it's attached to (not inherited).
	BeforeFolder *HookCommand `json:"beforeFolder,omitempty"`
	AfterFolder  *HookCommand `json:"afterFolder,omitempty"`

	// commands run once before and after each snapshot root (can be inherited).
	BeforeSnapshotRoot *HookCommand `json:"beforeSnapshotRoot,omitempty"`
	AfterSnapshotRoot  *HookCommand `json:"afterSnapshotRoot,omitempty"`
}

// HookCommand configures a hook command.
type HookCommand struct {
	// command + args to run
	Command   string   `json:"path,omitempty"`
	Arguments []string `json:"args,omitempty"`

	// alternatively inline script to run using either Unix shell or cmd.exe on Windows.
	Script string `json:"script,omitempty"`

	TimeoutSeconds int    `json:"timeout,omitempty"`
	Mode           string `json:"mode,omitempty"` // essential,optional,async
}

// Merge applies default values from the provided policy.
// nolint:gocritic
func (p *HooksPolicy) Merge(src HooksPolicy) {
	if p.BeforeSnapshotRoot == nil {
		p.BeforeSnapshotRoot = src.BeforeSnapshotRoot
	}

	if p.AfterSnapshotRoot == nil {
		p.AfterSnapshotRoot = src.AfterSnapshotRoot
	}
}

// MergeNonInheritable copies non-inheritable properties from the provided hooks policy.
func (p *HooksPolicy) MergeNonInheritable(src HooksPolicy) {
	p.BeforeFolder = src.BeforeFolder
	p.AfterFolder = src.AfterFolder
}

// defaultHooksPolicy is the default hooks policy.
var defaultHooksPolicy = HooksPolicy{}
