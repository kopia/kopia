package policy

// ActionsPolicy describes actions to be invoked when taking snapshots.
type ActionsPolicy struct {
	// command runs once before and after the folder it's attached to (not inherited).
	BeforeFolder *ActionCommand `json:"beforeFolder,omitempty"`
	AfterFolder  *ActionCommand `json:"afterFolder,omitempty"`

	// commands run once before and after each snapshot root (can be inherited).
	BeforeSnapshotRoot *ActionCommand `json:"beforeSnapshotRoot,omitempty"`
	AfterSnapshotRoot  *ActionCommand `json:"afterSnapshotRoot,omitempty"`
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
// nolint:gocritic
func (p *ActionsPolicy) Merge(src ActionsPolicy) {
	if p.BeforeSnapshotRoot == nil {
		p.BeforeSnapshotRoot = src.BeforeSnapshotRoot
	}

	if p.AfterSnapshotRoot == nil {
		p.AfterSnapshotRoot = src.AfterSnapshotRoot
	}
}

// MergeNonInheritable copies non-inheritable properties from the provided actions policy.
func (p *ActionsPolicy) MergeNonInheritable(src ActionsPolicy) {
	p.BeforeFolder = src.BeforeFolder
	p.AfterFolder = src.AfterFolder
}

// defaultActionsPolicy is the default actions policy.
var defaultActionsPolicy = ActionsPolicy{}
