package cli

import (
	"github.com/kopia/kopia/repo/maintenance"
)

type commandSnapshotGC struct {
	snapshotGCDelete bool
	snapshotGCSafety maintenance.SafetyParameters
}

func (c *commandSnapshotGC) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("gc", "DEPRECATED: This command does not do anything and will be removed. Snapshot GC is now automatically done as part of repository maintenance.").Hidden()
	cmd.Flag("delete", "Delete unreferenced contents").BoolVar(&c.snapshotGCDelete)
	safetyFlagVar(cmd, &c.snapshotGCSafety)
}
