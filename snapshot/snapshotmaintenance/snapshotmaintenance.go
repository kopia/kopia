// Package snapshotmaintenance provides helpers to run snapshot GC and low-level repository snapshotmaintenance.
package snapshotmaintenance

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/maintenance"
	"github.com/kopia/kopia/snapshot/snapshotgc"
)

// Run runs the complete snapshot and repository maintenance.
func Run(ctx context.Context, rep repo.Repository, mode maintenance.Mode, force bool) error {
	dr, ok := rep.(*repo.DirectRepository)
	if !ok {
		return nil
	}

	return maintenance.RunExclusive(ctx, dr, mode, force,
		func(runParams maintenance.RunParameters) error {
			// run snapshot GC before full maintenance
			if runParams.Mode == maintenance.ModeFull {
				if _, err := snapshotgc.Run(ctx, dr, runParams.Params.SnapshotGC, true); err != nil {
					return errors.Wrap(err, "snapshot GC failure")
				}
			}

			return maintenance.Run(ctx, runParams)
		})
}
