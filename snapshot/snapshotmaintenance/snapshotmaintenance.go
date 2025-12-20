// Package snapshotmaintenance provides helpers to run snapshot GC and low-level repository snapshotmaintenance.
package snapshotmaintenance

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/maintenance"
	"github.com/kopia/kopia/snapshot/snapshotgc"
)

// ErrReadonly indicates a failure when attempting to run maintenance on a read-only repository.
var ErrReadonly = errors.New("not running maintenance on read-only repository connection")

// Run runs the complete snapshot and repository maintenance.
func Run(ctx context.Context, dr repo.DirectRepositoryWriter, mode maintenance.Mode, force bool, safety maintenance.SafetyParameters) error {
	if dr.ClientOptions().ReadOnly {
		return ErrReadonly
	}

	dr.LogManager().Enable()

	//nolint:wrapcheck
	return maintenance.RunExclusive(ctx, dr, mode, force,
		func(ctx context.Context, runParams maintenance.RunParameters) error {
			// run snapshot GC before full maintenance or in emergency mode
			if runParams.Mode == maintenance.ModeFull || runParams.LowSpace {
				if err := snapshotgc.Run(ctx, dr, true, safety, runParams.MaintenanceStartTime); err != nil {
					return errors.Wrap(err, "snapshot GC failure")
				}
			}

			//nolint:wrapcheck
			return maintenance.Run(ctx, runParams, safety)
		})
}
