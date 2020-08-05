package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/maintenance"
)

func maybeAutoUpgradeRepository(ctx context.Context, r repo.Repository) {
	if r == nil {
		return
	}

	mr, ok := r.(maintenance.MaintainableRepository)
	if !ok {
		return
	}

	has, err := maintenance.HasParams(ctx, mr)
	if err == nil && !has {
		_, _ = noticeColor.Printf("Setting default maintenance parameters...\n")

		if err := setDefaultMaintenanceParameters(ctx, mr); err != nil {
			log(ctx).Warningf("unable to set default maintenance parameters: %v", err)
		}
	}
}

func setDefaultMaintenanceParameters(ctx context.Context, rep maintenance.MaintainableRepository) error {
	p := maintenance.DefaultParams()
	p.Owner = rep.Username() + "@" + rep.Hostname()

	if err := maintenance.SetParams(ctx, rep, &p); err != nil {
		return errors.Wrap(err, "unable to set maintenance params")
	}

	_, _ = noticeColor.Printf(`
Kopia will perform quick maintenance of the repository automatically every %v
when running as %v. This operation never deletes any data.

Full maintenance (which also deletes unreferenced data) is disabled by default.

To run it manually use:

$ kopia maintenance run --full

Alternatively you can schedule full maintenance to run periodically using:

$ kopia maintenance set --enable-full=true --full-interval=4h
`, p.QuickCycle.Interval, p.Owner)

	return nil
}
