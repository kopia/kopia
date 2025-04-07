package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/maintenance"
)

func maybeAutoUpgradeRepository(ctx context.Context, r repo.Repository) error {
	// only upgrade repository when it's directly connected, not via API.
	dr, _ := r.(repo.DirectRepository)
	if dr == nil {
		return nil
	}

	has, err := maintenance.HasParams(ctx, r)
	if err != nil {
		return errors.Wrap(err, "error looking for maintenance parameters")
	}

	if has {
		return nil
	}

	log(ctx).Debug("Setting default maintenance parameters...")

	//nolint:wrapcheck
	return repo.DirectWriteSession(ctx, dr, repo.WriteSessionOptions{
		Purpose: "setDefaultMaintenanceParameters",
	}, func(ctx context.Context, w repo.DirectRepositoryWriter) error {
		return setDefaultMaintenanceParameters(ctx, w)
	})
}

func setDefaultMaintenanceParameters(ctx context.Context, rep repo.RepositoryWriter) error {
	p := maintenance.DefaultParams()
	p.Owner = rep.ClientOptions().UsernameAtHost()

	if dw, ok := rep.(repo.DirectRepositoryWriter); ok {
		_, ok, err := dw.ContentReader().EpochManager(ctx)
		if err != nil {
			return errors.Wrap(err, "epoch manager")
		}

		if ok {
			// disable quick maintenance cycle
			p.QuickCycle.Enabled = false
		}
	}

	if err := maintenance.SetParams(ctx, rep, &p); err != nil {
		return errors.Wrap(err, "unable to set maintenance params")
	}

	log(ctx).Infof(`
NOTE: Kopia will perform quick maintenance of the repository automatically every %v
and full maintenance every %v when running as %v.

See https://kopia.io/docs/advanced/maintenance/ for more information.
`, p.QuickCycle.Interval, p.FullCycle.Interval, p.Owner)

	return nil
}
