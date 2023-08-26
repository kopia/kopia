package cli

import (
	"context"
	"sort"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

type commandSnapshotExpire struct {
	snapshotExpireAll    bool
	snapshotExpirePaths  []string
	snapshotExpireDelete bool
}

func (c *commandSnapshotExpire) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("expire", "Remove old snapshots according to defined expiration policies.")

	cmd.Flag("all", "Expire all snapshots").BoolVar(&c.snapshotExpireAll)
	cmd.Arg("path", "Expire snapshots for given paths only").StringsVar(&c.snapshotExpirePaths)
	cmd.Flag("delete", "Whether to actually delete snapshots").BoolVar(&c.snapshotExpireDelete)
	cmd.Action(svc.repositoryWriterAction(c.run))
}

func (c *commandSnapshotExpire) getSnapshotSourcesToExpire(ctx context.Context, rep repo.Repository) ([]snapshot.SourceInfo, error) {
	if c.snapshotExpireAll {
		//nolint:wrapcheck
		return snapshot.ListSources(ctx, rep)
	}

	var result []snapshot.SourceInfo

	for _, p := range c.snapshotExpirePaths {
		src, err := snapshot.ParseSourceInfo(p, rep.ClientOptions().Hostname, rep.ClientOptions().Username)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to parse %q", p)
		}

		result = append(result, src)
	}

	return result, nil
}

func (c *commandSnapshotExpire) run(ctx context.Context, rep repo.RepositoryWriter) error {
	sources, err := c.getSnapshotSourcesToExpire(ctx, rep)
	if err != nil {
		return err
	}

	sort.Slice(sources, func(i, j int) bool {
		return sources[i].String() < sources[j].String()
	})

	for _, src := range sources {
		deleted, err := policy.ApplyRetentionPolicy(ctx, rep, src, c.snapshotExpireDelete)
		if err != nil {
			return errors.Wrapf(err, "error applying retention policy to %v", src)
		}

		if len(deleted) == 0 {
			log(ctx).Infof("Nothing to delete for %v.", src)
			continue
		}

		if c.snapshotExpireDelete {
			log(ctx).Infof("Deleted %v snapshots of %v...", len(deleted), src)
		} else {
			log(ctx).Infof("%v snapshot(s) of %v would be deleted. Pass --delete to do it.", len(deleted), src)
		}
	}

	return nil
}
