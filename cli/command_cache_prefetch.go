package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

type commandCachePrefetch struct {
	objectIDs []string
	hint      string
}

func (c *commandCachePrefetch) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("prefetch", "Prefetches the provided objects into cache")
	cmd.Flag("hint", "Prefetch hint").StringVar(&c.hint)
	cmd.Action(svc.directRepositoryWriteAction(c.run))

	cmd.Arg("object", "Object ID to prefetch").Required().StringsVar(&c.objectIDs)
}

func (c *commandCachePrefetch) run(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	var oids []object.ID

	for _, s := range c.objectIDs {
		oid, err := snapshotfs.ParseObjectIDWithPath(ctx, rep, s)
		if err != nil {
			return errors.Wrapf(err, "unable to parse ID: %v", s)
		}

		oids = append(oids, oid)
	}

	cids, err := rep.PrefetchObjects(ctx, oids, c.hint)

	log(ctx).Infof("prefetched %v contents", len(cids))

	return errors.Wrap(err, "error prefetching")
}
