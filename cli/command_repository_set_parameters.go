package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
)

type commandRepositorySetParameters struct {
	maxPackSizeMB      int
	indexFormatVersion int

	svc appServices
}

func (c *commandRepositorySetParameters) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("set-parameters", "Set repository parameters.")

	cmd.Flag("max-pack-size-mb", "Set max pack file size").PlaceHolder("MB").IntVar(&c.maxPackSizeMB)
	cmd.Flag("index-version", "Set version of index format used for writing").IntVar(&c.indexFormatVersion)
	cmd.Action(svc.directRepositoryWriteAction(c.run))

	c.svc = svc
}

func (c *commandRepositorySetParameters) run(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	var anyChange bool

	mp := rep.ContentReader().ContentFormat().MutableParameters

	if c.maxPackSizeMB != 0 {
		mp.MaxPackSize = c.maxPackSizeMB << 20 // nolint:gomnd
		anyChange = true

		log(ctx).Infof(" - setting maximum pack size to %v.\n", units.BytesStringBase2(int64(mp.MaxPackSize)))
	}

	if c.indexFormatVersion != 0 {
		mp.IndexVersion = c.indexFormatVersion
		anyChange = true

		log(ctx).Infof(" - setting index format version to %v.\n", c.indexFormatVersion)
	}

	if !anyChange {
		return errors.Errorf("no changes")
	}

	if err := rep.SetParameters(ctx, mp); err != nil {
		return errors.Wrap(err, "error setting parameters")
	}

	log(ctx).Infof("NOTE: Repository parameters updated, you must disconnect and re-connect all other Kopia clients.")

	return nil
}
