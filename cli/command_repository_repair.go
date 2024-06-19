package cli

import (
	"context"

	"github.com/alecthomas/kingpin/v2"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/format"
)

type commandRepositoryRepair struct {
	repairCommandRecoverFormatBlob         string
	repairCommandRecoverFormatBlobPrefixes []string
	repairDryRun                           bool
}

func (c *commandRepositoryRepair) setup(svc advancedAppServices, parent commandParent) {
	cmd := parent.Command("repair", "Repairs repository.")

	cmd.Flag("recover-format", "Recover format blob from a copy").Default("auto").EnumVar(&c.repairCommandRecoverFormatBlob, "auto", "yes", "no")
	cmd.Flag("recover-format-block-prefixes", "Prefixes of file names").StringsVar(&c.repairCommandRecoverFormatBlobPrefixes)
	cmd.Flag("dry-run", "Do not modify repository").Short('n').BoolVar(&c.repairDryRun)

	for _, prov := range svc.storageProviders() {
		f := prov.NewFlags()
		cc := cmd.Command(prov.Name, "Repair repository in "+prov.Description)
		f.Setup(svc, cc)
		cc.Action(func(kpc *kingpin.ParseContext) error {
			return svc.runAppWithContext(kpc.SelectedCommand, func(ctx context.Context) error {
				st, err := f.Connect(ctx, false, 0)
				if err != nil {
					return errors.Wrap(err, "can't connect to storage")
				}

				return c.runRepairCommandWithStorage(ctx, st)
			})
		})
	}
}

func packBlockPrefixes() []string {
	var str []string

	for _, p := range content.PackBlobIDPrefixes {
		str = append(str, string(p))
	}

	return str
}

func (c *commandRepositoryRepair) runRepairCommandWithStorage(ctx context.Context, st blob.Storage) error {
	switch c.repairCommandRecoverFormatBlob {
	case "auto":
		log(ctx).Info("looking for format blob...")

		var tmp gather.WriteBuffer
		defer tmp.Close()

		if err := st.GetBlob(ctx, format.KopiaRepositoryBlobID, 0, -1, &tmp); err == nil {
			log(ctx).Info("format blob already exists, not recovering, pass --recover-format=yes")
			return nil
		}

	case "no":
		return nil
	}

	prefixes := c.repairCommandRecoverFormatBlobPrefixes
	if len(prefixes) == 0 {
		prefixes = packBlockPrefixes()
	}

	return c.recoverFormatBlob(ctx, st, prefixes)
}

func (c *commandRepositoryRepair) recoverFormatBlob(ctx context.Context, st blob.Storage, prefixes []string) error {
	errSuccess := errors.New("success")

	for _, prefix := range prefixes {
		err := st.ListBlobs(ctx, blob.ID(prefix), func(bi blob.Metadata) error {
			log(ctx).Infof("looking for replica of format blob in %v...", bi.BlobID)

			if b, err := format.RecoverFormatBlob(ctx, st, bi.BlobID, bi.Length); err == nil {
				if !c.repairDryRun {
					if puterr := st.PutBlob(ctx, format.KopiaRepositoryBlobID, gather.FromSlice(b), blob.PutOptions{}); puterr != nil {
						return errors.Wrap(puterr, "error writing format blob")
					}
				}

				log(ctx).Infof("recovered replica block from %v", bi.BlobID)

				return errSuccess
			}

			return nil
		})

		switch {
		case err == nil:
			// do nothing
		case errors.Is(err, errSuccess):
			return nil
		default:
			return errors.Wrap(err, "unexpected error when listing blobs")
		}
	}

	return errors.New("could not find a replica of a format blob")
}
