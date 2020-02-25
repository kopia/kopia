package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
)

var (
	repairCommand = repositoryCommands.Command("repair", "Repairs repository.")

	repairCommandRecoverFormatBlob         = repairCommand.Flag("recover-format", "Recover format blob from a copy").Default("auto").Enum("auto", "yes", "no")
	repairCommandRecoverFormatBlobPrefixes = repairCommand.Flag("recover-format-block-prefixes", "Prefixes of file names").Strings()
	repairDryDrun                          = repairCommand.Flag("dry-run", "Do not modify repository").Short('n').Bool()
)

func packBlockPrefixes() []string {
	var str []string

	for _, p := range content.PackBlobIDPrefixes {
		str = append(str, string(p))
	}

	return str
}

func runRepairCommandWithStorage(ctx context.Context, st blob.Storage) error {
	if err := maybeRecoverFormatBlob(ctx, st); err != nil {
		return err
	}

	return nil
}

func maybeRecoverFormatBlob(ctx context.Context, st blob.Storage) error {
	switch *repairCommandRecoverFormatBlob {
	case "auto":
		log(ctx).Infof("looking for format blob...")

		if _, err := st.GetBlob(ctx, repo.FormatBlobID, 0, -1); err == nil {
			log(ctx).Infof("format blob already exists, not recovering, pass --recover-format=yes")
			return nil
		}

	case "no":
		return nil
	}

	prefixes := *repairCommandRecoverFormatBlobPrefixes
	if len(prefixes) == 0 {
		prefixes = packBlockPrefixes()
	}

	return recoverFormatBlob(ctx, st, prefixes)
}

func recoverFormatBlob(ctx context.Context, st blob.Storage, prefixes []string) error {
	errSuccess := errors.New("success")

	for _, prefix := range prefixes {
		err := st.ListBlobs(ctx, blob.ID(prefix), func(bi blob.Metadata) error {
			log(ctx).Infof("looking for replica of format blob in %v...", bi.BlobID)
			if b, err := repo.RecoverFormatBlob(ctx, st, bi.BlobID, bi.Length); err == nil {
				if !*repairDryDrun {
					if puterr := st.PutBlob(ctx, repo.FormatBlobID, b); puterr != nil {
						return puterr
					}
				}

				log(ctx).Infof("recovered replica block from %v", bi.BlobID)
				return errSuccess
			}

			return nil
		})

		switch err {
		case errSuccess:
			return nil
		case nil:
			// do nothing
		default:
			return err
		}
	}

	return errors.New("could not find a replica of a format blob")
}
