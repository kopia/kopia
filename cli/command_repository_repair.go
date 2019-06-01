package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/storage"
)

var (
	repairCommand = repositoryCommands.Command("repair", "Repairs respository.")

	repairCommandRecoverFormatBlock       = repairCommand.Flag("recover-format", "Recover format block from a copy").Default("auto").Enum("auto", "yes", "no")
	repairCommandRecoverFormatBlockPrefix = repairCommand.Flag("recover-format-block-prefix", "Prefix of file names").Default("p").String()
	repairDryDrun                         = repairCommand.Flag("dry-run", "Do not modify repository").Short('n').Bool()
)

func runRepairCommandWithStorage(ctx context.Context, st storage.Storage) error {
	if err := maybeRecoverFormatBlock(ctx, st, *repairCommandRecoverFormatBlockPrefix); err != nil {
		return err
	}
	return nil
}

func maybeRecoverFormatBlock(ctx context.Context, st storage.Storage, prefix string) error {
	switch *repairCommandRecoverFormatBlock {
	case "auto":
		log.Infof("looking for format block...")
		if _, err := st.GetBlock(ctx, repo.FormatBlockID, 0, -1); err == nil {
			log.Infof("format block already exists, not recovering, pass --recover-format=yes")
			return nil
		}

	case "no":
		return nil
	}

	return recoverFormatBlock(ctx, st, *repairCommandRecoverFormatBlockPrefix)
}

func recoverFormatBlock(ctx context.Context, st storage.Storage, prefix string) error {
	errSuccess := errors.New("success")

	err := st.ListBlocks(ctx, *repairCommandRecoverFormatBlockPrefix, func(bi storage.BlockMetadata) error {
		log.Infof("looking for replica of format block in %v...", bi.BlockID)
		if b, err := repo.RecoverFormatBlock(ctx, st, bi.BlockID, bi.Length); err == nil {
			if !*repairDryDrun {
				if puterr := st.PutBlock(ctx, repo.FormatBlockID, b); puterr != nil {
					return puterr
				}
			}

			log.Infof("recovered replica block from %v", bi.BlockID)
			return errSuccess
		}

		return nil
	})

	switch err {
	case errSuccess:
		return nil
	case nil:
		return errors.New("could not find a replica of a format block")
	default:
		return err
	}
}
