package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/iocopy"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
)

var (
	blobShowCommand = blobCommands.Command("show", "Show contents of BLOBs").Alias("cat")
	blobShowDecrypt = blobShowCommand.Flag("decrypt", "Decrypt blob if possible").Bool()
	blobShowIDs     = blobShowCommand.Arg("blobID", "Blob IDs").Required().Strings()
)

func runBlobShow(ctx context.Context, rep *repo.DirectRepository) error {
	for _, blobID := range *blobShowIDs {
		if err := maybeDecryptBlob(ctx, os.Stdout, rep, blob.ID(blobID)); err != nil {
			return errors.Wrap(err, "error presenting blob")
		}
	}

	return nil
}

func maybeDecryptBlob(ctx context.Context, w io.Writer, rep *repo.DirectRepository, blobID blob.ID) error {
	var (
		d   []byte
		err error
	)

	if *blobShowDecrypt && canDecryptBlob(blobID) {
		d, err = rep.Content.DecryptBlob(ctx, blobID)

		if isJSONBlob(blobID) && err == nil {
			var b bytes.Buffer

			if err = json.Indent(&b, d, "", "  "); err != nil {
				return errors.Wrap(err, "invalid JSON")
			}

			d = b.Bytes()
		}
	} else {
		d, err = rep.Blobs.GetBlob(ctx, blobID, 0, -1)
	}

	if err != nil {
		return errors.Wrapf(err, "error getting %v", blobID)
	}

	_, err = iocopy.Copy(w, bytes.NewReader(d))

	return err
}

func canDecryptBlob(b blob.ID) bool {
	switch b[0] {
	case 'n', 'm', 'l':
		return true
	default:
		return false
	}
}

func isJSONBlob(b blob.ID) bool {
	switch b[0] {
	case 'm', 'l':
		return true
	default:
		return false
	}
}

func init() {
	blobShowCommand.Action(directRepositoryAction(runBlobShow))
}
