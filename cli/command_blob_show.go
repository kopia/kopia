package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/iocopy"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
)

type commandBlobShow struct {
	blobShowDecrypt bool
	blobShowIDs     []string

	out textOutput
}

func (c *commandBlobShow) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("show", "Show contents of BLOBs").Alias("cat")
	cmd.Flag("decrypt", "Decrypt blob if possible").BoolVar(&c.blobShowDecrypt)
	cmd.Arg("blobID", "Blob IDs").Required().StringsVar(&c.blobShowIDs)
	cmd.Action(svc.directRepositoryReadAction(c.run))

	c.out.setup(svc)
}

func (c *commandBlobShow) run(ctx context.Context, rep repo.DirectRepository) error {
	for _, blobID := range c.blobShowIDs {
		if err := c.maybeDecryptBlob(ctx, c.out.stdout(), rep, blob.ID(blobID)); err != nil {
			return errors.Wrap(err, "error presenting blob")
		}
	}

	return nil
}

func (c *commandBlobShow) maybeDecryptBlob(ctx context.Context, w io.Writer, rep repo.DirectRepository, blobID blob.ID) error {
	var (
		d gather.WriteBuffer
		b gather.Bytes
	)

	if err := rep.BlobReader().GetBlob(ctx, blobID, 0, -1, &d); err != nil {
		return errors.Wrap(err, "error reading blob")
	}

	b = d.Bytes()

	if c.blobShowDecrypt && canDecryptBlob(blobID) {
		var tmp gather.WriteBuffer
		defer tmp.Close()

		if err := content.ConvertBlobFromRepository(rep.ContentReader().ContentFormat(), b, blobID, &tmp); err != nil {
			return errors.Wrap(err, "error converting back blob")
		}

		b = tmp.Bytes()
	}

	if isJSONBlob(blobID) {
		var buf bytes.Buffer

		if err := json.Indent(&buf, b.ToByteSlice(), "", "  "); err != nil {
			return errors.Wrap(err, "invalid JSON")
		}

		b = gather.FromSlice(buf.Bytes())
	}

	if err := iocopy.JustCopy(w, b.Reader()); err != nil {
		return errors.Wrap(err, "error copying data")
	}

	return nil
}

func canDecryptBlob(b blob.ID) bool {
	switch b[0] {
	case '_', 'n', 'm', 'l':
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
