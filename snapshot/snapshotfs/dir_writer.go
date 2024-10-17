package snapshotfs

import (
	"context"
	"encoding/json"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/compression"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
)

func writeDirManifest(ctx context.Context, rep repo.RepositoryWriter, dirRelativePath string, dirManifest *snapshot.DirManifest, metadataComp compression.Name) (object.ID, error) {
	writer := rep.NewObjectWriter(ctx, object.WriterOptions{
		Description:        "DIR:" + dirRelativePath,
		Prefix:             objectIDPrefixDirectory,
		Compressor:         metadataComp,
		MetadataCompressor: metadataComp,
	})

	defer writer.Close() //nolint:errcheck

	if err := json.NewEncoder(writer).Encode(dirManifest); err != nil {
		return object.EmptyID, errors.Wrap(err, "unable to encode directory JSON")
	}

	oid, err := writer.Result()
	if err != nil {
		return object.EmptyID, errors.Wrap(err, "unable to write directory")
	}

	return oid, nil
}
