package cli

import (
	"context"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/maintenance"
)

var (
	contentRewriteCommand     = contentCommands.Command("rewrite", "Rewrite content using most recent format")
	contentRewriteIDs         = contentRewriteCommand.Arg("contentID", "Identifiers of contents to rewrite").Strings()
	contentRewriteParallelism = contentRewriteCommand.Flag("parallelism", "Number of parallel workers").Default("16").Int()

	contentRewriteShortPacks    = contentRewriteCommand.Flag("short", "Rewrite contents from short packs").Bool()
	contentRewriteFormatVersion = contentRewriteCommand.Flag("format-version", "Rewrite contents using the provided format version").Default("-1").Int()
	contentRewritePackPrefix    = contentRewriteCommand.Flag("pack-prefix", "Only rewrite contents from pack blobs with a given prefix").String()
	contentRewriteDryRun        = contentRewriteCommand.Flag("dry-run", "Do not actually rewrite, only print what would happen").Short('n').Bool()
	contentRewriteMinAge        = contentRewriteCommand.Flag("min-age", "Only rewrite contents above given age").Default("1h").Duration()
)

func runContentRewriteCommand(ctx context.Context, rep *repo.DirectRepository) error {
	return maintenance.RewriteContents(ctx, rep, &maintenance.RewriteContentsOptions{
		ContentIDRange: contentIDRange(),
		ContentIDs:     toContentIDs(*contentRewriteIDs),
		FormatVersion:  *contentRewriteFormatVersion,
		MinAge:         *contentRewriteMinAge,
		PackPrefix:     blob.ID(*contentRewritePackPrefix),
		Parallel:       *contentRewriteParallelism,
		ShortPacks:     *contentRewriteShortPacks,
		DryRun:         *contentRewriteDryRun,
	})
}

func toContentIDs(s []string) []content.ID {
	var result []content.ID
	for _, cid := range s {
		result = append(result, content.ID(cid))
	}

	return result
}

func init() {
	contentRewriteCommand.Action(directRepositoryAction(runContentRewriteCommand))
	setupContentIDRangeFlags(contentRewriteCommand)
}
