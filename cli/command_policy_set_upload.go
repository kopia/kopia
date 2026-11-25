package cli

import (
	"context"

	"github.com/alecthomas/kingpin/v2"

	"github.com/kopia/kopia/snapshot/policy"
)

type policyUploadFlags struct {
	maxParallelUploads            string
	maxParallelFileReads          string
	parallelizeUploadAboveSizeMiB string
	reuseHardlinkContent          string
}

func (c *policyUploadFlags) setup(cmd *kingpin.CmdClause) {
	cmd.Flag("max-parallel-file-reads", "Maximum number of parallel file reads").StringVar(&c.maxParallelFileReads)
	cmd.Flag("max-parallel-snapshots", "Maximum number of parallel snapshots (server, KopiaUI only)").StringVar(&c.maxParallelUploads)
	cmd.Flag("parallel-upload-above-size-mib", "Use parallel uploads above size").StringVar(&c.parallelizeUploadAboveSizeMiB)
	cmd.Flag("reuse-hardlink-content",
		"Skip reading files that are hardlinks of a file already uploaded in the same snapshot; only safe when the source does not change during the snapshot ('true', 'false', 'inherit')").
		EnumVar(&c.reuseHardlinkContent, booleanEnumValues...)
}

func (c *policyUploadFlags) setUploadPolicyFromFlags(ctx context.Context, up *policy.UploadPolicy, changeCount *int) error {
	if err := applyOptionalInt(ctx, "max parallel file reads", &up.MaxParallelFileReads, c.maxParallelFileReads, changeCount); err != nil {
		return err
	}

	if err := applyOptionalInt(ctx, "max parallel snapshots", &up.MaxParallelSnapshots, c.maxParallelUploads, changeCount); err != nil {
		return err
	}

	if err := applyOptionalInt64MiB(ctx, "parallel upload above size", &up.ParallelUploadAboveSize, c.parallelizeUploadAboveSizeMiB, changeCount); err != nil {
		return err
	}

	return applyPolicyBoolPtr(ctx, "reuse hardlink content", &up.ReuseHardlinkContent, c.reuseHardlinkContent, changeCount)
}
