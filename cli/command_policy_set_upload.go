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
}

func (c *policyUploadFlags) setup(cmd *kingpin.CmdClause) {
	cmd.Flag("max-parallel-file-reads", "Maximum number of parallel file reads").StringVar(&c.maxParallelFileReads)
	cmd.Flag("max-parallel-snapshots", "Maximum number of parallel snapshots (server, KopiaUI only)").StringVar(&c.maxParallelUploads)
	cmd.Flag("parallel-upload-above-size-mib", "Use parallel uploads above size").StringVar(&c.parallelizeUploadAboveSizeMiB)
}

func (c *policyUploadFlags) setUploadPolicyFromFlags(ctx context.Context, up *policy.UploadPolicy, changeCount *int) error {
	if err := applyOptionalInt(ctx, "max parallel file reads", &up.MaxParallelFileReads, c.maxParallelFileReads, changeCount); err != nil {
		return err
	}

	if err := applyOptionalInt(ctx, "max parallel snapshots", &up.MaxParallelSnapshots, c.maxParallelUploads, changeCount); err != nil {
		return err
	}

	return applyOptionalInt64MiB(ctx, "parallel upload above size", &up.ParallelUploadAboveSize, c.parallelizeUploadAboveSizeMiB, changeCount)
}
