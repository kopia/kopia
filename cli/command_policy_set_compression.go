package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/compression"
	"github.com/kopia/kopia/snapshot/policy"
)

var (

	// Name of compression algorithm.
	policySetCompressionAlgorithm = policySetCommand.Flag("compression", "Compression algorithm").Enum(supportedCompressionAlgorithms()...)
	policySetCompressionMinSize   = policySetCommand.Flag("compression-min-size", "Min size of file to attempt compression for").String()
	policySetCompressionMaxSize   = policySetCommand.Flag("compression-max-size", "Max size of file to attempt compression for").String()

	// Files to only compress.
	policySetAddOnlyCompress    = policySetCommand.Flag("add-only-compress", "List of extensions to add to the only-compress list").PlaceHolder("PATTERN").Strings()
	policySetRemoveOnlyCompress = policySetCommand.Flag("remove-only-compress", "List of extensions to remove from the only-compress list").PlaceHolder("PATTERN").Strings()
	policySetClearOnlyCompress  = policySetCommand.Flag("clear-only-compress", "Clear list of extensions in the only-compress list").Bool()

	// Files to never compress.
	policySetAddNeverCompress    = policySetCommand.Flag("add-never-compress", "List of extensions to add to the never compress list").PlaceHolder("PATTERN").Strings()
	policySetRemoveNeverCompress = policySetCommand.Flag("remove-never-compress", "List of extensions to remove from the never compress list").PlaceHolder("PATTERN").Strings()
	policySetClearNeverCompress  = policySetCommand.Flag("clear-never-compress", "Clear list of extensions in the never compress list").Bool()
)

func setCompressionPolicyFromFlags(ctx context.Context, p *policy.CompressionPolicy, changeCount *int) error {
	if err := applyPolicyNumber64(ctx, "minimum file size subject to compression", &p.MinSize, *policySetCompressionMinSize, changeCount); err != nil {
		return errors.Wrap(err, "minimum file size subject to compression")
	}

	if err := applyPolicyNumber64(ctx, "maximum file size subject to compression", &p.MaxSize, *policySetCompressionMaxSize, changeCount); err != nil {
		return errors.Wrap(err, "maximum file size subject to compression")
	}

	if v := *policySetCompressionAlgorithm; v != "" {
		*changeCount++

		if v == inheritPolicyString {
			log(ctx).Infof(" - resetting compression algorithm to default value inherited from parent\n")

			p.CompressorName = ""
		} else {
			log(ctx).Infof(" - setting compression algorithm to %v\n", v)

			p.CompressorName = compression.Name(v)
		}
	}

	applyPolicyStringList(ctx, "only-compress extensions",
		&p.OnlyCompress, *policySetAddOnlyCompress, *policySetRemoveOnlyCompress, *policySetClearOnlyCompress, changeCount)

	applyPolicyStringList(ctx, "never-compress extensions",
		&p.NeverCompress, *policySetAddNeverCompress, *policySetRemoveNeverCompress, *policySetClearNeverCompress, changeCount)

	return nil
}
