package cli

import (
	"context"

	"github.com/alecthomas/kingpin/v2"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/compression"
	"github.com/kopia/kopia/snapshot/policy"
)

type policyCompressionFlags struct {
	policySetCompressionAlgorithm string
	policySetCompressionMinSize   string
	policySetCompressionMaxSize   string

	policySetAddOnlyCompress    []string
	policySetRemoveOnlyCompress []string
	policySetClearOnlyCompress  bool

	policySetAddNeverCompress    []string
	policySetRemoveNeverCompress []string
	policySetClearNeverCompress  bool
}

type policyMetadataCompressionFlags struct {
	policySetMetadataCompressionAlgorithm string
}

func (c *policyMetadataCompressionFlags) setup(cmd *kingpin.CmdClause) {
	// Name of compression algorithm.
	cmd.Flag("metadata-compression", "Metadata Compression algorithm").EnumVar(&c.policySetMetadataCompressionAlgorithm, supportedCompressionAlgorithms()...)
}

func (c *policyMetadataCompressionFlags) setMetadataCompressionPolicyFromFlags(
	ctx context.Context,
	p *policy.MetadataCompressionPolicy,
	changeCount *int,
) error { //nolint:unparam
	if v := c.policySetMetadataCompressionAlgorithm; v != "" {
		*changeCount++

		if v == inheritPolicyString {
			log(ctx).Info(" - resetting metadata compression algorithm to default value inherited from parent")

			p.CompressorName = ""
		} else {
			log(ctx).Infof(" - setting metadata compression algorithm to %v", v)

			p.CompressorName = compression.Name(v)
		}
	}

	return nil
}

func (c *policyCompressionFlags) setup(cmd *kingpin.CmdClause) {
	// Name of compression algorithm.
	cmd.Flag("compression", "Compression algorithm").EnumVar(&c.policySetCompressionAlgorithm, supportedCompressionAlgorithms()...)
	cmd.Flag("compression-min-size", "Min size of file to attempt compression for").StringVar(&c.policySetCompressionMinSize)
	cmd.Flag("compression-max-size", "Max size of file to attempt compression for").StringVar(&c.policySetCompressionMaxSize)

	// Files to only compress.
	cmd.Flag("add-only-compress", "List of extensions to add to the only-compress list").PlaceHolder("PATTERN").StringsVar(&c.policySetAddOnlyCompress)
	cmd.Flag("remove-only-compress", "List of extensions to remove from the only-compress list").PlaceHolder("PATTERN").StringsVar(&c.policySetRemoveOnlyCompress)
	cmd.Flag("clear-only-compress", "Clear list of extensions in the only-compress list").BoolVar(&c.policySetClearOnlyCompress)

	// Files to never compress.
	cmd.Flag("add-never-compress", "List of extensions to add to the never compress list").PlaceHolder("PATTERN").StringsVar(&c.policySetAddNeverCompress)
	cmd.Flag("remove-never-compress", "List of extensions to remove from the never compress list").PlaceHolder("PATTERN").StringsVar(&c.policySetRemoveNeverCompress)
	cmd.Flag("clear-never-compress", "Clear list of extensions in the never compress list").BoolVar(&c.policySetClearNeverCompress)
}

func (c *policyCompressionFlags) setCompressionPolicyFromFlags(ctx context.Context, p *policy.CompressionPolicy, changeCount *int) error {
	if err := applyPolicyNumber64(ctx, "minimum file size subject to compression", &p.MinSize, c.policySetCompressionMinSize, changeCount); err != nil {
		return errors.Wrap(err, "minimum file size subject to compression")
	}

	if err := applyPolicyNumber64(ctx, "maximum file size subject to compression", &p.MaxSize, c.policySetCompressionMaxSize, changeCount); err != nil {
		return errors.Wrap(err, "maximum file size subject to compression")
	}

	if v := c.policySetCompressionAlgorithm; v != "" {
		*changeCount++

		if v == inheritPolicyString {
			log(ctx).Info(" - resetting compression algorithm to default value inherited from parent")

			p.CompressorName = ""
		} else {
			log(ctx).Infof(" - setting compression algorithm to %v", v)

			p.CompressorName = compression.Name(v)
		}
	}

	applyPolicyStringList(ctx, "only-compress extensions",
		&p.OnlyCompress, c.policySetAddOnlyCompress, c.policySetRemoveOnlyCompress, c.policySetClearOnlyCompress, changeCount)

	applyPolicyStringList(ctx, "never-compress extensions",
		&p.NeverCompress, c.policySetAddNeverCompress, c.policySetRemoveNeverCompress, c.policySetClearNeverCompress, changeCount)

	return nil
}
