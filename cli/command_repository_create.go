package cli

import (
	"context"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/ecc"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/kopia/kopia/repo/format"
	"github.com/kopia/kopia/repo/hashing"
	"github.com/kopia/kopia/repo/splitter"
	"github.com/kopia/kopia/snapshot/policy"
)

const runValidationNote = `NOTE: To validate that your provider is compatible with Kopia, please run:

$ kopia repository validate-provider

`

type commandRepositoryCreate struct {
	createBlockHashFormat             string
	createBlockEncryptionFormat       string
	createBlockECCFormat              string
	createBlockECCOverheadPercent     int
	createBlockKeyDerivationAlgorithm string
	createSplitter                    string
	createOnly                        bool
	createFormatVersion               int
	retentionMode                     string
	retentionPeriod                   time.Duration

	co  connectOptions
	svc advancedAppServices
	out textOutput
}

func (c *commandRepositoryCreate) setup(svc advancedAppServices, parent commandParent) {
	cmd := parent.Command("create", "Create new repository in a specified location.")

	cmd.Flag("block-hash", "Content hash algorithm.").PlaceHolder("ALGO").Default(hashing.DefaultAlgorithm).EnumVar(&c.createBlockHashFormat, hashing.SupportedAlgorithms()...)
	cmd.Flag("encryption", "Content encryption algorithm.").PlaceHolder("ALGO").Default(encryption.DefaultAlgorithm).EnumVar(&c.createBlockEncryptionFormat, encryption.SupportedAlgorithms(false)...)
	cmd.Flag("ecc", "[EXPERIMENTAL] Error correction algorithm.").PlaceHolder("ALGO").Default(ecc.DefaultAlgorithm).EnumVar(&c.createBlockECCFormat, ecc.SupportedAlgorithms()...)
	cmd.Flag("ecc-overhead-percent", "[EXPERIMENTAL] How much space overhead can be used for error correction, in percentage. Use 0 to disable ECC.").Default("0").IntVar(&c.createBlockECCOverheadPercent)
	cmd.Flag("object-splitter", "The splitter to use for new objects in the repository").Default(splitter.DefaultAlgorithm).EnumVar(&c.createSplitter, splitter.SupportedAlgorithms()...)
	cmd.Flag("create-only", "Create repository, but don't connect to it.").Short('c').BoolVar(&c.createOnly)
	cmd.Flag("format-version", "Force a particular repository format version (1, 2 or 3, 0==default)").IntVar(&c.createFormatVersion)
	cmd.Flag("retention-mode", "Set the blob retention-mode for supported storage backends.").EnumVar(&c.retentionMode, blob.Governance.String(), blob.Compliance.String())
	cmd.Flag("retention-period", "Set the blob retention-period for supported storage backends.").DurationVar(&c.retentionPeriod)
	//nolint:lll
	cmd.Flag("format-block-key-derivation-algorithm", "Algorithm to derive the encryption key for the format block from the repository password").Default(format.DefaultKeyDerivationAlgorithm).EnumVar(&c.createBlockKeyDerivationAlgorithm, format.SupportedFormatBlobKeyDerivationAlgorithms()...)

	c.co.setup(svc, cmd)
	c.svc = svc
	c.out.setup(svc)

	for _, prov := range svc.storageProviders() {
		// Set up 'create' subcommand
		f := prov.NewFlags()
		cc := cmd.Command(prov.Name, "Create repository in "+prov.Description)
		f.Setup(svc, cc)
		cc.Action(func(kpc *kingpin.ParseContext) error {
			return svc.runAppWithContext(kpc.SelectedCommand, func(ctx context.Context) error {
				st, err := f.Connect(ctx, true, c.createFormatVersion)
				if err != nil {
					return errors.Wrap(err, "can't connect to storage")
				}

				return c.runCreateCommandWithStorage(ctx, st)
			})
		})
	}
}

func (c *commandRepositoryCreate) newRepositoryOptionsFromFlags() *repo.NewRepositoryOptions {
	return &repo.NewRepositoryOptions{
		BlockFormat: format.ContentFormat{
			MutableParameters: format.MutableParameters{
				Version: format.Version(c.createFormatVersion),
			},
			Hash:               c.createBlockHashFormat,
			Encryption:         c.createBlockEncryptionFormat,
			ECC:                c.createBlockECCFormat,
			ECCOverheadPercent: c.createBlockECCOverheadPercent,
		},

		ObjectFormat: format.ObjectFormat{
			Splitter: c.createSplitter,
		},

		RetentionMode:                     blob.RetentionMode(c.retentionMode),
		RetentionPeriod:                   c.retentionPeriod,
		FormatBlockKeyDerivationAlgorithm: c.createBlockKeyDerivationAlgorithm,
	}
}

func (c *commandRepositoryCreate) ensureEmpty(ctx context.Context, s blob.Storage) error {
	hasDataError := errors.New("has data")

	err := s.ListBlobs(ctx, "", func(_ blob.Metadata) error {
		return hasDataError
	})

	if errors.Is(err, hasDataError) {
		return errors.New("found existing data in storage location")
	}

	return errors.Wrap(err, "error listing blobs")
}

func (c *commandRepositoryCreate) runCreateCommandWithStorage(ctx context.Context, st blob.Storage) error {
	err := c.ensureEmpty(ctx, st)
	if err != nil {
		return errors.Wrap(err, "unable to get repository storage")
	}

	options := c.newRepositoryOptionsFromFlags()

	pass, err := c.svc.getPasswordFromFlags(ctx, true, false)
	if err != nil {
		return errors.Wrap(err, "getting password")
	}

	log(ctx).Info("Initializing repository with:")

	if options.BlockFormat.Version != 0 {
		log(ctx).Infof("  format version:      %v", options.BlockFormat.Version)
	}

	log(ctx).Infof("  block hash:          %v", options.BlockFormat.Hash)
	log(ctx).Infof("  encryption:          %v", options.BlockFormat.Encryption)
	log(ctx).Infof("  key derivation:      %v", options.FormatBlockKeyDerivationAlgorithm)

	if options.BlockFormat.ECC != "" && options.BlockFormat.ECCOverheadPercent > 0 {
		log(ctx).Infof("  ecc:                 %v with %v%% overhead", options.BlockFormat.ECC, options.BlockFormat.ECCOverheadPercent)
	}

	log(ctx).Infof("  splitter:            %v", options.ObjectFormat.Splitter)

	if err := repo.Initialize(ctx, st, options, pass); err != nil {
		return errors.Wrap(err, "cannot initialize repository")
	}

	if c.createOnly {
		return nil
	}

	if err := c.svc.runConnectCommandWithStorageAndPassword(ctx, &c.co, st, pass); err != nil {
		return errors.Wrap(err, "unable to connect to repository")
	}

	if err := c.populateRepository(ctx, pass); err != nil {
		return errors.Wrap(err, "error populating repository")
	}

	noteColor.Fprintf(c.out.stdout(), runValidationNote) //nolint:errcheck

	return nil
}

func (c *commandRepositoryCreate) populateRepository(ctx context.Context, password string) error {
	rep, err := repo.Open(ctx, c.svc.repositoryConfigFileName(), password, c.svc.optionsFromFlags(ctx))
	if err != nil {
		return errors.Wrap(err, "unable to open repository")
	}
	defer rep.Close(ctx) //nolint:errcheck

	//nolint:wrapcheck
	return repo.WriteSession(ctx, rep, repo.WriteSessionOptions{
		Purpose: "populate repository",
	}, func(ctx context.Context, w repo.RepositoryWriter) error {
		if err := policy.SetPolicy(ctx, w, policy.GlobalPolicySourceInfo, policy.DefaultPolicy); err != nil {
			return errors.Wrap(err, "unable to set global policy")
		}

		var rows []policyTableRow

		rows = appendRetentionPolicyRows(rows, policy.DefaultPolicy, &policy.Definition{})
		rows = appendCompressionPolicyRows(rows, policy.DefaultPolicy, &policy.Definition{})

		c.out.printStdout("%v\n", alignedPolicyTableRows(rows))

		c.out.printStderr("\nTo find more information about default policy run 'kopia policy get'.\nTo change the policy use 'kopia policy set' command.\n")

		if err := setDefaultMaintenanceParameters(ctx, w); err != nil {
			return errors.Wrap(err, "unable to set maintenance parameters")
		}

		return nil
	})
}
