package cli

import (
	"context"

	"github.com/alecthomas/kingpin"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/kopia/kopia/repo/hashing"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/repo/splitter"
	"github.com/kopia/kopia/snapshot/policy"
)

const runValidationNote = `NOTE: To validate that your provider is compatible with Kopia, please run:

$ kopia repository validate-provider

`

type commandRepositoryCreate struct {
	createBlockHashFormat       string
	createBlockEncryptionFormat string
	createSplitter              string
	createOnly                  bool
	createFormatVersion         int

	co  connectOptions
	svc advancedAppServices
	out textOutput
}

func (c *commandRepositoryCreate) setup(svc advancedAppServices, parent commandParent) {
	cmd := parent.Command("create", "Create new repository in a specified location.")

	cmd.Flag("block-hash", "Content hash algorithm.").PlaceHolder("ALGO").Default(hashing.DefaultAlgorithm).EnumVar(&c.createBlockHashFormat, hashing.SupportedAlgorithms()...)
	cmd.Flag("encryption", "Content encryption algorithm.").PlaceHolder("ALGO").Default(encryption.DefaultAlgorithm).EnumVar(&c.createBlockEncryptionFormat, encryption.SupportedAlgorithms(false)...)
	cmd.Flag("object-splitter", "The splitter to use for new objects in the repository").Default(splitter.DefaultAlgorithm).EnumVar(&c.createSplitter, splitter.SupportedAlgorithms()...)
	cmd.Flag("create-only", "Create repository, but don't connect to it.").Short('c').BoolVar(&c.createOnly)
	cmd.Flag("format-version", "Force a particular repository format version (1 or 2, 0==default)").IntVar(&c.createFormatVersion)

	c.co.setup(cmd)
	c.svc = svc
	c.out.setup(svc)

	for _, prov := range storageProviders {
		if prov.name == "from-config" {
			continue
		}

		// Set up 'create' subcommand
		f := prov.newFlags()
		cc := cmd.Command(prov.name, "Create repository in "+prov.description)
		f.setup(svc, cc)
		cc.Action(func(_ *kingpin.ParseContext) error {
			ctx := svc.rootContext()
			st, err := f.connect(ctx, true)
			if err != nil {
				return errors.Wrap(err, "can't connect to storage")
			}

			return c.runCreateCommandWithStorage(ctx, st)
		})
	}
}

func (c *commandRepositoryCreate) newRepositoryOptionsFromFlags() *repo.NewRepositoryOptions {
	return &repo.NewRepositoryOptions{
		BlockFormat: content.FormattingOptions{
			MutableParameters: content.MutableParameters{
				Version: content.FormatVersion(c.createFormatVersion),
			},
			Hash:       c.createBlockHashFormat,
			Encryption: c.createBlockEncryptionFormat,
		},

		ObjectFormat: object.Format{
			Splitter: c.createSplitter,
		},
	}
}

func (c *commandRepositoryCreate) ensureEmpty(ctx context.Context, s blob.Storage) error {
	hasDataError := errors.Errorf("has data")

	err := s.ListBlobs(ctx, "", func(cb blob.Metadata) error {
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

	log(ctx).Infof("Initializing repository with:")

	if options.BlockFormat.Version != 0 {
		log(ctx).Infof("  format version:      %v", options.BlockFormat.Version)
	}

	log(ctx).Infof("  block hash:          %v", options.BlockFormat.Hash)
	log(ctx).Infof("  encryption:          %v", options.BlockFormat.Encryption)
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

	noteColor.Fprintf(c.out.stdout(), runValidationNote) // nolint:errcheck

	return nil
}

func (c *commandRepositoryCreate) populateRepository(ctx context.Context, password string) error {
	rep, err := repo.Open(ctx, c.svc.repositoryConfigFileName(), password, c.svc.optionsFromFlags(ctx))
	if err != nil {
		return errors.Wrap(err, "unable to open repository")
	}
	defer rep.Close(ctx) //nolint:errcheck

	// nolint:wrapcheck
	return repo.WriteSession(ctx, rep, repo.WriteSessionOptions{
		Purpose: "populate repository",
	}, func(ctx context.Context, w repo.RepositoryWriter) error {
		if err := policy.SetPolicy(ctx, w, policy.GlobalPolicySourceInfo, policy.DefaultPolicy); err != nil {
			return errors.Wrap(err, "unable to set global policy")
		}

		printRetentionPolicy(&c.out, policy.DefaultPolicy, nil)
		printCompressionPolicy(&c.out, policy.DefaultPolicy, nil)

		c.out.printStderr("\nTo find more information about default policy run 'kopia policy get'.\nTo change the policy use 'kopia policy set' command.\n")

		if err := setDefaultMaintenanceParameters(ctx, w); err != nil {
			return errors.Wrap(err, "unable to set maintenance parameters")
		}

		return nil
	})
}
