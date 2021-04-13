package cli

import (
	"context"

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

var (
	createCommand = repositoryCommands.Command("create", "Create new repository in a specified location.")

	createBlockHashFormat       = createCommand.Flag("block-hash", "Content hash algorithm.").PlaceHolder("ALGO").Default(hashing.DefaultAlgorithm).Enum(hashing.SupportedAlgorithms()...)
	createBlockEncryptionFormat = createCommand.Flag("encryption", "Content encryption algorithm.").PlaceHolder("ALGO").Default(encryption.DefaultAlgorithm).Enum(encryption.SupportedAlgorithms(false)...)
	createSplitter              = createCommand.Flag("object-splitter", "The splitter to use for new objects in the repository").Default(splitter.DefaultAlgorithm).Enum(splitter.SupportedAlgorithms()...)

	createOnly = createCommand.Flag("create-only", "Create repository, but don't connect to it.").Short('c').Bool()
)

func init() {
	setupConnectOptions(createCommand)
}

func newRepositoryOptionsFromFlags() *repo.NewRepositoryOptions {
	return &repo.NewRepositoryOptions{
		BlockFormat: content.FormattingOptions{
			Hash:       *createBlockHashFormat,
			Encryption: *createBlockEncryptionFormat,
		},

		ObjectFormat: object.Format{
			Splitter: *createSplitter,
		},
	}
}

func ensureEmpty(ctx context.Context, s blob.Storage) error {
	hasDataError := errors.Errorf("has data")

	err := s.ListBlobs(ctx, "", func(cb blob.Metadata) error {
		// nolint:wrapcheck
		return hasDataError
	})

	if errors.Is(err, hasDataError) {
		return errors.New("found existing data in storage location")
	}

	return errors.Wrap(err, "error listing blobs")
}

func runCreateCommandWithStorage(ctx context.Context, st blob.Storage) error {
	err := ensureEmpty(ctx, st)
	if err != nil {
		return errors.Wrap(err, "unable to get repository storage")
	}

	options := newRepositoryOptionsFromFlags()

	password, err := getPasswordFromFlags(ctx, true, false)
	if err != nil {
		return errors.Wrap(err, "getting password")
	}

	log(ctx).Infof("Initializing repository with:")
	log(ctx).Infof("  block hash:          %v", options.BlockFormat.Hash)
	log(ctx).Infof("  encryption:          %v", options.BlockFormat.Encryption)
	log(ctx).Infof("  splitter:            %v", options.ObjectFormat.Splitter)

	if err := repo.Initialize(ctx, st, options, password); err != nil {
		return errors.Wrap(err, "cannot initialize repository")
	}

	if *createOnly {
		return nil
	}

	if err := runConnectCommandWithStorageAndPassword(ctx, st, password); err != nil {
		return errors.Wrap(err, "unable to connect to repository")
	}

	return populateRepository(ctx, password)
}

func populateRepository(ctx context.Context, password string) error {
	rep, err := repo.Open(ctx, repositoryConfigFileName(), password, optionsFromFlags(ctx))
	if err != nil {
		return errors.Wrap(err, "unable to open repository")
	}
	defer rep.Close(ctx) //nolint:errcheck

	return repo.WriteSession(ctx, rep, repo.WriteSessionOptions{
		Purpose: "populateRepository",
	}, func(w repo.RepositoryWriter) error {
		if err := policy.SetPolicy(ctx, w, policy.GlobalPolicySourceInfo, policy.DefaultPolicy); err != nil {
			return errors.Wrap(err, "unable to set global policy")
		}

		printRetentionPolicy(policy.DefaultPolicy, nil)
		printCompressionPolicy(policy.DefaultPolicy, nil)

		printStderr("\nTo find more information about default policy run 'kopia policy get'.\nTo change the policy use 'kopia policy set' command.\n")

		if err := setDefaultMaintenanceParameters(ctx, w); err != nil {
			return errors.Wrap(err, "unable to set maintenance parameters")
		}

		return nil
	})
}
