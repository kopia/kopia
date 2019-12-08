package cli

import (
	"context"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot/policy"
)

var (
	createCommand = repositoryCommands.Command("create", "Create new repository in a specified location.")

	createBlockHashFormat       = createCommand.Flag("block-hash", "Block hash algorithm.").PlaceHolder("ALGO").Default(content.DefaultHash).Enum(content.SupportedHashAlgorithms()...)
	createBlockEncryptionFormat = createCommand.Flag("encryption", "Block encryption algorithm.").PlaceHolder("ALGO").Default(content.DefaultEncryption).Enum(content.SupportedEncryptionAlgorithms()...)
	createSplitter              = createCommand.Flag("object-splitter", "The splitter to use for new objects in the repository").Default(object.DefaultSplitter).Enum(object.SupportedSplitters...)

	createOnly = createCommand.Flag("create-only", "Create repository, but don't connect to it.").Short('c').Bool()

	createGlobalPolicyKeepLatest  = createCommand.Flag("keep-latest", "Number of most recent backups to keep per source").PlaceHolder("N").Default("10").Int()
	createGlobalPolicyKeepHourly  = createCommand.Flag("keep-hourly", "Number of most-recent hourly backups to keep per source").PlaceHolder("N").Default("48").Int()
	createGlobalPolicyKeepDaily   = createCommand.Flag("keep-daily", "Number of most-recent daily backups to keep per source").PlaceHolder("N").Default("14").Int()
	createGlobalPolicyKeepWeekly  = createCommand.Flag("keep-weekly", "Number of most-recent weekly backups to keep per source").PlaceHolder("N").Default("25").Int()
	createGlobalPolicyKeepMonthly = createCommand.Flag("keep-monthly", "Number of most-recent monthly backups to keep per source").PlaceHolder("N").Default("24").Int()
	createGlobalPolicyKeepAnnual  = createCommand.Flag("keep-annual", "Number of most-recent annual backups to keep per source").PlaceHolder("N").Default("3").Int()

	createGlobalPolicyDotIgnoreFiles = createCommand.Flag("dot-ignore", "List of dotfiles to look for ignore rules").Default(".kopiaignore").Strings()

	createGlobalPolicyInterval   = createCommand.Flag("snapshot-interval", "Interval between snapshots").Duration()
	createGlobalPolicyTimesOfDay = createCommand.Flag("snapshot-time", "Times of day when to take snapshot (HH:mm)").Strings()
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
	hasDataError := errors.New("has data")

	err := s.ListBlobs(ctx, "", func(cb blob.Metadata) error {
		return hasDataError
	})
	if err == hasDataError {
		return errors.New("found existing data in storage location")
	}

	return err
}

func runCreateCommandWithStorage(ctx context.Context, st blob.Storage) error {
	err := ensureEmpty(ctx, st)
	if err != nil {
		return errors.Wrap(err, "unable to get repository storage")
	}

	options := newRepositoryOptionsFromFlags()

	password, err := getPasswordFromFlags(true, false)
	if err != nil {
		return errors.Wrap(err, "getting password")
	}

	printStderr("Initializing repository with:\n")
	printStderr("  block hash:          %v\n", options.BlockFormat.Hash)
	printStderr("  encryption:          %v\n", options.BlockFormat.Encryption)
	printStderr("  splitter:            %v\n", options.ObjectFormat.Splitter)

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
	rep, err := repo.Open(ctx, repositoryConfigFileName(), password, applyOptionsFromFlags(nil))
	if err != nil {
		return errors.Wrap(err, "unable to open repository")
	}
	defer rep.Close(ctx) //nolint:errcheck

	globalPolicy, err := getInitialGlobalPolicy()
	if err != nil {
		return errors.Wrap(err, "unable to initialize global policy")
	}

	if err := policy.SetPolicy(ctx, rep, policy.GlobalPolicySourceInfo, globalPolicy); err != nil {
		return errors.Wrap(err, "unable to set global policy")
	}

	printPolicy(globalPolicy, nil)

	return nil
}

func getInitialGlobalPolicy() (*policy.Policy, error) {
	var sp policy.SchedulingPolicy

	sp.SetInterval(*createGlobalPolicyInterval)

	var timesOfDay []policy.TimeOfDay

	for _, tods := range *createGlobalPolicyTimesOfDay {
		for _, tod := range strings.Split(tods, ",") {
			var timeOfDay policy.TimeOfDay
			if err := timeOfDay.Parse(tod); err != nil {
				return nil, errors.Wrap(err, "unable to parse time of day")
			}

			timesOfDay = append(timesOfDay, timeOfDay)
		}
	}

	sp.TimesOfDay = policy.SortAndDedupeTimesOfDay(timesOfDay)

	return &policy.Policy{
		FilesPolicy: policy.FilesPolicy{
			DotIgnoreFiles: *createGlobalPolicyDotIgnoreFiles,
		},
		RetentionPolicy: policy.RetentionPolicy{
			KeepLatest:  createGlobalPolicyKeepLatest,
			KeepHourly:  createGlobalPolicyKeepHourly,
			KeepDaily:   createGlobalPolicyKeepDaily,
			KeepWeekly:  createGlobalPolicyKeepWeekly,
			KeepMonthly: createGlobalPolicyKeepMonthly,
			KeepAnnual:  createGlobalPolicyKeepAnnual,
		},
		SchedulingPolicy: sp,
		CompressionPolicy: policy.CompressionPolicy{
			CompressorName: "none",
		},
	}, nil
}
