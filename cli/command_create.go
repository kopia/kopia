package cli

import (
	"fmt"

	kingpin "gopkg.in/alecthomas/kingpin.v2"

	"github.com/kopia/kopia/blob"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
)

var (
	createCommand            = app.Command("create", "Create new repository in a specified location.")
	createRepositoryLocation = createCommand.Arg("location", "Location where to create the repository").Required().String()

	createMetadataEncryptionFormat = createCommand.Flag("metadata-encryption", "Metadata item encryption.").PlaceHolder("FORMAT").Default(repo.SupportedMetadataEncryptionAlgorithms[0]).Enum(repo.SupportedMetadataEncryptionAlgorithms...)
	createObjectFormat             = createCommand.Flag("object-format", "Format of repository objects.").PlaceHolder("FORMAT").Default(repo.DefaultObjectFormat).Enum(repo.SupportedObjectFormats...)
	createObjectSplitter           = createCommand.Flag("object-splitter", "The splitter to use for new objects in the repository").Default("DYNAMIC").Enum(repo.SupportedObjectSplitters...)

	createMinBlockSize = createCommand.Flag("min-block-size", "Minimum size of a data block.").PlaceHolder("KB").Default("1024").Int()
	createAvgBlockSize = createCommand.Flag("avg-block-size", "Average size of a data block.").PlaceHolder("KB").Default("10240").Int()
	createMaxBlockSize = createCommand.Flag("max-block-size", "Maximum size of a data block.").PlaceHolder("KB").Default("20480").Int()

	createInlineBlobSize = createCommand.Flag("inline-blob-size", "Maximum size of an inline data object.").PlaceHolder("KB").Default("32").Int()
	createOverwrite      = createCommand.Flag("overwrite", "Overwrite existing data (DANGEROUS).").Bool()
	createOnly           = createCommand.Flag("create-only", "Create repository, but don't connect to it.").Short('c').Bool()
)

func init() {
	setupConnectOptions(createCommand)
	createCommand.Action(runCreateCommand)
}

func newRepositoryOptionsFromFlags() *repo.NewRepositoryOptions {
	return &repo.NewRepositoryOptions{
		MetadataEncryptionAlgorithm: *createMetadataEncryptionFormat,
		MaxInlineContentLength:      *createInlineBlobSize * 1024,
		ObjectFormat:                *createObjectFormat,

		Splitter:     *createObjectSplitter,
		MinBlockSize: *createMinBlockSize * 1024,
		AvgBlockSize: *createAvgBlockSize * 1024,
		MaxBlockSize: *createMaxBlockSize * 1024,
	}
}

func openStorageAndEnsureEmpty(url string) (blob.Storage, error) {
	s, err := newStorageFromURL(getContext(), url)
	if err != nil {
		return nil, err
	}
	ch, cancel := s.ListBlocks("")
	_, hasData := <-ch
	cancel()

	if hasData && !*createOverwrite {
		return nil, fmt.Errorf("found existing data in %v, specify --overwrite to use anyway", url)
	}

	return s, nil
}

func runCreateCommand(_ *kingpin.ParseContext) error {
	st, err := openStorageAndEnsureEmpty(*createRepositoryLocation)
	if err != nil {
		return fmt.Errorf("unable to get repository storage: %v", err)
	}

	options := newRepositoryOptionsFromFlags()

	creds, err := getRepositoryCredentials(true)
	if err != nil {
		return fmt.Errorf("unable to get credentials: %v", err)
	}

	fmt.Printf("Initializing repository with:\n")
	fmt.Printf("  metadata encryption: %v\n", options.MetadataEncryptionAlgorithm)
	fmt.Printf("  object format:       %v\n", options.ObjectFormat)
	switch options.Splitter {
	case "DYNAMIC":
		fmt.Printf("  object splitter:     DYNAMIC with block sizes (min:%v avg:%v max:%v)\n",
			units.BytesStringBase2(int64(options.MinBlockSize)),
			units.BytesStringBase2(int64(options.AvgBlockSize)),
			units.BytesStringBase2(int64(options.MaxBlockSize)))

	case "FIXED":
		fmt.Printf("  object splitter:     FIXED with with block size: %v\n", units.BytesStringBase2(int64(options.MaxBlockSize)))

	case "NEVER":
		fmt.Printf("  object splitter:     NEVER\n")
	}

	if err := repo.Initialize(st, options, creds); err != nil {
		return fmt.Errorf("cannot initialize repository: %v", err)
	}

	if !*createOnly {
		err := repo.Connect(getContext(), repositoryConfigFileName(), st, creds, connectOptions())
		if err != nil {
			return err
		}

		fmt.Println("Connected to repository:", *createRepositoryLocation)
	}

	return nil
}
