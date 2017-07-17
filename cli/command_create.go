package cli

import (
	"crypto/rand"
	"fmt"
	"io"

	kingpin "gopkg.in/alecthomas/kingpin.v2"

	"github.com/kopia/kopia/blob"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/vault"
)

var (
	createCommand           = app.Command("create", "Create new vault and repository.")
	createCommandRepository = createCommand.Flag("repository", "Repository path.").Default("colocated").String()
	createObjectFormat      = createCommand.Flag("repo-format", "Format of repository objects.").PlaceHolder("FORMAT").Default(repo.DefaultObjectFormat).Enum(supportedObjectFormats()...)

	createMinBlockSize          = createCommand.Flag("min-block-size", "Minimum size of a data block.").PlaceHolder("KB").Default("1024").Int()
	createAvgBlockSize          = createCommand.Flag("avg-block-size", "Average size of a data block.").PlaceHolder("KB").Default("10240").Int()
	createMaxBlockSize          = createCommand.Flag("max-block-size", "Maximum size of a data block.").PlaceHolder("KB").Default("20480").Int()
	createObjectSplitter        = createCommand.Flag("object-splitter", "The splitter to use for new objects in the repository").Default("DYNAMIC").Enum(supportedObjectSplitters()...)
	createInlineBlobSize        = createCommand.Flag("inline-blob-size", "Maximum size of an inline data object.").PlaceHolder("KB").Default("32").Int()
	createVaultEncryptionFormat = createCommand.Flag("vault-encryption", "Vault encryption.").PlaceHolder("FORMAT").Default(vault.SupportedEncryptionAlgorithms[0]).Enum(vault.SupportedEncryptionAlgorithms...)
	createOverwrite             = createCommand.Flag("overwrite", "Overwrite existing data (DANGEROUS).").Bool()
	createOnly                  = createCommand.Flag("create-only", "Create the vault, but don't connect to it.").Short('c').Bool()
)

func init() {
	createCommand.Action(runCreateCommand)
}

func vaultFormat() (*vault.Format, error) {
	f := &vault.Format{
		Version: "1",
	}
	f.UniqueID = make([]byte, 32)
	_, err := io.ReadFull(rand.Reader, f.UniqueID)
	if err != nil {
		return nil, err
	}
	f.EncryptionAlgorithm = *createVaultEncryptionFormat
	return f, nil
}

func repositoryFormat() (*repo.Format, error) {
	f := &repo.Format{
		Version:                1,
		Secret:                 make([]byte, 32),
		MasterKey:              make([]byte, 32),
		MaxInlineContentLength: int32(*createInlineBlobSize * 1024),
		ObjectFormat:           *createObjectFormat,

		Splitter:     *createObjectSplitter,
		MinBlockSize: int32(*createMinBlockSize * 1024),
		AvgBlockSize: int32(*createAvgBlockSize * 1024),
		MaxBlockSize: int32(*createMaxBlockSize * 1024),
	}

	if _, err := io.ReadFull(rand.Reader, f.Secret); err != nil {
		return nil, err
	}

	if _, err := io.ReadFull(rand.Reader, f.MasterKey); err != nil {
		return nil, err
	}

	return f, nil
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

func runCreateCommand(context *kingpin.ParseContext) error {
	if *vaultPath == "" {
		return fmt.Errorf("--vault is required")
	}
	vaultStorage, err := openStorageAndEnsureEmpty(*vaultPath)
	if err != nil {
		return fmt.Errorf("unable to get vault storage: %v", err)
	}

	repoFormat, err := repositoryFormat()
	if err != nil {
		return fmt.Errorf("unable to initialize repository format: %v", err)
	}

	fmt.Printf(
		"Initializing repository in with format %q, splitter %q and maximum object size %v.\n",
		repoFormat.ObjectFormat,
		repoFormat.Splitter,
		repoFormat.MaxBlockSize)

	vf, err := vaultFormat()
	if err != nil {
		return fmt.Errorf("unable to initialize vault format: %v", err)
	}

	creds, err := getVaultCredentials(true)
	if err != nil {
		return fmt.Errorf("unable to get credentials: %v", err)
	}

	if err := repoFormat.Validate(); err != nil {
		return fmt.Errorf("invalid format")
	}

	fmt.Printf(
		"Initializing vault with encryption '%v'.\n",
		vf.EncryptionAlgorithm)

	var vlt *vault.Vault

	if *createCommandRepository == "colocated" {
		vlt, err = vault.CreateColocated(vaultStorage, vf, creds, repoFormat)
	} else {
		repositoryStorage, err := openStorageAndEnsureEmpty(*createCommandRepository)
		if err != nil {
			return fmt.Errorf("unable to get repository storage: %v", err)
		}
		vlt, err = vault.Create(vaultStorage, vf, creds, repositoryStorage, repoFormat)
	}

	if err != nil {
		return fmt.Errorf("cannot create vault: %v", err)
	}

	if !*createOnly {
		if err := persistVaultConfig(vlt); err != nil {
			return err
		}

		fmt.Println("Connected to vault:", *vaultPath)
	}

	return nil
}

func supportedObjectFormats() []string {
	var r []string
	for k := range repo.SupportedFormats {
		r = append(r, k)
	}
	return r
}

func supportedObjectSplitters() []string {
	var r []string
	for k := range repo.SupportedSplitters {
		r = append(r, k)
	}
	return r
}
