package main

import (
	"crypto/rand"
	"fmt"
	"io"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/vault"

	"github.com/kopia/kopia/blob"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	createCommand               = app.Command("create", "Create new vault.")
	createCommandRepository     = createCommand.Flag("repository", "Repository path.").Required().String()
	createMaxBlobSize           = createCommand.Flag("max-blob-size", "Maximum size of a data chunk in bytes.").Default("20000000").Int()
	createInlineBlobSize        = createCommand.Flag("inline-blob-size", "Maximum size of an inline data chunk in bytes.").Default("32768").Int()
	createVaultEncryptionFormat = createCommand.Flag("vault-encryption", "Vault encryption format").Default("aes-256").Enum(supportedVaultEncryptionFormats()...)
	createObjectFormat          = createCommand.Flag("object-format", "Specifies custom object format to be used").Default("sha256t128-aes256").Enum(supportedObjectFormats()...)
	createOverwrite             = createCommand.Flag("overwrite", "Overwrite existing data.").Bool()
)

func init() {
	createCommand.Action(runCreateCommand)
}

func vaultFormat() (*vault.Format, error) {
	f := &vault.Format{
		Version:  "1",
		Checksum: "hmac-sha-256",
	}
	f.UniqueID = make([]byte, 32)
	_, err := io.ReadFull(rand.Reader, f.UniqueID)
	if err != nil {
		return nil, err
	}
	f.Encryption = *createVaultEncryptionFormat
	return f, nil
}

func repositoryFormat() (*repo.Format, error) {
	f := &repo.Format{
		Version:           "1",
		Secret:            make([]byte, 32),
		MaxBlobSize:       *createMaxBlobSize,
		MaxInlineBlobSize: *createInlineBlobSize,
		ObjectFormat:      *createObjectFormat,
	}

	_, err := io.ReadFull(rand.Reader, f.Secret)
	if err != nil {
		return nil, err
	}

	return f, nil
}

func openStorageAndEnsureEmpty(url string) (blob.Storage, error) {
	s, err := blob.NewStorageFromURL(url)
	if err != nil {
		return nil, err
	}
	ch := s.ListBlocks("")
	_, hasData := <-ch

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

	repositoryStorage, err := openStorageAndEnsureEmpty(*createCommandRepository)
	if err != nil {
		return fmt.Errorf("unable to get repository storage: %v", err)
	}

	repoFormat, err := repositoryFormat()
	if err != nil {
		return fmt.Errorf("unable to initialize repository format: %v", err)
	}

	fmt.Printf(
		"Initializing repository in '%s' with format '%v' and maximum object size %v.\n",
		repositoryStorage.Configuration().Config.ToURL().String(),
		repoFormat.ObjectFormat,
		repoFormat.MaxBlobSize)

	masterKey, password, err := getKeyOrPassword(true)
	if err != nil {
		return fmt.Errorf("unable to get credentials: %v", err)
	}

	var v *vault.Vault
	vf, err := vaultFormat()
	if err != nil {
		return fmt.Errorf("unable to initialize vault format: %v", err)
	}

	fmt.Printf(
		"Initializing vault in '%s' with encryption '%v'.\n",
		vaultStorage.Configuration().Config.ToURL().String(),
		vf.Encryption)
	if masterKey != nil {
		v, err = vault.CreateWithKey(vaultStorage, vf, masterKey)
	} else {
		v, err = vault.CreateWithPassword(vaultStorage, vf, password)
	}
	if err != nil {
		return fmt.Errorf("cannot create vault: %v", err)
	}

	// Make repository to make sure the format is supported.
	_, err = repo.NewRepository(repositoryStorage, repoFormat)
	if err != nil {
		return fmt.Errorf("unable to initialize repository: %v", err)
	}

	if err := v.SetRepository(vault.RepositoryConfig{
		Storage: repositoryStorage.Configuration(),
		Format:  repoFormat,
	}); err != nil {
		return fmt.Errorf("unable to save repository configuration in vault: %v", err)
	}

	return nil
}

func supportedVaultEncryptionFormats() []string {
	return []string{
		"none",
		"aes-128",
		"aes-256",
	}
}

func supportedObjectFormats() []string {
	var r []string
	for _, o := range repo.SupportedFormats {
		r = append(r, o.Name)
	}
	return r
}
