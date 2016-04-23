package main

import (
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/kopia/kopia/auth"

	"github.com/kopia/kopia/config"

	"github.com/kopia/kopia/blob"
	"github.com/kopia/kopia/cas"
	"github.com/kopia/kopia/session"
)

var (
	initCommand        = app.Command("init", "Creates default configuration file.")
	initForceOverwrite = initCommand.Flag("force", "Force creation, even if one exists.").Bool()

	initMaxBlobSize    = initCommand.Flag("max-blob-size", "Maximum size of a data chunk in bytes.").Default("4000000").Int()
	initInlineBlobSize = initCommand.Flag("inline-blob-size", "Maximum size of an inline data chunk in bytes.").Default("32768").Int()

	initSaveKey  = initCommand.Flag("save-key", "Save master key in the configuration file").Default("true").Bool()
	initSecurity = initCommand.Flag("security", "Security mode, one of 'none', 'default' or 'custom'.").Default("default").Enum("none", "default", "custom")

	initCustomFormat = initCommand.Flag("object-format", "Specifies custom object format to be used").String()
)

func runInitCommandForRepository(s blob.Storage, defaultSalt string) error {
	var creds auth.Credentials

	sess, err := session.New(s, creds)
	if err != nil {
		return err
	}

	_, err = sess.OpenObjectManager()
	switch err {
	case session.ErrConfigNotFound:
		// all good, config not found.

	case nil:
		if !*initForceOverwrite {
			// Config already present, only proceed if --force is
			fmt.Printf("Repository already exists. Use --force to overwrite.\n")
			return nil
		}

	default:
		fmt.Printf("Unexpected error when accessing repository: %v. Use --force to overwrite.\n", err)
		if !*initForceOverwrite {
			return nil
		}
	}

	var format cas.Format
	format.Version = "1"
	format.MaxInlineBlobSize = *initInlineBlobSize

	format.Secret = make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, format.Secret); err != nil {
		return err
	}

	switch *initSecurity {
	case "none":
		format.ObjectFormat = "sha256"

	case "default":
		format.ObjectFormat = "hmac-sha512-aes256"

	case "custom":
		format.ObjectFormat = getCustomFormat()
	}

	fmt.Printf("Initializing repository with format: '%v'\n", format.ObjectFormat)

	if _, err := sess.InitObjectManager(format); err != nil {
		return err
	}

	cfg := config.Config{
		Storage: s.Configuration(),
	}

	f, err := os.Create(configFileName())
	if err != nil {
		return err
	}
	defer f.Close()
	cfg.SaveTo(f)

	return nil
}

func getCustomFormat() string {
	if *initCustomFormat != "" {
		if cas.SupportedFormats.Find(*initCustomFormat) == nil {
			fmt.Printf("Format '%s' is not recognized.\n", *initCustomFormat)
		}
		return *initCustomFormat
	}

	fmt.Printf("  %2v | %-30v | %v | %v | %v |\n", "#", "Format", "Hash", "Encryption", "Block ID Length")
	fmt.Println(strings.Repeat("-", 76) + "+")
	for i, o := range cas.SupportedFormats {
		encryptionString := ""
		if o.IsEncrypted() {
			encryptionString = fmt.Sprintf("%d-bit", o.EncryptionKeySizeBits())
		}
		fmt.Printf("  %2v | %-30v | %4v | %10v | %15v |\n", i+1, o.Name, o.HashSizeBits(), encryptionString, o.ObjectIDLength()*2)
	}
	fmt.Println(strings.Repeat("-", 76) + "+")

	fmt.Printf("Select format (1-%d): ", len(cas.SupportedFormats))
	for {
		var number int

		if n, err := fmt.Scanf("%d\n", &number); n == 1 && err == nil && number >= 1 && number <= len(cas.SupportedFormats) {
			fmt.Printf("You selected '%v'\n", cas.SupportedFormats[number-1].Name)
			return cas.SupportedFormats[number-1].Name
		}

		fmt.Printf("Invalid selection. Select format (1-%d): ", len(cas.SupportedFormats))
	}
}
