package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"reflect"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/scrubber"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
)

var (
	statusCommand                       = repositoryCommands.Command("status", "Display the status of connected repository.")
	statusReconnectToken                = statusCommand.Flag("reconnect-token", "Display reconnect command").Short('t').Bool()
	statusReconnectTokenIncludePassword = statusCommand.Flag("reconnect-token-with-password", "Include password in reconnect token").Short('s').Bool()
)

func runStatusCommand(ctx context.Context, rep repo.Repository) error {
	fmt.Printf("Config file:         %v\n", repositoryConfigFileName())
	fmt.Println()
	fmt.Printf("Description:         %v\n", rep.ClientOptions().Description)
	fmt.Printf("Hostname:            %v\n", rep.ClientOptions().Hostname)
	fmt.Printf("Username:            %v\n", rep.ClientOptions().Username)
	fmt.Printf("Read-only:           %v\n", rep.ClientOptions().ReadOnly)

	dr, ok := rep.(*repo.DirectRepository)
	if !ok {
		return nil
	}

	fmt.Println()

	ci := dr.Blobs.ConnectionInfo()
	fmt.Printf("Storage type:        %v\n", ci.Type)

	if cjson, err := json.MarshalIndent(scrubber.ScrubSensitiveData(reflect.ValueOf(ci.Config)).Interface(), "                     ", "  "); err == nil {
		fmt.Printf("Storage config:      %v\n", string(cjson))
	}

	fmt.Println()
	fmt.Printf("Unique ID:           %x\n", dr.UniqueID)
	fmt.Printf("Hash:                %v\n", dr.Content.Format.Hash)
	fmt.Printf("Encryption:          %v\n", dr.Content.Format.Encryption)
	fmt.Printf("Splitter:            %v\n", dr.Objects.Format.Splitter)
	fmt.Printf("Format version:      %v\n", dr.Content.Format.Version)
	fmt.Printf("Max pack length:     %v\n", units.BytesStringBase2(int64(dr.Content.Format.MaxPackSize)))

	if !*statusReconnectToken {
		return nil
	}

	pass := ""

	if *statusReconnectTokenIncludePassword {
		var err error

		pass, err = getPasswordFromFlags(ctx, false, true)
		if err != nil {
			return errors.Wrap(err, "getting password")
		}
	}

	tok, err := dr.Token(pass)
	if err != nil {
		return errors.Wrap(err, "error computing repository token")
	}

	fmt.Printf("\nTo reconnect to the repository use:\n\n$ kopia repository connect from-config --token %v\n\n", tok)

	if pass != "" {
		fmt.Printf("NOTICE: The token printed above can be trivially decoded to reveal the repository password. Do not store it in an unsecured place.\n")
	}

	return nil
}

func scanCacheDir(dirname string) (fileCount int, totalFileLength int64, err error) {
	entries, err := ioutil.ReadDir(dirname)
	if err != nil {
		return 0, 0, nil
	}

	for _, e := range entries {
		if e.IsDir() {
			subdir := filepath.Join(dirname, e.Name())

			c, l, err2 := scanCacheDir(subdir)
			if err2 != nil {
				return 0, 0, err2
			}

			fileCount += c
			totalFileLength += l

			continue
		}

		fileCount++

		totalFileLength += e.Size()
	}

	return
}

func init() {
	statusCommand.Action(repositoryAction(runStatusCommand))
}
