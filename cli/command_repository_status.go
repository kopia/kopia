package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"reflect"

	"github.com/kopia/kopia/internal/scrubber"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
)

var (
	statusCommand                       = repositoryCommands.Command("status", "Display the status of connected repository.")
	statusReconnectToken                = statusCommand.Flag("reconnect-token", "Display reconnect command").Short('t').Bool()
	statusReconnectTokenIncludePassword = statusCommand.Flag("reconnect-token-with-password", "Include password in reconnect token").Short('s').Bool()
)

func runStatusCommand(ctx context.Context, rep *repo.Repository) error {
	fmt.Printf("Config file:         %v\n", rep.ConfigFile)

	ci := rep.Blobs.ConnectionInfo()
	fmt.Printf("Storage type:        %v\n", ci.Type)

	if cjson, err := json.MarshalIndent(scrubber.ScrubSensitiveData(reflect.ValueOf(ci.Config)).Interface(), "                     ", "  "); err == nil {
		fmt.Printf("Storage config:      %v\n", string(cjson))
	}

	fmt.Println()

	fmt.Println()
	fmt.Printf("Unique ID:           %x\n", rep.UniqueID)
	fmt.Println()
	fmt.Printf("Block hash:          %v\n", rep.Content.Format.Hash)
	fmt.Printf("Block encryption:    %v\n", rep.Content.Format.Encryption)
	fmt.Printf("Block fmt version:   %v\n", rep.Content.Format.Version)
	fmt.Printf("Max pack length:     %v\n", units.BytesStringBase2(int64(rep.Content.Format.MaxPackSize)))
	fmt.Printf("Splitter:            %v\n", rep.Objects.Format.Splitter)

	if *statusReconnectToken {
		pass := ""

		if *statusReconnectTokenIncludePassword {
			pass = mustGetPasswordFromFlags(false, true)
		}

		tok, err := rep.Token(pass)
		if err != nil {
			return err
		}

		fmt.Printf("\nTo reconnect to the repository use:\n\n$ kopia repository connect from-config --token %v\n\n", tok)
		if pass != "" {
			fmt.Printf("NOTICE: The token printed above can be trivially decoded to reveal the repository password. Do not store it in an unsecured place.\n")
		}
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
