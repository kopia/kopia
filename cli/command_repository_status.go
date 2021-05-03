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

type commandRepositoryStatus struct {
	statusReconnectToken                bool
	statusReconnectTokenIncludePassword bool

	svc advancedAppServices
}

func (c *commandRepositoryStatus) setup(svc advancedAppServices, parent commandParent) {
	cmd := parent.Command("status", "Display the status of connected repository.")
	cmd.Flag("reconnect-token", "Display reconnect command").Short('t').BoolVar(&c.statusReconnectToken)
	cmd.Flag("reconnect-token-with-password", "Include password in reconnect token").Short('s').BoolVar(&c.statusReconnectTokenIncludePassword)
	cmd.Action(svc.repositoryReaderAction(c.run))

	c.svc = svc
}

func (c *commandRepositoryStatus) run(ctx context.Context, rep repo.Repository) error {
	fmt.Printf("Config file:         %v\n", c.svc.repositoryConfigFileName())
	fmt.Println()
	fmt.Printf("Description:         %v\n", rep.ClientOptions().Description)
	fmt.Printf("Hostname:            %v\n", rep.ClientOptions().Hostname)
	fmt.Printf("Username:            %v\n", rep.ClientOptions().Username)
	fmt.Printf("Read-only:           %v\n", rep.ClientOptions().ReadOnly)

	dr, ok := rep.(repo.DirectRepository)
	if !ok {
		return nil
	}

	fmt.Println()

	ci := dr.BlobReader().ConnectionInfo()
	fmt.Printf("Storage type:        %v\n", ci.Type)

	if cjson, err := json.MarshalIndent(scrubber.ScrubSensitiveData(reflect.ValueOf(ci.Config)).Interface(), "                     ", "  "); err == nil {
		fmt.Printf("Storage config:      %v\n", string(cjson))
	}

	fmt.Println()
	fmt.Printf("Unique ID:           %x\n", dr.UniqueID())
	fmt.Printf("Hash:                %v\n", dr.ContentReader().ContentFormat().Hash)
	fmt.Printf("Encryption:          %v\n", dr.ContentReader().ContentFormat().Encryption)
	fmt.Printf("Splitter:            %v\n", dr.ObjectFormat().Splitter)
	fmt.Printf("Format version:      %v\n", dr.ContentReader().ContentFormat().Version)
	fmt.Printf("Max pack length:     %v\n", units.BytesStringBase2(int64(dr.ContentReader().ContentFormat().MaxPackSize)))

	if !c.statusReconnectToken {
		return nil
	}

	pass := ""

	if c.statusReconnectTokenIncludePassword {
		var err error

		pass, err = c.svc.getPasswordFromFlags(ctx, false, true)
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
