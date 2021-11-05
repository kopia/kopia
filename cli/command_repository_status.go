package cli

import (
	"context"
	"encoding/json"
	"os"
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
	out textOutput
}

func (c *commandRepositoryStatus) setup(svc advancedAppServices, parent commandParent) {
	cmd := parent.Command("status", "Display the status of connected repository.")
	cmd.Flag("reconnect-token", "Display reconnect command").Short('t').BoolVar(&c.statusReconnectToken)
	cmd.Flag("reconnect-token-with-password", "Include password in reconnect token").Short('s').BoolVar(&c.statusReconnectTokenIncludePassword)
	cmd.Action(svc.repositoryReaderAction(c.run))

	c.svc = svc
	c.out.setup(svc)
}

func (c *commandRepositoryStatus) run(ctx context.Context, rep repo.Repository) error {
	c.out.printStdout("Config file:         %v\n", c.svc.repositoryConfigFileName())
	c.out.printStdout("\n")
	c.out.printStdout("Description:         %v\n", rep.ClientOptions().Description)
	c.out.printStdout("Hostname:            %v\n", rep.ClientOptions().Hostname)
	c.out.printStdout("Username:            %v\n", rep.ClientOptions().Username)
	c.out.printStdout("Read-only:           %v\n", rep.ClientOptions().ReadOnly)

	if t := rep.ClientOptions().FormatBlobCacheDuration; t > 0 {
		c.out.printStdout("Format blob cache:   %v\n", t)
	} else {
		c.out.printStdout("Format blob cache:   disabled\n")
	}

	dr, ok := rep.(repo.DirectRepository)
	if !ok {
		return nil
	}

	c.out.printStdout("\n")

	ci := dr.BlobReader().ConnectionInfo()
	c.out.printStdout("Storage type:        %v\n", ci.Type)

	if cjson, err := json.MarshalIndent(scrubber.ScrubSensitiveData(reflect.ValueOf(ci.Config)).Interface(), "                     ", "  "); err == nil {
		c.out.printStdout("Storage config:      %v\n", string(cjson))
	}

	c.out.printStdout("\n")
	c.out.printStdout("Unique ID:           %x\n", dr.UniqueID())
	c.out.printStdout("Hash:                %v\n", dr.ContentReader().ContentFormat().Hash)
	c.out.printStdout("Encryption:          %v\n", dr.ContentReader().ContentFormat().Encryption)
	c.out.printStdout("Splitter:            %v\n", dr.ObjectFormat().Splitter)
	c.out.printStdout("Format version:      %v\n", dr.ContentReader().ContentFormat().Version)
	c.out.printStdout("Content compression: %v\n", dr.ContentReader().SupportsContentCompression())
	c.out.printStdout("Password changes:    %v\n", dr.ContentReader().ContentFormat().EnablePasswordChange)

	c.out.printStdout("Max pack length:     %v\n", units.BytesStringBase2(int64(dr.ContentReader().ContentFormat().MaxPackSize)))
	c.out.printStdout("Index Format:        v%v\n", dr.ContentReader().ContentFormat().IndexVersion)

	if emgr, ok := dr.ContentReader().EpochManager(); ok {
		c.out.printStdout("\n")
		c.out.printStdout("Epoch Manager:       enabled\n")

		snap, err := emgr.Current(ctx)
		if err == nil {
			c.out.printStdout("Current Epoch: %v\n", snap.WriteEpoch)
		}

		c.out.printStdout("\n")
		c.out.printStdout("Epoch refresh frequency: %v\n", emgr.Params.EpochRefreshFrequency)
		c.out.printStdout("Epoch advance on:        %v blobs or %v, minimum %v\n", emgr.Params.EpochAdvanceOnCountThreshold, units.BytesStringBase2(emgr.Params.EpochAdvanceOnTotalSizeBytesThreshold), emgr.Params.MinEpochDuration)
		c.out.printStdout("Epoch cleanup margin:    %v\n", emgr.Params.CleanupSafetyMargin)
		c.out.printStdout("Epoch checkpoint every:  %v epochs\n", emgr.Params.FullCheckpointFrequency)
	} else {
		c.out.printStdout("Epoch Manager:       disabled\n")
	}

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

	c.out.printStdout("\nTo reconnect to the repository use:\n\n$ kopia repository connect from-config --token %v\n\n", tok)

	if pass != "" {
		c.out.printStdout("NOTICE: The token printed above can be trivially decoded to reveal the repository password. Do not store it in an unsecured place.\n")
	}

	return nil
}

func scanCacheDir(dirname string) (fileCount int, totalFileLength int64, err error) {
	entries, err := os.ReadDir(dirname)
	if err != nil {
		return 0, 0, errors.Wrap(err, "unable to read cache directory")
	}

	for _, e := range entries {
		fi, err := e.Info()
		if os.IsNotExist(err) {
			// we lost the race, the file was deleted since it was listed.
			continue
		}

		if err != nil {
			return 0, 0, errors.Wrap(err, "unable to read file info")
		}

		if fi.IsDir() {
			subdir := filepath.Join(dirname, fi.Name())

			c, l, err2 := scanCacheDir(subdir)
			if err2 != nil {
				return 0, 0, err2
			}

			fileCount += c
			totalFileLength += l

			continue
		}

		fileCount++

		totalFileLength += fi.Size()
	}

	return fileCount, totalFileLength, nil
}
