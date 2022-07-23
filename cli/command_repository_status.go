package cli

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/scrubber"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/object"
)

type commandRepositoryStatus struct {
	statusReconnectToken                bool
	statusReconnectTokenIncludePassword bool

	svc advancedAppServices
	jo  jsonOutput
	out textOutput
}

// RepositoryStatus is used to display the repository info in JSON format.
type RepositoryStatus struct {
	ConfigFile  string `json:"configFile"`
	UniqueIDHex string `json:"uniqueIDHex"`

	ClientOptions repo.ClientOptions        `json:"clientOptions"`
	Storage       blob.ConnectionInfo       `json:"storage"`
	Capacity      *blob.Capacity            `json:"volume,omitempty"`
	ContentFormat content.FormattingOptions `json:"contentFormat"`
	ObjectFormat  object.Format             `json:"objectFormat"`
	BlobRetention content.BlobCfgBlob       `json:"blobRetention"`
}

func (c *commandRepositoryStatus) setup(svc advancedAppServices, parent commandParent) {
	cmd := parent.Command("status", "Display the status of connected repository.")
	cmd.Flag("reconnect-token", "Display reconnect command").Short('t').BoolVar(&c.statusReconnectToken)
	cmd.Flag("reconnect-token-with-password", "Include password in reconnect token").Short('s').BoolVar(&c.statusReconnectTokenIncludePassword)
	cmd.Action(svc.repositoryReaderAction(c.run))

	c.svc = svc
	c.out.setup(svc)
	c.jo.setup(svc, cmd)
}

func (c *commandRepositoryStatus) outputJSON(ctx context.Context, r repo.Repository) error {
	s := RepositoryStatus{
		ConfigFile:    c.svc.repositoryConfigFileName(),
		ClientOptions: r.ClientOptions(),
	}

	dr, ok := r.(repo.DirectRepository)
	if ok {
		ci := dr.BlobReader().ConnectionInfo()
		s.UniqueIDHex = hex.EncodeToString(dr.UniqueID())
		s.ObjectFormat = dr.ObjectFormat()
		s.BlobRetention = dr.BlobCfg()
		s.Storage = scrubber.ScrubSensitiveData(reflect.ValueOf(ci)).Interface().(blob.ConnectionInfo)                                                      // nolint:forcetypeassert
		s.ContentFormat = scrubber.ScrubSensitiveData(reflect.ValueOf(dr.ContentReader().ContentFormat().Struct())).Interface().(content.FormattingOptions) // nolint:forcetypeassert

		switch cp, err := dr.BlobVolume().GetCapacity(ctx); {
		case err == nil:
			s.Capacity = &cp
		case errors.Is(err, blob.ErrNotAVolume):
			// This is okay, we will just not populate the result.
		default:
			return errors.Wrap(err, "unable to get storage volume capacity")
		}
	}

	c.out.printStdout("%s\n", c.jo.jsonBytes(s))

	return nil
}

func (c *commandRepositoryStatus) dumpUpgradeStatus(ctx context.Context, dr repo.DirectRepository) error {
	drw, isDr := dr.(repo.DirectRepositoryWriter)
	if !isDr {
		return nil
	}

	l, err := drw.GetUpgradeLockIntent(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to get the upgrade lock intent")
	}

	if l == nil {
		return nil
	}

	locked, drainedClients := l.IsLocked(drw.Time())
	upgradeTime := l.UpgradeTime()

	c.out.printStdout("\n")
	c.out.printStdout("Ongoing upgrade:     %s\n", l.Message)
	c.out.printStdout("Upgrade Time:        %s\n", upgradeTime.Local())

	if locked {
		c.out.printStdout("Upgrade lock:        Locked\n")
	} else {
		c.out.printStdout("Upgrade lock:        Unlocked\n")
	}

	if drainedClients {
		c.out.printStdout("Lock status:         Fully Established\n")
	} else {
		c.out.printStdout("Lock status:         Draining\n")
	}

	return nil
}

func (c *commandRepositoryStatus) dumpRetentionStatus(dr repo.DirectRepository) {
	if blobcfg := dr.BlobCfg(); blobcfg.IsRetentionEnabled() {
		c.out.printStdout("\n")
		c.out.printStdout("Blob retention mode:     %s\n", blobcfg.RetentionMode)
		c.out.printStdout("Blob retention period:   %s\n", blobcfg.RetentionPeriod)
	}
}

// nolint: funlen
func (c *commandRepositoryStatus) run(ctx context.Context, rep repo.Repository) error {
	if c.jo.jsonOutput {
		return c.outputJSON(ctx, rep)
	}

	c.out.printStdout("Config file:         %v\n", c.svc.repositoryConfigFileName())
	c.out.printStdout("\n")
	c.out.printStdout("Description:         %v\n", rep.ClientOptions().Description)
	c.out.printStdout("Hostname:            %v\n", rep.ClientOptions().Hostname)
	c.out.printStdout("Username:            %v\n", rep.ClientOptions().Username)
	c.out.printStdout("Read-only:           %v\n", rep.ClientOptions().ReadOnly)

	t := rep.ClientOptions().FormatBlobCacheDuration
	if t > 0 {
		c.out.printStdout("Format blob cache:   %v\n", t)
	} else {
		c.out.printStdout("Format blob cache:   disabled\n")
	}

	dr, isDr := rep.(repo.DirectRepository)
	if !isDr {
		return nil
	}

	c.out.printStdout("\n")

	ci := dr.BlobReader().ConnectionInfo()
	c.out.printStdout("Storage type:        %v\n", ci.Type)

	switch cp, err := dr.BlobVolume().GetCapacity(ctx); {
	case err == nil:
		c.out.printStdout("Storage capacity:    %v\n", units.BytesStringBase10(int64(cp.SizeB)))
		c.out.printStdout("Storage available:   %v\n", units.BytesStringBase10(int64(cp.FreeB)))
	case errors.Is(err, blob.ErrNotAVolume):
		c.out.printStdout("Storage capacity:    unbounded\n")
	default:
		return errors.Wrap(err, "unable to get storage volume capacity")
	}

	if cjson, err := json.MarshalIndent(scrubber.ScrubSensitiveData(reflect.ValueOf(ci.Config)).Interface(), "                     ", "  "); err == nil {
		c.out.printStdout("Storage config:      %v\n", string(cjson))
	}

	contentFormat := dr.ContentReader().ContentFormat()

	c.out.printStdout("\n")
	c.out.printStdout("Unique ID:           %x\n", dr.UniqueID())
	c.out.printStdout("Hash:                %v\n", contentFormat.GetHashFunction())
	c.out.printStdout("Encryption:          %v\n", contentFormat.GetEncryptionAlgorithm())
	c.out.printStdout("Splitter:            %v\n", dr.ObjectFormat().Splitter)
	c.out.printStdout("Format version:      %v\n", contentFormat.FormatVersion())
	c.out.printStdout("Content compression: %v\n", dr.ContentReader().SupportsContentCompression())
	c.out.printStdout("Password changes:    %v\n", contentFormat.SupportsPasswordChange())

	c.outputRequiredFeatures(dr)

	c.out.printStdout("Max pack length:     %v\n", units.BytesStringBase2(int64(contentFormat.MaxPackBlobSize())))
	c.out.printStdout("Index Format:        v%v\n", contentFormat.WriteIndexVersion())

	if emgr, ok := dr.ContentReader().EpochManager(); ok {
		c.out.printStdout("\n")
		c.out.printStdout("Epoch Manager:       enabled\n")

		snap, err := emgr.Current(ctx)
		if err == nil {
			c.out.printStdout("Current Epoch: %v\n", snap.WriteEpoch)
		}

		c.out.printStdout("\n")
		c.out.printStdout("Epoch refresh frequency: %v\n", contentFormat.GetEpochRefreshFrequency())
		c.out.printStdout("Epoch advance on:        %v blobs or %v, minimum %v\n", contentFormat.GetEpochAdvanceOnCountThreshold(), units.BytesStringBase2(contentFormat.GetEpochAdvanceOnTotalSizeBytesThreshold()), contentFormat.GetMinEpochDuration())
		c.out.printStdout("Epoch cleanup margin:    %v\n", contentFormat.GetEpochCleanupSafetyMargin())
		c.out.printStdout("Epoch checkpoint every:  %v epochs\n", contentFormat.GetEpochFullCheckpointFrequency())
	} else {
		c.out.printStdout("Epoch Manager:       disabled\n")
	}

	c.dumpRetentionStatus(dr)

	if err := c.dumpUpgradeStatus(ctx, dr); err != nil {
		return errors.Wrap(err, "failed to dump upgrade status")
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

func (c *commandRepositoryStatus) outputRequiredFeatures(dr repo.DirectRepository) {
	if req, _ := dr.RequiredFeatures(); len(req) > 0 {
		var featureIDs []string

		for _, r := range req {
			featureIDs = append(featureIDs, string(r.Feature))
		}

		c.out.printStdout("Required Features:   %v\n", strings.Join(featureIDs, " "))
	}
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
