package cli

import (
	"context"
	"os"
	"path/filepath"
	"strconv"

	"github.com/alecthomas/kingpin"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/ospath"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/filesystem"
)

const (
	defaultFileMode = 0o600
	defaultDirMode  = 0o700
)

type storageFilesystemFlags struct {
	options filesystem.Options

	connectOwnerUID string
	connectOwnerGID string
	connectFileMode string
	connectDirMode  string
	connectFlat     bool
}

func (c *storageFilesystemFlags) setup(_ storageProviderServices, cmd *kingpin.CmdClause) {
	cmd.Flag("path", "Path to the repository").Required().StringVar(&c.options.Path)
	cmd.Flag("owner-uid", "User ID owning newly created files").PlaceHolder("USER").StringVar(&c.connectOwnerUID)
	cmd.Flag("owner-gid", "Group ID owning newly created files").PlaceHolder("GROUP").StringVar(&c.connectOwnerGID)
	cmd.Flag("file-mode", "File mode for newly created files (0600)").PlaceHolder("MODE").StringVar(&c.connectFileMode)
	cmd.Flag("dir-mode", "Mode of newly directory files (0700)").PlaceHolder("MODE").StringVar(&c.connectDirMode)
	cmd.Flag("flat", "Use flat directory structure").BoolVar(&c.connectFlat)
}

func (c *storageFilesystemFlags) connect(ctx context.Context, isNew bool) (blob.Storage, error) {
	fso := c.options

	fso.Path = ospath.ResolveUserFriendlyPath(fso.Path, false)

	if !filepath.IsAbs(fso.Path) {
		return nil, errors.Errorf("filesystem repository path must be absolute")
	}

	if v := c.connectOwnerUID; v != "" {
		// nolint:gomnd
		fso.FileUID = getIntPtrValue(v, 10)
	}

	if v := c.connectOwnerGID; v != "" {
		// nolint:gomnd
		fso.FileGID = getIntPtrValue(v, 10)
	}

	fso.FileMode = getFileModeValue(c.connectFileMode, defaultFileMode)
	fso.DirectoryMode = getFileModeValue(c.connectDirMode, defaultDirMode)

	if c.connectFlat {
		fso.DirectoryShards = []int{}
	}

	if isNew {
		log(ctx).Debugf("creating directory for repository: %v dir mode: %v", fso.Path, fso.DirectoryMode)

		if err := os.MkdirAll(fso.Path, fso.DirectoryMode); err != nil {
			log(ctx).Errorf("unable to create directory: %v", fso.Path)
		}
	}

	// nolint:wrapcheck
	return filesystem.New(ctx, &fso)
}

func getIntPtrValue(value string, base int) *int {
	// nolint:gomnd
	if int64Val, err := strconv.ParseInt(value, base, 32); err == nil {
		intVal := int(int64Val)
		return &intVal
	}

	return nil
}

func getFileModeValue(value string, def os.FileMode) os.FileMode {
	// nolint:gomnd
	if uint32Val, err := strconv.ParseUint(value, 8, 32); err == nil {
		return os.FileMode(uint32Val)
	}

	return def
}
