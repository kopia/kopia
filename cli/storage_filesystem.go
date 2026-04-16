package cli

import (
	"context"
	"os"
	"strconv"

	"github.com/alecthomas/kingpin/v2"
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

func (c *storageFilesystemFlags) Setup(_ StorageProviderServices, cmd *kingpin.CmdClause) {
	cmd.Flag("path", "Path to the repository").Required().StringVar(&c.options.Path)
	cmd.Flag("owner-uid", "User ID owning newly created files").PlaceHolder("USER").StringVar(&c.connectOwnerUID)
	cmd.Flag("owner-gid", "Group ID owning newly created files").PlaceHolder("GROUP").StringVar(&c.connectOwnerGID)
	cmd.Flag("file-mode", "File mode for newly created files (0600)").PlaceHolder("MODE").StringVar(&c.connectFileMode)
	cmd.Flag("dir-mode", "Mode of newly directory files (0700)").PlaceHolder("MODE").StringVar(&c.connectDirMode)
	cmd.Flag("flat", "Use flat directory structure").BoolVar(&c.connectFlat)
	cmd.Flag("list-parallelism", "Set list parallelism").Hidden().IntVar(&c.options.ListParallelism)

	commonThrottlingFlags(cmd, &c.options.Limits)
}

func (c *storageFilesystemFlags) Connect(ctx context.Context, isCreate bool, formatVersion int) (blob.Storage, error) {
	fso := c.options

	fso.Path = ospath.ResolveUserFriendlyPath(fso.Path, false)

	if !ospath.IsAbs(fso.Path) {
		return nil, errors.New("filesystem repository path must be absolute")
	}

	if v := c.connectOwnerUID; v != "" {
		//nolint:mnd
		fso.FileUID = getIntPtrValue(v, 10)
	}

	if v := c.connectOwnerGID; v != "" {
		//nolint:mnd
		fso.FileGID = getIntPtrValue(v, 10)
	}

	fso.FileMode = getFileModeValue(c.connectFileMode, defaultFileMode)
	fso.DirectoryMode = getFileModeValue(c.connectDirMode, defaultDirMode)
	fso.DirectoryShards = initialDirectoryShards(c.connectFlat, formatVersion)

	//nolint:wrapcheck
	return filesystem.New(ctx, &fso, isCreate)
}

func initialDirectoryShards(flat bool, formatVersion int) []int {
	if flat {
		return []int{}
	}

	// when creating a repository for version 1, use fixed {3,3} sharding,
	// otherwise old client can't read it.
	if formatVersion == 1 {
		return []int{3, 3}
	}

	return nil
}

func getIntPtrValue(value string, base int) *int {
	if int64Val, err := strconv.ParseInt(value, base, 32); err == nil {
		intVal := int(int64Val)
		return &intVal
	}

	return nil
}

func getFileModeValue(value string, def os.FileMode) os.FileMode {
	if uint32Val, err := strconv.ParseUint(value, 8, 32); err == nil {
		return os.FileMode(uint32Val)
	}

	return def
}
