package cli

import (
	"context"
	"os"
	"strconv"

	"github.com/kopia/kopia/repo/storage"
	"github.com/kopia/kopia/repo/storage/filesystem"
	"gopkg.in/alecthomas/kingpin.v2"
)

var options filesystem.Options

var (
	connectOwnerUID string
	connectOwnerGID string
	connectFileMode string
	connectDirMode  string
	connectFlat     bool
)

func connect(ctx context.Context, isNew bool) (storage.Storage, error) {
	fso := options
	if v := connectOwnerUID; v != "" {
		fso.FileUID = getIntPtrValue(v, 10)
	}
	if v := connectOwnerGID; v != "" {
		fso.FileGID = getIntPtrValue(v, 10)
	}

	fso.FileMode = getFileModeValue(connectFileMode, 0600)
	fso.DirectoryMode = getFileModeValue(connectDirMode, 0700)

	if connectFlat {
		fso.DirectoryShards = []int{}
	}

	if isNew {
		log.Infof("creating directory for repository: %v dir mode: %v", fso.Path, fso.DirectoryMode)
		if err := os.MkdirAll(fso.Path, fso.DirectoryMode); err != nil {
			log.Warningf("unable to create directory: %v", fso.Path)
		}
	}
	return filesystem.New(ctx, &fso)
}

func init() {
	RegisterStorageConnectFlags(
		"filesystem",
		"a filesystem",
		func(cmd *kingpin.CmdClause) {
			cmd.Flag("path", "Path to the repository").Required().StringVar(&options.Path)
			cmd.Flag("owner-uid", "User ID owning newly created files").PlaceHolder("USER").StringVar(&connectOwnerUID)
			cmd.Flag("owner-gid", "Group ID owning newly created files").PlaceHolder("GROUP").StringVar(&connectOwnerGID)
			cmd.Flag("file-mode", "File mode for newly created files (0600)").PlaceHolder("MODE").StringVar(&connectFileMode)
			cmd.Flag("dir-mode", "Mode of newly directory files (0700)").PlaceHolder("MODE").StringVar(&connectDirMode)
			cmd.Flag("flat", "Use flat directory structure").BoolVar(&connectFlat)
		},
		connect)
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
