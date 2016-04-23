package main

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/kopia/kopia/blob"

	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	initFSCommand  = initCommand.Command("fs", "Initializes repository in a filesystem directory.")
	initFSPath     = initFSCommand.Arg("directory", "Repository path.").Required().String()
	initFSVerbose  = initFSCommand.Flag("verbose_filesystem", "Log filesystem operations.").Bool()
	initFSUID      = initFSCommand.Flag("uid", "Owner ID for files in the repository.").String()
	initFSGID      = initFSCommand.Flag("gid", "Group ID for files in the repository.").String()
	initFSFileMode = initFSCommand.Flag("filemode", "File mode (octal, prefixed with 0).").String()
	initFSDirMode  = initFSCommand.Flag("dirmode", "Directory mode (octal, prefixed with 0)").String()
)

func init() {
	initFSCommand.Action(runInitFileSystemCommand)
}

func runInitFileSystemCommand(context *kingpin.ParseContext) error {
	var options blob.FSStorageOptions

	options.Path = *initFSPath

	if *initFSUID != "" {
		uid, err := strconv.Atoi(*initFSUID)
		if err != nil {
			return err
		}

		options.FileUID = &uid
	}
	if *initFSGID != "" {
		gid, err := strconv.Atoi(*initFSGID)
		if err != nil {
			return err
		}

		options.FileGID = &gid
	}

	if *initFSFileMode != "" {
		m, err := strconv.ParseInt(*initFSFileMode, 8, 16)
		if err != nil {
			return err
		}
		options.FileMode = os.FileMode(m)
	} else {
		options.FileMode = 0664
	}

	if *initFSDirMode != "" {
		m, err := strconv.ParseInt(*initFSDirMode, 8, 16)
		if err != nil {
			return err
		}
		options.DirectoryMode = os.FileMode(m)
	} else {
		options.DirectoryMode = 0775
	}

	_, err := os.Stat(options.Path)
	if os.IsNotExist(err) {
		os.MkdirAll(options.Path, options.DirectoryMode)
	}

	repository, err := blob.NewFSStorage(&options)

	if err != nil {
		return err
	}

	salt := strings.ToLower(filepath.Base(*initFSPath))

	return runInitCommandForRepository(repository, salt)
}
