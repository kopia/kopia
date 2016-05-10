package main

import (
	"fmt"
	"log"
	"path/filepath"
	"time"

	"github.com/kopia/kopia/backup"
	"github.com/kopia/kopia/repo"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	backupsCommand    = app.Command("backups", "List backup history.")
	backupsDirectory  = backupsCommand.Arg("directory", "Directory to show history of").ExistingDir()
	backupsAll        = backupsCommand.Flag("all", "Show history of all backups").Bool()
	maxResultsPerPath = backupsCommand.Flag("maxresults", "Maximum number of results").Default("100").Int()
)

func runBackupsCommand(context *kingpin.ParseContext) error {
	var options []repo.RepositoryOption

	if *backupWriteBack > 0 {
		options = append(options, repo.WriteBack(*backupWriteBack))
	}

	if *backupWriteLimit > 0 {
		options = append(options, repo.WriteLimit(*backupWriteLimit*1000000))

	}

	vlt, err := openVault()
	if err != nil {
		return err
	}

	mgr, err := vlt.OpenRepository()
	if err != nil {
		return err
	}
	defer mgr.Close()

	var prefix string

	if !*backupsAll {

		dir, err := filepath.Abs(*backupsDirectory)
		if err != nil {
			return fmt.Errorf("invalid directory: '%s': %s", *backupsDirectory, err)
		}

		manifest := backup.Manifest{
			SourceDirectory: filepath.Clean(dir),
			HostName:        getBackupHostName(),
			UserName:        getBackupUser(),
		}
		prefix = manifest.SourceID() + "."
	}

	previous, err := vlt.List("B" + prefix)
	if err != nil {
		return fmt.Errorf("error listing previous backups")
	}

	var lastHost string
	var lastUser string
	var lastDir string
	var count int

	for _, n := range previous {
		var m backup.Manifest
		if err := vlt.Get(n, &m); err != nil {
			return fmt.Errorf("error loading previous backup: %vlt", err)
		}

		if m.HostName != lastHost || m.UserName != lastUser || m.SourceDirectory != lastDir {
			log.Printf("%v @ %v : %v", m.UserName, m.HostName, m.SourceDirectory)
			lastDir = m.SourceDirectory
			lastUser = m.UserName
			lastHost = m.HostName
			count = 0
		}

		if count < *maxResultsPerPath {
			log.Printf("  %v %v", m.RootObjectID, m.StartTime.Format(time.RFC850))
			count++
		}
	}

	return nil
}

func init() {
	backupsCommand.Action(runBackupsCommand)
	backupsCommand.Flag("host", "Override backup hostname.").StringVar(backupHostName)
	backupsCommand.Flag("user", "Override backup user.").StringVar(backupUser)
}
