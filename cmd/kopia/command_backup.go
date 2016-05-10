package main

import (
	"crypto/rand"
	"fmt"
	"log"
	"math"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"time"

	"github.com/kopia/kopia/backup"

	"github.com/kopia/kopia/cas"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

const (
	backupMaxDescriptionLength = 1024
)

var (
	backupCommand = app.Command("backup", "Copies local directory to backup repository.")

	backupDirectories = backupCommand.Arg("directory", "Directories to back up").Required().ExistingDirs()

	backupHostName    = backupCommand.Flag("host", "Override backup hostname.").String()
	backupUser        = backupCommand.Flag("user", "Override backup user.").String()
	backupDescription = backupCommand.Flag("description", "Free-form backup description.").String()

	backupCheckpointInterval      = backupCommand.Flag("checkpoint_interval", "Periodically flush backup (default=30m).").PlaceHolder("TIME").Default("30m").Duration()
	backupCheckpointEveryMB       = backupCommand.Flag("checkpoint_every_mb", "Checkpoint backup after each N megabytes (default=1000).").PlaceHolder("N").Default("1000").Int()
	backupCheckpointUploadLimitMB = backupCommand.Flag("upload_limit_mb", "Stop the backup process after the specified amount of data (in MB) has been uploaded.").PlaceHolder("MB").Default("0").Int()

	backupWriteBack  = backupCommand.Flag("async-write", "Perform updates asynchronously.").PlaceHolder("N").Default("0").Int()
	backupWriteLimit = backupCommand.Flag("write-limit", "Stop backup after writing the given amount of data").PlaceHolder("MB").Default("0").Int64()
)

func runBackupCommand(context *kingpin.ParseContext) error {
	var options []cas.RepositoryOption

	if *backupWriteBack > 0 {
		options = append(options, cas.WriteBack(*backupWriteBack))
	}

	if *backupWriteLimit > 0 {
		options = append(options, cas.WriteLimit(*backupWriteLimit*1000000))

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

	bgen, err := backup.NewGenerator(mgr)
	if err != nil {
		return err
	}

	for _, backupDirectory := range *backupDirectories {
		dir, err := filepath.Abs(backupDirectory)
		if err != nil {
			return fmt.Errorf("invalid directory: '%s': %s", backupDirectory, err)
		}

		manifest := backup.Manifest{
			StartTime:       time.Now(),
			SourceDirectory: filepath.Clean(dir),

			HostName:    getBackupHostName(),
			UserName:    getBackupUser(),
			Description: *backupDescription,
		}

		if len(manifest.Description) > backupMaxDescriptionLength {
			return fmt.Errorf("description too long")
		}

		previous, err := vlt.List("B" + manifest.SourceID() + ".")
		if err != nil {
			return fmt.Errorf("error listing previous backups")
		}

		var oldManifest *backup.Manifest

		if len(previous) > 0 {
			var m backup.Manifest
			if err := vlt.Get(previous[0], &m); err != nil {
				return fmt.Errorf("error loading previous backup: %vlt", err)
			}
			oldManifest = &m
		}

		uniqueID := make([]byte, 8)
		rand.Read(uniqueID)
		fileID := fmt.Sprintf("B%v.%08x.%x", manifest.SourceID(), math.MaxInt64-manifest.StartTime.UnixNano(), uniqueID)

		if err := bgen.Backup(&manifest, oldManifest); err != nil {
			return err
		}

		err = vlt.Put(fileID, &manifest)
		if err != nil {
			return fmt.Errorf("cannot save manifest: %vlt", err)
		}

		log.Printf("Root: %vlt", manifest.RootObjectID)
	}

	return nil
}

func getBackupUser() string {
	if *backupUser != "" {
		return *backupUser
	}

	currentUser, err := user.Current()
	if err != nil {
		log.Fatalf("Cannot determine current user: %s", err)
	}

	return currentUser.Username
}

func getBackupHostName() string {
	if *backupHostName != "" {
		return *backupHostName
	}

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("Unable to determine hostname: %s", err)
	}

	// Normalize hostname.
	hostname = strings.ToLower(strings.Split(hostname, ".")[0])

	return hostname
}

func init() {
	backupCommand.Action(runBackupCommand)
}
