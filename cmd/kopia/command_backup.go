package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/kopia/kopia/backup"
	"github.com/kopia/kopia/repo"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

const (
	backupMaxDescriptionLength = 1024
)

var (
	backupCommand = app.Command("backup", "Copies local files or directories to backup repository.")

	backupSources = backupCommand.Arg("source", "Files or directories to back up.").Required().ExistingFilesOrDirs()

	backupHostName    = backupCommand.Flag("host", "Override backup hostname.").String()
	backupUser        = backupCommand.Flag("user", "Override backup user.").String()
	backupDescription = backupCommand.Flag("description", "Free-form backup description.").String()

	backupCheckpointInterval      = backupCommand.Flag("checkpoint_interval", "Periodically flush backup (default=30m).").PlaceHolder("TIME").Default("30m").Duration()
	backupCheckpointEveryMB       = backupCommand.Flag("checkpoint_every_mb", "Checkpoint backup after each N megabytes (default=1000).").PlaceHolder("N").Default("1000").Int()
	backupCheckpointUploadLimitMB = backupCommand.Flag("upload_limit_mb", "Stop the backup process after the specified amount of data (in MB) has been uploaded.").PlaceHolder("MB").Default("0").Int()

	backupWriteBack = backupCommand.Flag("async-write", "Perform updates asynchronously.").PlaceHolder("N").Default("0").Int()
)

func runBackupCommand(context *kingpin.ParseContext) error {
	var options []repo.RepositoryOption

	if *backupWriteBack > 0 {
		options = append(options, repo.WriteBack(*backupWriteBack))
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

	for _, backupDirectory := range *backupSources {
		dir, err := filepath.Abs(backupDirectory)
		if err != nil {
			return fmt.Errorf("invalid source: '%s': %s", backupDirectory, err)
		}

		manifest := backup.Manifest{
			StartTime: time.Now(),
			Source:    filepath.Clean(dir),

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
			oldManifest, err = loadBackupManifest(vlt, previous[0])
		}

		if err := bgen.Backup(&manifest, oldManifest); err != nil {
			return err
		}

		handleID, err := vlt.SaveObjectID(repo.ObjectID(manifest.RootObjectID))
		if err != nil {
			return err
		}

		uniqueID := make([]byte, 8)
		rand.Read(uniqueID)
		fileID := fmt.Sprintf("B%v.%08x.%x", manifest.SourceID(), math.MaxInt64-manifest.StartTime.UnixNano(), uniqueID)
		manifest.Handle = handleID

		err = saveBackupManifest(vlt, fileID, &manifest)
		if err != nil {
			return fmt.Errorf("cannot save manifest: %v", err)
		}

		log.Printf("Root: %v", manifest.RootObjectID.UIString())
		log.Printf("Key: %v", handleID)
	}

	return nil
}

func hashObjectID(oid string) string {
	h := sha256.New()
	io.WriteString(h, oid)
	sum := h.Sum(nil)
	foldLen := 16
	for i := foldLen; i < len(sum); i++ {
		sum[i%foldLen] ^= sum[i]
	}
	return hex.EncodeToString(sum[0:foldLen])
}

func getBackupUser() string {
	if *backupUser != "" {
		return *backupUser
	}

	currentUser, err := user.Current()
	if err != nil {
		log.Fatalf("Cannot determine current user: %s", err)
	}

	u := currentUser.Username
	if runtime.GOOS == "windows" {
		if p := strings.Index(u, "\\"); p >= 0 {
			// On Windows ignore domain name.
			u = u[p+1:]
		}
	}

	return u
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
