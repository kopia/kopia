package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
	"github.com/kopia/kopia/fs/repofs"
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
	var repoOptions []repo.RepositoryOption

	if *backupWriteBack > 0 {
		repoOptions = append(repoOptions, repo.WriteBack(*backupWriteBack))
	}

	conn := mustOpenConnection(repoOptions...)
	defer conn.Close()

	var options []repofs.UploadOption

	bgen, err := backup.NewGenerator(conn.Repository, options...)
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

		previous, err := conn.Vault.List("B" + manifest.SourceID() + ".")
		if err != nil {
			return fmt.Errorf("error listing previous backups")
		}

		var oldManifest *backup.Manifest

		if len(previous) > 0 {
			oldManifest, err = loadBackupManifest(conn.Vault, previous[0])
		}

		localEntry := mustGetLocalFSEntry(manifest.Source)
		if err != nil {
			return err
		}

		if err := bgen.Backup(localEntry, &manifest, oldManifest); err != nil {
			return err
		}

		handleID, err := conn.Vault.SaveObjectID(manifest.RootObjectID)
		if err != nil {
			return err
		}

		uniqueID := make([]byte, 8)
		rand.Read(uniqueID)
		fileID := fmt.Sprintf("B%v.%08x.%x", manifest.SourceID(), math.MaxInt64-manifest.StartTime.UnixNano(), uniqueID)
		manifest.Handle = handleID

		err = saveBackupManifest(conn.Vault, fileID, &manifest)
		if err != nil {
			return fmt.Errorf("cannot save manifest: %v", err)
		}

		log.Printf("Root: %v", manifest.RootObjectID.String())
		log.Printf("Hash Cache: %v", manifest.HashCacheID.String())
		log.Printf("Key: %v", handleID)

		b, _ := json.MarshalIndent(&manifest, "", "  ")
		log.Printf("%s", string(b))
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
