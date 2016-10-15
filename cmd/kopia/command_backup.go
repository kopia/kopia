package main

import (
	"context"
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

	"github.com/kopia/kopia/fs/repofs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/vault"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

const (
	backupMaxDescriptionLength = 1024
)

var (
	backupCommand = app.Command("backup", "Copies local files or directories to backup repository.")

	backupSources = backupCommand.Arg("source", "Files or directories to back up.").ExistingFilesOrDirs()
	backupAll     = backupCommand.Flag("all", "Back-up all directories previously backed up by this user on this computer").Bool()

	backupHostName    = backupCommand.Flag("host", "Override backup hostname.").String()
	backupUser        = backupCommand.Flag("user", "Override backup user.").String()
	backupDescription = backupCommand.Flag("description", "Free-form backup description.").String()

	backupCheckpointInterval      = backupCommand.Flag("checkpoint_interval", "Periodically flush backup (default=30m).").PlaceHolder("TIME").Default("30m").Duration()
	backupCheckpointEveryMB       = backupCommand.Flag("checkpoint_every_mb", "Checkpoint backup after each N megabytes (default=1000).").PlaceHolder("N").Default("1000").Int()
	backupCheckpointUploadLimitMB = backupCommand.Flag("upload_limit_mb", "Stop the backup process after the specified amount of data (in MB) has been uploaded.").PlaceHolder("MB").Default("0").Int()

	backupWriteBack = backupCommand.Flag("async-write", "Perform updates asynchronously.").PlaceHolder("N").Default("0").Int()
)

func runBackupCommand(c *kingpin.ParseContext) error {
	var repoOptions []repo.RepositoryOption

	if *backupWriteBack > 0 {
		repoOptions = append(repoOptions, repo.WriteBack(*backupWriteBack))
	}

	conn := mustOpenConnection(repoOptions...)
	defer conn.Close()

	ctx := context.Background()

	sources := *backupSources
	if *backupAll {
		local, err := getLocalBackupPaths(conn.Vault)
		if err != nil {
			return err
		}
		sources = append(sources, local...)
	}

	if len(sources) == 0 {
		return fmt.Errorf("No backup sources.")
	}

	for _, backupDirectory := range sources {
		log.Printf("Backing up %v", backupDirectory)
		dir, err := filepath.Abs(backupDirectory)
		if err != nil {
			return fmt.Errorf("invalid source: '%s': %s", backupDirectory, err)
		}

		sourceInfo := repofs.SnapshotSourceInfo{
			Path:     filepath.Clean(dir),
			Host:     getBackupHostName(),
			UserName: getBackupUser(),
		}

		if len(*backupDescription) > backupMaxDescriptionLength {
			return fmt.Errorf("description too long")
		}

		previous, err := conn.Vault.List("B" + sourceInfo.HashString() + ".")
		if err != nil {
			return fmt.Errorf("error listing previous backups")
		}

		var oldManifest *repofs.Snapshot

		if len(previous) > 0 {
			oldManifest, err = loadBackupManifest(conn.Vault, previous[0])
		}

		localEntry := mustGetLocalFSEntry(sourceInfo.Path)
		if err != nil {
			return err
		}

		manifest, err := repofs.Upload(ctx, conn.Repository, localEntry, &sourceInfo, oldManifest)
		if err != nil {
			return err
		}

		handleID, err := conn.Vault.SaveObjectID(manifest.RootObjectID)
		if err != nil {
			return err
		}

		uniqueID := make([]byte, 8)
		rand.Read(uniqueID)
		fileID := fmt.Sprintf("B%v.%08x.%x", sourceInfo.HashString(), math.MaxInt64-manifest.StartTime.UnixNano(), uniqueID)
		manifest.Handle = handleID
		manifest.Description = *backupDescription

		err = saveBackupManifest(conn.Vault, fileID, manifest)
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

func getLocalBackupPaths(vlt *vault.Vault) ([]string, error) {
	u := getBackupUser()
	h := getBackupHostName()
	log.Printf("Looking for previous backups of '%v@%v'...", u, h)
	backupItems, err := vlt.List("B")
	if err != nil {
		return nil, err
	}

	manifests := loadBackupManifests(vlt, backupItems)
	var lastSource repofs.SnapshotSourceInfo

	var result []string

	for _, m := range manifests {
		if m.Source != lastSource {
			lastSource = m.Source

			if m.Source.Host == h && m.Source.UserName == u {
				result = append(result, m.Source.Path)
			}
		}
	}

	return result, nil
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
