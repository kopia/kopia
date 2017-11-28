package cli

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/kopia/kopia/policy"

	"github.com/kopia/kopia/repo"

	"github.com/kopia/kopia/object"

	"github.com/kopia/kopia/snapshot"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

const (
	maxSnapshotDescriptionLength = 1024
)

var (
	snapshotCreateCommand = snapshotCommands.Command("create", "Creates a snapshot of local directory or file.")

	snapshotCreateSources                 = snapshotCreateCommand.Arg("source", "Files or directories to create snapshot(s) of.").ExistingFilesOrDirs()
	snapshotCreateAll                     = snapshotCreateCommand.Flag("all", "Create snapshots for files or directories previously backed up by this user on this computer").Bool()
	snapshotCreateCheckpointUploadLimitMB = snapshotCreateCommand.Flag("upload-limit-mb", "Stop the backup process after the specified amount of data (in MB) has been uploaded.").PlaceHolder("MB").Default("0").Int64()
	snapshotCreateDescription             = snapshotCreateCommand.Flag("description", "Free-form snapshot description.").String()
	snapshotCreateForceHash               = snapshotCreateCommand.Flag("force-hash", "Force hashing of source files for a given percentage of files [0..100]").Default("0").Int()
	snapshotCreateHashCacheMinAge         = snapshotCreateCommand.Flag("hash-cache-min-age", "Do not hash-cache files below certain age").Default("1h").Duration()
	snapshotCreateWriteBack               = snapshotCreateCommand.Flag("async-write", "Perform updates asynchronously.").PlaceHolder("N").Default("0").Int()
)

func runBackupCommand(c *kingpin.ParseContext) error {
	rep := mustOpenRepository(&repo.Options{
		ObjectManagerOptions: object.ManagerOptions{
			WriteBack: *snapshotCreateWriteBack,
		},
	})
	defer rep.Close()

	mgr := snapshot.NewManager(rep)
	pmgr := policy.NewManager(rep)

	sources := *snapshotCreateSources
	if *snapshotCreateAll {
		local, err := getLocalBackupPaths(mgr)
		if err != nil {
			return err
		}
		sources = append(sources, local...)
	}

	if len(sources) == 0 {
		return errors.New("no backup sources")
	}

	u := snapshot.NewUploader(rep)
	u.MaxUploadBytes = *snapshotCreateCheckpointUploadLimitMB * 1024 * 1024
	u.ForceHashPercentage = *snapshotCreateForceHash
	u.HashCacheMinAge = *snapshotCreateHashCacheMinAge
	onCtrlC(u.Cancel)

	u.Progress = &uploadProgress{}

	for _, backupDirectory := range sources {
		rep.Blocks.ResetStats()
		log.Printf("Backing up %v", backupDirectory)
		dir, err := filepath.Abs(backupDirectory)
		if err != nil {
			return fmt.Errorf("invalid source: '%s': %s", backupDirectory, err)
		}

		sourceInfo := snapshot.SourceInfo{Path: filepath.Clean(dir), Host: getHostName(), UserName: getUserName()}
		policy, err := pmgr.GetEffectivePolicy(sourceInfo.UserName, sourceInfo.Host, sourceInfo.Path)
		if err != nil {
			return fmt.Errorf("unable to get backup policy for source %v: %v", sourceInfo, err)
		}

		if len(*snapshotCreateDescription) > maxSnapshotDescriptionLength {
			return fmt.Errorf("description too long")
		}

		previous, err := mgr.ListSnapshots(sourceInfo)
		if err != nil {
			return fmt.Errorf("error listing previous backups: %v", err)
		}

		var oldManifest *snapshot.Manifest

		if len(previous) > 0 {
			oldManifest = previous[0]
		}

		localEntry := mustGetLocalFSEntry(sourceInfo.Path)
		if err != nil {
			return err
		}

		u.FilesPolicy = policy.FilesPolicy

		manifest, err := u.Upload(localEntry, sourceInfo, oldManifest)
		if err != nil {
			return err
		}

		manifest.Description = *snapshotCreateDescription

		if _, err := mgr.SaveSnapshot(manifest); err != nil {
			return fmt.Errorf("cannot save manifest: %v", err)
		}

		log.Printf("Root: %v", manifest.RootObjectID.String())
		log.Printf("Hash Cache: %v", manifest.HashCacheID.String())

		b, _ := json.MarshalIndent(&manifest, "", "  ")
		log.Printf("%s", string(b))
	}

	return nil
}

func getLocalBackupPaths(mgr *snapshot.Manager) ([]string, error) {
	h := getHostName()
	u := getUserName()
	log.Printf("Looking for previous backups of '%v@%v'...", u, h)

	sources := mgr.ListSources()

	var result []string

	for _, src := range sources {
		if src.Host == h && src.UserName == u {
			result = append(result, src.Path)
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

func getUserOrDefault(userName string) string {
	if userName != "" {
		return userName
	}

	return getUserName()
}

func getUserName() string {
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

func getHostNameOrDefault(hostName string) string {
	if hostName != "" {
		return hostName
	}

	return getHostName()
}

func getHostName() string {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("Unable to determine hostname: %s", err)
	}

	// Normalize hostname.
	hostname = strings.ToLower(strings.Split(hostname, ".")[0])

	return hostname
}

func init() {
	snapshotCreateCommand.Action(runBackupCommand)
}
