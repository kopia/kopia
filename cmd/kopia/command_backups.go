package main

import (
	"fmt"
	"log"
	"path/filepath"

	"github.com/kopia/kopia/fs/repofs"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/vault"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	backupsCommand    = app.Command("backups", "List history of file or directory backups.")
	backupsPath       = backupsCommand.Arg("source", "File or directory to show history of.").String()
	maxResultsPerPath = backupsCommand.Flag("maxresults", "Maximum number of results.").Default("100").Int()
)

func findBackups(vlt *vault.Vault, path string) ([]string, string, error) {
	var relPath string

	for len(path) > 0 {
		sourceInfo := repofs.SnapshotSourceInfo{
			Path:     path,
			Host:     getBackupHostName(),
			UserName: getBackupUser(),
		}

		prefix := sourceInfo.HashString() + "."

		list, err := vlt.List("B" + prefix)
		if err != nil {
			return nil, "", err
		}

		if len(list) > 0 {
			return list, relPath, nil
		}

		if len(relPath) > 0 {
			relPath = filepath.Base(path) + "/" + relPath
		} else {
			relPath = filepath.Base(path)
		}

		log.Printf("No backups of %v@%v:%v", sourceInfo.UserName, sourceInfo.Host, sourceInfo.Path)

		parent := filepath.Dir(path)
		if parent == path {
			break
		}
		path = parent
	}

	return nil, "", nil
}

func runBackupsCommand(context *kingpin.ParseContext) error {
	conn := mustOpenConnection()
	defer conn.Close()

	var previous []string
	var relPath string
	var err error

	if *backupsPath != "" {
		path, err := filepath.Abs(*backupsPath)
		if err != nil {
			return fmt.Errorf("invalid directory: '%s': %s", *backupsPath, err)
		}

		previous, relPath, err = findBackups(conn.Vault, filepath.Clean(path))
		if relPath != "" {
			relPath = "/" + relPath
		}
	} else {
		previous, err = conn.Vault.List("B")
	}

	if err != nil {
		return fmt.Errorf("cannot list backups: %v", err)
	}

	var lastSource repofs.SnapshotSourceInfo
	var count int

	for _, m := range loadBackupManifests(conn.Vault, previous) {
		if m.Source != lastSource {
			fmt.Printf("\n%v\n", m.Source)
			lastSource = m.Source
			count = 0
		}

		if count < *maxResultsPerPath {
			fmt.Printf(
				"  %v%v %v %10v %v\n",
				m.Handle,
				relPath,
				m.StartTime.Format("2006-01-02 15:04:05 MST"),
				units.BytesString(m.Stats.TotalFileSize),
				deltaBytes(m.Stats.Repository.WrittenBytes),
			)
			count++
		}
	}

	return nil
}

func deltaBytes(b int64) string {
	if b > 0 {
		return "(+" + units.BytesString(b) + ")"
	}

	return "(no change)"
}

func loadBackupManifests(vlt *vault.Vault, names []string) []*repofs.Snapshot {
	result := make([]*repofs.Snapshot, len(names))
	sem := make(chan bool, 5)

	for i, n := range names {
		sem <- true
		go func(i int, n string) {
			defer func() { <-sem }()

			m, err := loadBackupManifest(vlt, n)
			if err != nil {
				log.Printf("WARNING: Unable to parse backup manifest %v: %v", n, err)
				return
			}
			result[i] = m
		}(i, n)
	}

	for i := 0; i < cap(sem); i++ {
		sem <- true
	}
	close(sem)

	return result
}

func init() {
	backupsCommand.Action(runBackupsCommand)
	backupsCommand.Flag("host", "Override backup hostname.").StringVar(backupHostName)
	backupsCommand.Flag("user", "Override backup user.").StringVar(backupUser)
}
