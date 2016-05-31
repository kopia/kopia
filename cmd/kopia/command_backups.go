package main

import (
	"fmt"
	"log"
	"path/filepath"

	"github.com/kopia/kopia/backup"
	"github.com/kopia/kopia/repo"
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
		manifest := backup.Manifest{
			Source:   path,
			HostName: getBackupHostName(),
			UserName: getBackupUser(),
		}

		prefix := manifest.SourceID() + "."

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

		log.Printf("No backups of %v@%v:%v", manifest.UserName, manifest.HostName, manifest.Source)

		parent := filepath.Dir(path)
		if parent == path {
			break
		}
		path = parent
	}

	return nil, "", nil
}

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

	var previous []string
	var relPath string

	if *backupsPath != "" {
		path, err := filepath.Abs(*backupsPath)
		if err != nil {
			return fmt.Errorf("invalid directory: '%s': %s", *backupsPath, err)
		}

		previous, relPath, err = findBackups(vlt, filepath.Clean(path))
		if relPath != "" {
			relPath = "/" + relPath
		}
	} else {
		previous, err = vlt.List("B")
	}

	if err != nil {
		return fmt.Errorf("cannot list backups: %v", err)
	}

	var lastHost string
	var lastUser string
	var lastSource string
	var count int

	for _, n := range previous {
		var m backup.Manifest
		if err := vlt.Get(n, &m); err != nil {
			return fmt.Errorf("error loading previous backup: %v", err)
		}

		if m.HostName != lastHost || m.UserName != lastUser || m.Source != lastSource {
			log.Printf("%v@%v:%v", m.UserName, m.HostName, m.Source)
			lastSource = m.Source
			lastUser = m.UserName
			lastHost = m.HostName
			count = 0
		}

		if count < *maxResultsPerPath {
			log.Printf("  %v%v %v", m.Handle, relPath, m.StartTime.Format("2006-01-02 15:04:05 MST"))
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
