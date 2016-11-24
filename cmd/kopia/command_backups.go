package main

import (
	"fmt"
	"log"
	"path/filepath"
	"sort"

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

func findBackups(vlt *vault.Vault, sourceInfo repofs.SnapshotSourceInfo) ([]string, string, error) {
	var relPath string

	for len(sourceInfo.Path) > 0 {
		prefix := sourceInfo.HashString() + "."

		list, err := vlt.List("B" + prefix)
		if err != nil {
			return nil, "", err
		}

		if len(list) > 0 {
			return list, relPath, nil
		}

		if len(relPath) > 0 {
			relPath = filepath.Base(sourceInfo.Path) + "/" + relPath
		} else {
			relPath = filepath.Base(sourceInfo.Path)
		}

		log.Printf("No backups of %v@%v:%v", sourceInfo.UserName, sourceInfo.Host, sourceInfo.Path)

		parentPath := filepath.Dir(sourceInfo.Path)
		if parentPath == sourceInfo.Path {
			break
		}
		sourceInfo.Path = parentPath
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
		si, err := repofs.ParseSourceSnashotInfo(
			*backupsPath,
			getHostName(),
			getUserName())
		if err != nil {
			return fmt.Errorf("invalid directory: '%s': %s", *backupsPath, err)
		}

		previous, relPath, err = findBackups(conn.Vault, si)
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

	manifests := loadBackupManifests(conn.Vault, previous)
	sort.Sort(manifestSorter(manifests))

	for _, m := range manifests {
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

type manifestSorter []*repofs.Snapshot

func (b manifestSorter) Len() int { return len(b) }
func (b manifestSorter) Less(i, j int) bool {
	if b[i].Source.String() < b[j].Source.String() {
		return true
	}
	if b[i].Source.String() > b[j].Source.String() {
		return false
	}

	return b[i].StartTime.UnixNano() < b[j].StartTime.UnixNano()
}

func (b manifestSorter) Swap(i, j int) { b[i], b[j] = b[j], b[i] }

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
}
