package backup

import (
	"fmt"
	"time"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo"
)

var (
	zeroByte = []byte{0}
)

// Generator allows creation of backups.
type Generator interface {
	Backup(m *Manifest, old *Manifest) error
}

type backupGenerator struct {
	repo    *repo.Repository
	options []fs.UploaderOption
}

func (bg *backupGenerator) Backup(m *Manifest, old *Manifest) error {
	uploader := fs.NewUploader(bg.repo, bg.options...)

	m.StartTime = time.Now()
	var hashCacheID *repo.ObjectID

	if old != nil {
		hashCacheID = &old.HashCacheID
	}

	entry, err := fs.NewFilesystemEntry(m.Source, nil)

	var r *fs.UploadResult
	switch entry := entry.(type) {
	case fs.Directory:
		r, err = uploader.UploadDir(entry, hashCacheID)
	case fs.File:
		r, err = uploader.UploadFile(entry)
	default:
		return fmt.Errorf("unsupported source: %v", m.Source)
	}
	m.EndTime = time.Now()
	if err != nil {
		return err
	}
	m.RootObjectID = r.ObjectID
	m.HashCacheID = r.ManifestID
	s := bg.repo.Stats
	m.Stats = &s

	return nil
}

// NewGenerator creates new backup generator.
func NewGenerator(repo *repo.Repository, options ...fs.UploaderOption) (Generator, error) {
	return &backupGenerator{
		repo:    repo,
		options: options,
	}, nil
}
