package backup

import (
	"fmt"
	"time"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/repofs"
	"github.com/kopia/kopia/repo"
)

var (
	zeroByte = []byte{0}
)

// Generator allows creation of backups.
type Generator struct {
	repo    *repo.Repository
	options []repofs.UploadOption
}

// Backup uploads contents of the specified filesystem entry (file or directory) to the repository and updates given manifest with statistics.
// Old manifest, when provided can be used to speed up backups by utilizing hash cache.
func (bg *Generator) Backup(entry fs.Entry, m *Manifest, old *Manifest) error {
	uploader := repofs.NewUploader(bg.repo, bg.options...)

	m.StartTime = time.Now()
	var hashCacheID *repo.ObjectID

	if old != nil {
		hashCacheID = &old.HashCacheID
	}

	var r *repofs.UploadResult
	var err error
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
func NewGenerator(repo *repo.Repository, options ...repofs.UploadOption) (*Generator, error) {
	return &Generator{
		repo:    repo,
		options: options,
	}, nil
}
