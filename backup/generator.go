package backup

import (
	"log"
	"time"

	"github.com/kopia/kopia/fs"

	"github.com/kopia/kopia/cas"
)

var (
	zeroByte = []byte{0}
)

// Generator allows creation of backups.
type Generator interface {
	Backup(m *Manifest, old *Manifest) error
}

type backupGenerator struct {
	repo cas.Repository
}

func (bg *backupGenerator) Backup(m *Manifest, old *Manifest) error {
	uploader, err := fs.NewUploader(bg.repo)
	if err != nil {
		return err
	}

	m.StartTime = time.Now()
	var hashCacheID cas.ObjectID

	if old != nil {
		hashCacheID = cas.ObjectID(old.HashCacheID)
		log.Printf("Using hash cache ID: %v", hashCacheID)
	} else {
		log.Printf("No hash cache.")
	}
	r, err := uploader.UploadDir(m.SourceDirectory, hashCacheID)
	if err != nil {
		return err
	}

	m.RootObjectID = string(r.ObjectID)
	m.HashCacheID = string(r.ManifestID)
	m.EndTime = time.Now()

	return nil
}

// NewGenerator creates new backup generator.
func NewGenerator(repo cas.Repository) (Generator, error) {
	return &backupGenerator{
		repo: repo,
	}, nil
}
