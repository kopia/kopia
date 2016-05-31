package backup

import (
	"fmt"
	"os"
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
	repo repo.Repository
}

func (bg *backupGenerator) Backup(m *Manifest, old *Manifest) error {
	uploader, err := fs.NewUploader(bg.repo)
	if err != nil {
		return err
	}

	m.StartTime = time.Now()
	var hashCacheID repo.ObjectID

	if old != nil {
		hashCacheID = repo.ObjectID(old.HashCacheID)
	}

	st, err := os.Stat(m.Source)
	if err != nil {
		return err
	}

	var r *fs.UploadResult
	switch st.Mode() & os.ModeType {
	case os.ModeDir:
		r, err = uploader.UploadDir(m.Source, hashCacheID)
	case 0: // regular
		r, err = uploader.UploadFile(m.Source)
	default:
		return fmt.Errorf("unsupported source: %v", m.Source)
	}
	m.EndTime = time.Now()
	if err != nil {
		return err
	}
	m.RootObjectID = string(r.ObjectID)
	m.HashCacheID = string(r.ManifestID)

	return nil
}

// NewGenerator creates new backup generator.
func NewGenerator(repo repo.Repository) (Generator, error) {
	return &backupGenerator{
		repo: repo,
	}, nil
}
