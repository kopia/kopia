package backup

import "github.com/kopia/kopia/cas"

type Generator interface {
	Backup(m *Manifest) error
}

type backupGenerator struct {
	omgr cas.ObjectManager
}

func (bg *backupGenerator) Backup(m *Manifest) error {
	return nil
}

func NewGenerator(omgr cas.ObjectManager) (Generator, error) {
	return &backupGenerator{
		omgr: omgr,
	}, nil
}
