package backup

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"path/filepath"
	"time"

	"github.com/kopia/kopia/fs"

	"github.com/kopia/kopia/blob"
	"github.com/kopia/kopia/cas"
)

var (
	zeroByte = []byte{0}
)

// Generator allows creation of backups.
type Generator interface {
	Backup(m *Manifest) error
}

type backupGenerator struct {
	omgr cas.ObjectManager
}

func (bg *backupGenerator) Backup(m *Manifest) error {
	uploader, err := fs.NewUploader(bg.omgr)
	if err != nil {
		return err
	}

	if m.Alias == "" {
		m.Alias = filepath.Base(m.SourceDirectory)
	}

	h := sha1.New()
	io.WriteString(h, m.HostName)
	h.Write(zeroByte)
	io.WriteString(h, m.UserName)
	h.Write(zeroByte)
	io.WriteString(h, m.Alias)
	h.Write(zeroByte)

	backupSetID := "B" + hex.EncodeToString(h.Sum(nil))
	st := bg.omgr.Storage()
	hashCacheID := cas.NullObjectID
	for b := range st.ListBlocks(blob.BlockID(backupSetID + ".")) {
		log.Printf("Found block: %v", b)
		if bd, err := st.GetBlock(b.BlockID); err == nil {
			var oldManifest Manifest
			if err := json.Unmarshal(bd, &oldManifest); err == nil {
				log.Printf("Old manifest: %#v", oldManifest)
				hashCacheID = cas.ObjectID(oldManifest.HashCacheID)
				break
			}
		}
	}

	m.StartTime = time.Now()
	r, err := uploader.UploadDir(m.SourceDirectory, hashCacheID)
	if err != nil {
		return err
	}

	m.RootObjectID = string(r.ObjectID)
	m.HashCacheID = string(r.ManifestID)
	m.EndTime = time.Now()

	blockID := blob.BlockID(fmt.Sprintf("%v.%08x", backupSetID, math.MaxInt64-m.StartTime.UnixNano()))
	buf := bytes.NewBuffer(nil)
	json.NewEncoder(buf).Encode(&m)
	st.PutBlock(blockID, ioutil.NopCloser(buf), blob.PutOptions{})

	return nil
}

// NewGenerator creates new backup generator.
func NewGenerator(omgr cas.ObjectManager) (Generator, error) {
	return &backupGenerator{
		omgr: omgr,
	}, nil
}
