package session

import (
	"io/ioutil"

	"github.com/kopia/kopia/cas"

	"github.com/kopia/kopia/blob"

	"testing"
)

func TestA(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "kopia")
	if err != nil {
		t.Errorf("can't create temp directory: %v", err)
		return
	}

	// cfg := LoadConfig("kopia.config")
	sc := blob.StorageConfiguration{
		Type: "fs",
		Config: &blob.FSStorageOptions{
			Path: tmpDir,
		},
	}

	storage, err := blob.NewStorage(sc)
	if err != nil {
		t.Errorf("cannot create storage: %v", err)
		return
	}

	sess, err := New(storage, nil)
	defer sess.Close()

	om, err := sess.InitRepository(cas.Format{
		Version:      "1",
		ObjectFormat: "sha1",
	})

	if err != nil {
		t.Errorf("unable to init object manager: %v", err)
		return
	}

	w := om.NewWriter()
	w.Write([]byte{1, 2, 3})
	x, err := w.Result(true)
	t.Logf("%v x: %v %v", tmpDir, x, err)
}
