package main

import (
	"io"
	"log"
	"time"
	"unsafe"

	"github.com/kopia/kopia/blob"
	"github.com/kopia/kopia/cas"
	"github.com/kopia/kopia/fs"
)

type highLatencyStorage struct {
	blob.Storage

	readDelay  time.Duration
	writeDelay time.Duration
}

func (hls *highLatencyStorage) PutBlock(id blob.BlockID, data io.ReadCloser, options blob.PutOptions) error {
	go func() {
		time.Sleep(hls.writeDelay)
		hls.Storage.PutBlock(id, data, options)
	}()

	return nil
}

func (hls *highLatencyStorage) GetBlock(id blob.BlockID) ([]byte, error) {
	time.Sleep(hls.readDelay)
	return hls.Storage.GetBlock(id)
}

func uploadAndTime(omgr cas.ObjectManager, dir string, previous cas.ObjectID) cas.ObjectID {
	log.Println("---")
	uploader, err := fs.NewUploader(omgr)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	omgr.ResetStats()
	t0 := time.Now()
	oid, err := uploader.UploadDir(dir, previous)
	if err != nil {
		log.Fatalf("Error uploading: %v", err)
	}
	dt := time.Since(t0)

	log.Printf("Uploaded: %v in %v", oid, dt)
	log.Printf("Stats: %#v", omgr.Stats())
	return oid
}

func main() {
	var e fs.Entry

	log.Println(unsafe.Sizeof(e))

	data := map[string][]byte{}
	st := blob.NewMapStorage(data)

	st = &highLatencyStorage{
		Storage:    st,
		writeDelay: 1 * time.Millisecond,
		readDelay:  5 * time.Millisecond,
	}
	format := cas.Format{
		Version: "1",
		Hash:    "md5",
	}

	omgr, err := cas.NewObjectManager(st, format)
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}

	oid := uploadAndTime(omgr, "/Users/jarek/Projects/Kopia", "")
	uploadAndTime(omgr, "/Users/jarek/Projects/Kopia", "")
	uploadAndTime(omgr, "/Users/jarek/Projects/Kopia", oid)
}
