package main

import (
	"context"
	"crypto/rand"
	"io/ioutil"
	"os"

	"github.com/kopia/repo"
	"github.com/kopia/repo/object"
)

func uploadRandomObject(ctx context.Context, r *repo.Repository, length int) (object.ID, error) {
	w := r.Objects.NewWriter(ctx, object.WriterOptions{})
	defer w.Close() //nolint:errcheck

	buf := make([]byte, 256*1024)
	for length > 0 {
		todo := length
		if todo > len(buf) {
			todo = len(buf)
		}
		rand.Read(buf[0:todo]) //nolint:errcheck
		if _, err := w.Write(buf[0:todo]); err != nil {
			return "", err
		}
		length -= todo
	}
	return w.Result()
}

func downloadObject(ctx context.Context, r *repo.Repository, oid object.ID) ([]byte, error) {
	rd, err := r.Objects.Open(ctx, oid)
	if err != nil {
		return nil, err
	}
	defer rd.Close() //nolint:errcheck

	return ioutil.ReadAll(rd)
}

func uploadAndDownloadObjects(ctx context.Context, r *repo.Repository) {
	var oids []object.ID

	for size := 100; size < 100000000; size *= 2 {
		log.Debugf("uploading file with %v bytes", size)
		oid, err := uploadRandomObject(ctx, r, size)
		if err != nil {
			log.Errorf("unable to upload: %v", err)
			os.Exit(1)
		}
		log.Debugf("uploaded %v bytes as %v", size, oid)
		oids = append(oids, oid)
	}

	for _, oid := range oids {
		log.Infof("downloading %q", oid)
		b, err := downloadObject(ctx, r, oid)
		if err != nil {
			log.Errorf("unable to read object: %v", err)
		}
		log.Debugf("downloaded %v", len(b))
	}
}
