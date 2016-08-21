package main

import (
	"bytes"
	"log"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/storage/storagetesting"
)

func main() {
	d := map[string][]byte{}

	s := storagetesting.NewMapStorage(d)
	f := repo.Format{
		Version:      1,
		MaxBlobSize:  100,
		ObjectFormat: "TESTONLY_MD5",
	}
	r, err := repo.NewRepository(s, &f)
	if err != nil {
		log.Fatalf("Err: %v", err)
	}
	w := r.NewWriter(repo.WithDescription("tester"))

	w.Write(bytes.Repeat([]byte("a"), 5000))
	result, err := w.Result(true)
	log.Printf("result: %v", result.UIString())

	log.Printf("attempting to open")

	rdr, err := r.Open(result)
	if err != nil {
		log.Fatalf("open: %v", err)
	}
	log.Printf("len: %v\n", rdr.Length())
	log.Printf("----")
	// for k, v := range d {
	// 	log.Printf("%v = %v", k, string(v))
	// }

	rdr.Close()
}
