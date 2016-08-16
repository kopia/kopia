package main

import (
	"bufio"
	"bytes"
	"log"

	"github.com/kopia/kopia/internal"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/storage/storagetesting"
)

func dumpListBlock(prefix string, r repo.Repository, oid repo.ObjectID) {
	ind := oid.Indirect
	oid.Indirect = 0
	log.Printf("%vdumping %v", prefix, oid.String())
	rd, err := r.Open(oid)
	if err != nil {
		log.Fatalf("unable to open %v: %v", oid.String(), err)
	}
	defer rd.Close()

	var objects []repo.ObjectID

	pr := internal.NewProtoStreamReader(bufio.NewReader(rd), internal.ProtoStreamTypeIndirect)
	for {
		var v repo.IndirectObjectEntry
		if err := pr.Read(&v); err != nil {
			log.Printf("err: %v", err)
			break
		}
		log.Printf("%v [%v] entry: %v", prefix, oid.String(), v.String())
		if ind > 0 {
			objects = append(objects, *v.Object)
		}
	}

	for _, o := range objects {
		dumpListBlock(prefix+"  ", r, o)
	}
}

func main() {
	d := map[string][]byte{}

	s := storagetesting.NewMapStorage(d)
	f := repo.Format{
		Version:      1,
		MaxBlobSize:  100,
		ObjectFormat: repo.ObjectIDFormat_TESTONLY_MD5,
	}
	r, err := repo.NewRepository(s, &f)
	if err != nil {
		log.Fatalf("Err: %v", err)
	}
	w := r.NewWriter(repo.WithDescription("tester"))

	w.Write(bytes.Repeat([]byte("a"), 5000))
	result, err := w.Result(true)
	log.Printf("result: %v", result.UIString())

	//dumpListBlock("", r, result)

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
