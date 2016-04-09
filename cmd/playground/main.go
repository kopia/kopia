package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/kopia/kopia/blob"
	"github.com/kopia/kopia/cas"
	"github.com/kopia/kopia/fs"
)

const (
	maxWorkerCount = 3
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

func uploadAndTime(omgr cas.ObjectManager, dir string, previous cas.ObjectID) (cas.ObjectID, cas.ObjectID) {
	log.Println("---")
	uploader, err := fs.NewUploader(omgr)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	omgr.ResetStats()
	t0 := time.Now()
	oid, manifestOID, err := uploader.UploadDir(dir, previous)
	if err != nil {
		log.Fatalf("Error uploading: %v", err)
	}
	dt := time.Since(t0)

	log.Printf("Uploaded: %v in %v", oid, dt)
	log.Printf("Stats: %#v", omgr.Stats())
	return oid, manifestOID
}

type subdirEntry struct {
	entry      fs.Entry
	dirChannel chan fs.Directory
}

var parallelReads int32

type gantt struct {
	dir  string
	from time.Time
	to   time.Time
}

var allGantt []*gantt
var ganttMutex sync.Mutex

func walkTree2(ch chan fs.Entry, omgr cas.ObjectManager, path string, dir fs.Directory) {
	//log.Printf("walkTree2(%s)", path)
	m := map[int]chan fs.Directory{}

	// Channel containing channels with subdirectory contents, one for each subdirectory in 'dir'.
	subdirChannels := make(chan *subdirEntry, 10)

	subdirCount := 0
	for i, e := range dir {
		if e.IsDir() {
			m[i] = make(chan fs.Directory, 10)
			subdirCount++
		}
	}

	workerCount := subdirCount
	if workerCount > maxWorkerCount {
		workerCount = maxWorkerCount
	}
	for i := 0; i < workerCount; i++ {
		go func(id int) {
			for {
				//log.Printf("worker %v waiting for work", id)
				se, ok := <-subdirChannels
				if !ok {
					//log.Printf("worker %v quitting", id)
					break
				}
				defer close(se.dirChannel)

				//log.Printf("worker %v loading %v", id, se.entry.ObjectID())
				//defer close(se.dirChannel)

				d, err := omgr.Open(se.entry.ObjectID())
				if err != nil {
					log.Printf("ERROR: %v", err)
					return
				}

				//log.Printf("loading directory %v with prefix %v", se.entry.Name(), path +"/" + se.)

				g := &gantt{}
				g.from = time.Now()

				atomic.AddInt32(&parallelReads, 1)
				dir, err := fs.ReadDirectory(d, se.entry.Name()+"/")
				atomic.AddInt32(&parallelReads, -1)
				g.to = time.Now()
				g.dir = se.entry.Name()

				ganttMutex.Lock()
				allGantt = append(allGantt, g)
				ganttMutex.Unlock()

				if err != nil {
					log.Printf("ERROR: %v", err)
					return
				}

				se.dirChannel <- dir
			}
		}(i)
	}

	go func() {
		for i, e := range dir {
			if m[i] != nil {
				subdirChannels <- &subdirEntry{
					entry:      e,
					dirChannel: m[i],
				}
			}
		}
		close(subdirChannels)
	}()

	for i, e := range dir {
		//log.Printf("%v[%v] = %v", path, i, e.Name())
		if e.IsDir() {
			subdir := <-m[i]
			walkTree2(ch, omgr, e.Name()+"/", subdir)
		}
		ch <- e
	}
}

func walkTree(omgr cas.ObjectManager, path string, oid cas.ObjectID) chan fs.Entry {
	ch := make(chan fs.Entry, 20)
	go func() {
		d, err := omgr.Open(oid)
		defer close(ch)
		if err != nil {
			log.Printf("ERROR: %v", err)
			return
		}

		dir, err := fs.ReadDirectory(d, path)
		if err != nil {
			log.Printf("ERROR: %v", err)
			return
		}

		walkTree2(ch, omgr, path, dir)
	}()
	return ch
}

func readCached(omgr cas.ObjectManager, manifestOID cas.ObjectID) {
	r, err := omgr.Open(manifestOID)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	v, _ := ioutil.ReadAll(r)

	//fmt.Println(string(v))
	fmt.Printf("len: %v\n", len(v))
}

func main() {
	var e fs.Entry

	log.Println(unsafe.Sizeof(e))

	data := map[string][]byte{}
	st := blob.NewMapStorage(data)

	st = &highLatencyStorage{
		Storage:    st,
		writeDelay: 1 * time.Millisecond,
		readDelay:  10 * time.Millisecond,
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

	oid, manifestOID := uploadAndTime(omgr, "/Users/jarek/Projects/Kopia", "")
	readCached(omgr, manifestOID)
	// uploadAndTime(omgr, "/Users/jarek/Projects/Kopia", "")
	// uploadAndTime(omgr, "/Users/jarek/Projects/Kopia", oid)
	time.Sleep(1 * time.Second)
	for i := 0; i < 1; i++ {
		t0 := time.Now()
		c := 0
		d := 0
		allGantt = nil
		for e := range walkTree(omgr, "BASE/", oid) {
			//log.Printf("e: %v %v", e.Name(), e.ObjectID())
			if e.IsDir() {
				d++
			}
			c++
		}
		dt := time.Since(t0)
		log.Printf("walk took %v and returned %v (%v dirs)", dt, c, d)
		// for _, e := range allGantt {
		// 	fmt.Printf("%v,%v,%v\n", e.dir, e.from.Sub(t0).Nanoseconds()/1000, e.to.Sub(e.from).Nanoseconds()/1000)
		// }
	}
}
