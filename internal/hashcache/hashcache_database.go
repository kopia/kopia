// Package hashcache implements streaming cache of file hashes.
package hashcache

import (
	"bytes"
	"compress/gzip"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/boltdb/bolt"
	"github.com/kopia/kopia/object"
)

const batchSize = 10

var hashcacheBucket = []byte("hashcache")

// DB is a databsase caching ObjectID computations for local files.
type DB struct {
	db *bolt.DB
}

// DirectoryInfo manages a set of hashes for all directory entries.
type DirectoryInfo struct {
	parent  *DB
	dirty   bool
	dirName string

	previousFiles map[string]string
	currentFiles  map[string]string
}

// NewHashCacheDB returns new hash cache database stored in a given file.
func NewHashCacheDB(filename string) (*DB, error) {
	db, err := bolt.Open(filename, 0600, nil)
	if err != nil {
		return nil, err
	}

	return &DB{db: db}, nil
}

func (h *DB) keyOf(dirName string) []byte {
	o := sha1.New()
	fmt.Fprintf(o, "%v", dirName)
	return o.Sum(nil)
}

// OpenDir returns DirectoryInfo for a single local directory.
// The caller is expected to call Lookup()/Set() for all entries in the directory before calling Save().
func (h *DB) OpenDir(dirName string) DirectoryInfo {
	var dhi DirectoryInfo

	h.db.View(func(t *bolt.Tx) error {
		b := t.Bucket(hashcacheBucket)
		if b == nil {
			return nil
		}

		v := b.Get(h.keyOf(dirName))
		if v == nil {
			return nil
		}

		gz, err := gzip.NewReader(bytes.NewReader(v))
		if err != nil {
			return nil
		}

		var files map[string]string

		if err := json.NewDecoder(gz).Decode(&files); err != nil {
			return nil
		}

		dhi.previousFiles = files

		return nil
	})

	dhi.parent = h
	dhi.dirName = dirName
	dhi.currentFiles = make(map[string]string)

	return dhi
}

func (hi *DirectoryInfo) keyOf(fi os.FileInfo) string {
	h := sha1.New()
	fmt.Fprintf(h, "%v/%v/%v @ %v", fi.ModTime().UnixNano(), int(fi.Mode()), fi.Size(), fi.Name())
	return hex.EncodeToString(h.Sum(nil))
}

// Lookup fetches the ObjectID corresponding to the given FileInfo in a directory, if present.
func (hi *DirectoryInfo) Lookup(fi os.FileInfo) (object.ID, bool) {
	k := hi.keyOf(fi)
	s, ok := hi.previousFiles[k]
	if !ok {
		return object.NullID, false
	}

	oid, err := object.ParseID(s)
	if err != nil {
		return object.NullID, false
	}

	hi.currentFiles[k] = s
	return oid, true
}

// Set associates ObjectID with a FileInfo that will be persisted on Save().
func (hi *DirectoryInfo) Set(fi os.FileInfo, oid object.ID) {
	k := hi.keyOf(fi)
	new := oid.String()
	hi.currentFiles[k] = new
	hi.dirty = true
}

// Save writes the current associations of FileInfo->ObjectID for the directory to the databsase.
// Any entries for which neither Lookup() nor Set() has been called are assumed to be removed.
func (hi *DirectoryInfo) Save() error {
	if len(hi.previousFiles) == len(hi.currentFiles) && !hi.dirty {
		return nil
	}

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)

	if err := json.NewEncoder(gz).Encode(hi.currentFiles); err != nil {
		return err
	}
	gz.Flush()

	value := buf.Bytes()

	err := hi.parent.db.Update(func(t *bolt.Tx) error {
		b := t.Bucket(hashcacheBucket)
		if b == nil {
			var err error
			b, err = t.CreateBucket(hashcacheBucket)
			if err != nil {
				return err
			}
		}

		return b.Put(hi.parent.keyOf(hi.dirName), value)
	})

	if err != nil {
		log.Printf("warning: failed to update hash cache: %v", err)
	}

	return nil
}
