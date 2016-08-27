package upload

import (
	"bufio"
	"fmt"
	"io"

	"github.com/kopia/kopia/internal/jsonstream"
	"github.com/kopia/kopia/repo"
)

var hashCacheStreamType = "kopia:hashcache"

// hashCacheEntry represents an entry in hash cache database about single file or directory.
type hashCacheEntry struct {
	Name     string        `json:"name,omitempty"`
	Hash     uint64        `json:"hash,omitempty"`
	ObjectID repo.ObjectID `json:"oid,omitempty"`
}

type hashcacheReader struct {
	reader    *jsonstream.Reader
	nextEntry *hashCacheEntry
	entry0    hashCacheEntry
	entry1    hashCacheEntry
	odd       bool
	first     bool
}

func (hcr *hashcacheReader) open(r io.Reader) error {
	jsr, err := jsonstream.NewReader(bufio.NewReader(r), hashCacheStreamType)
	if err != nil {
		return err
	}
	hcr.reader = jsr
	hcr.nextEntry = nil
	hcr.first = true
	hcr.readahead()
	return nil
}

func (hcr *hashcacheReader) findEntry(relativeName string) *hashCacheEntry {
	for hcr.nextEntry != nil && isLess(hcr.nextEntry.Name, relativeName) {
		hcr.readahead()
	}

	if hcr.nextEntry != nil && relativeName == hcr.nextEntry.Name {
		e := hcr.nextEntry
		hcr.nextEntry = nil
		hcr.readahead()
		return e
	}

	return nil
}

func (hcr *hashcacheReader) readahead() {
	if hcr.reader != nil {
		hcr.nextEntry = nil
		e := hcr.nextManifestEntry()
		*e = hashCacheEntry{}
		if err := hcr.reader.Read(e); err == nil {
			hcr.nextEntry = e
		}
	}

	if hcr.nextEntry == nil {
		hcr.reader = nil
	}
}

func (hcr *hashcacheReader) nextManifestEntry() *hashCacheEntry {
	hcr.odd = !hcr.odd
	if hcr.odd {
		return &hcr.entry1
	}

	return &hcr.entry0
}

type hashcacheWriter struct {
	writer          *jsonstream.Writer
	lastNameWritten string
}

func newHashCacheWriter(w io.Writer) *hashcacheWriter {
	hcw := &hashcacheWriter{
		writer: jsonstream.NewWriter(w, hashCacheStreamType),
	}
	return hcw
}

func (hcw *hashcacheWriter) WriteEntry(e hashCacheEntry) error {
	if err := e.ObjectID.Validate(); err != nil {
		panic("invalid object ID: " + err.Error())
	}

	if hcw.lastNameWritten != "" {
		if isLessOrEqual(e.Name, hcw.lastNameWritten) {
			return fmt.Errorf("out-of-order directory entry, previous '%v' current '%v'", hcw.lastNameWritten, e.Name)
		}
		hcw.lastNameWritten = e.Name
	}

	hcw.writer.Write(&e)

	return nil
}

func (hcw *hashcacheWriter) Finalize() error {
	return hcw.writer.Finalize()
}
