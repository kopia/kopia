package fs

import (
	"bufio"
	"fmt"
	"io"

	"github.com/kopia/kopia/internal"
)

type hashcacheReader struct {
	reader    *internal.ProtoStreamReader
	nextEntry *HashCacheEntry
	entry0    HashCacheEntry
	entry1    HashCacheEntry
	odd       bool
	first     bool
}

func (hcr *hashcacheReader) open(r io.Reader) error {
	hcr.reader = internal.NewProtoStreamReader(bufio.NewReader(r), internal.ProtoStreamTypeHashCache)
	hcr.nextEntry = nil
	hcr.first = true
	hcr.readahead()
	return nil
}

func (hcr *hashcacheReader) findEntry(relativeName string) *HashCacheEntry {
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
		if err := hcr.reader.Read(e); err == nil {
			hcr.nextEntry = e
		}
	}

	if hcr.nextEntry == nil {
		hcr.reader = nil
	}
}

func (hcr *hashcacheReader) nextManifestEntry() *HashCacheEntry {
	hcr.odd = !hcr.odd
	if hcr.odd {
		return &hcr.entry1
	}

	return &hcr.entry0
}

type hashcacheWriter struct {
	writer          *internal.ProtoStreamWriter
	lastNameWritten string
}

func newHashCacheWriter(w io.Writer) *hashcacheWriter {
	hcw := &hashcacheWriter{
		writer: internal.NewProtoStreamWriter(w, internal.ProtoStreamTypeHashCache),
	}
	return hcw
}

func (hcw *hashcacheWriter) WriteEntry(e HashCacheEntry) error {
	if hcw.lastNameWritten != "" {
		if isLessOrEqual(e.Name, hcw.lastNameWritten) {
			return fmt.Errorf("out-of-order directory entry, previous '%v' current '%v'", hcw.lastNameWritten, e.Name)
		}
		hcw.lastNameWritten = e.Name
	}

	hcw.writer.Write(&e)

	return nil
}
