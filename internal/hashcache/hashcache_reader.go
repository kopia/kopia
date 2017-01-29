package hashcache

import (
	"bufio"
	"io"

	"github.com/kopia/kopia/internal/jsonstream"
)

// Reader supports reading a stream of hash cache entries.
type Reader struct {
	reader    *jsonstream.Reader
	nextEntry *Entry
	entry0    Entry
	entry1    Entry
	odd       bool
	first     bool
}

// Open starts reading hash cache content.
func (hcr *Reader) Open(r io.Reader) error {
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

// FindEntry looks for an entry with a given name in hash cache stream and returns it or nil if not found.
func (hcr *Reader) FindEntry(relativeName string) *Entry {
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

func (hcr *Reader) readahead() {
	if hcr.reader != nil {
		hcr.nextEntry = nil
		e := hcr.nextManifestEntry()
		*e = Entry{}
		if err := hcr.reader.Read(e); err == nil {
			hcr.nextEntry = e
		}
	}

	if hcr.nextEntry == nil {
		hcr.reader = nil
	}
}

func (hcr *Reader) nextManifestEntry() *Entry {
	hcr.odd = !hcr.odd
	if hcr.odd {
		return &hcr.entry1
	}

	return &hcr.entry0
}
