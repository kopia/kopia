package hashcache

import (
	"bufio"
	"io"

	"github.com/kopia/kopia/internal/jsonstream"
	"github.com/kopia/kopia/internal/kopialogging"
)

var log = kopialogging.Logger("kopia/hashcache")

// Reader supports reading a stream of hash cache entries.
type Reader interface {
	FindEntry(relativeName string) *Entry
	CopyTo(w Writer) error
}

type reader struct {
	reader    *jsonstream.Reader
	nextEntry *Entry
	entry0    Entry
	entry1    Entry
	odd       bool
	first     bool
}

// FindEntry looks for an entry with a given name in hash cache stream and returns it or nil if not found.
func (hcr *reader) FindEntry(relativeName string) *Entry {
	for hcr.nextEntry != nil && isLess(hcr.nextEntry.Name, relativeName) {
		log.Debugf("skipping %v while looking for %v", hcr.nextEntry.Name, relativeName)
		hcr.readahead()
	}

	if hcr.nextEntry != nil && relativeName == hcr.nextEntry.Name {
		e := hcr.nextEntry
		hcr.nextEntry = nil
		hcr.readahead()
		return e
	}

	if hcr.nextEntry == nil {
		log.Debugf("end of cache while looking for %v", relativeName)
		return nil
	}

	log.Debugf("skipping %v while looking for %v", hcr.nextEntry.Name, relativeName)
	return nil
}

func (hcr *reader) CopyTo(w Writer) error {
	for hcr.nextEntry != nil {
		if err := w.WriteEntry(*hcr.nextEntry); err != nil {
			return err
		}
		hcr.readahead()
	}

	return nil
}

func (hcr *reader) readahead() {
	if hcr.reader != nil {
		hcr.nextEntry = nil
		e := hcr.nextManifestEntry()
		*e = Entry{}
		if err := hcr.reader.Read(e); err == nil {
			hcr.nextEntry = e
		} else if err != io.EOF {
			log.Debugf("unable to read next hash cache entry: %v", err)
		}
	}

	if hcr.nextEntry == nil {
		hcr.reader = nil
	}
}

func (hcr *reader) nextManifestEntry() *Entry {
	hcr.odd = !hcr.odd
	if hcr.odd {
		return &hcr.entry1
	}

	return &hcr.entry0
}

type nullReader struct {
}

func (*nullReader) FindEntry(relativeName string) *Entry {
	return nil
}

func (*nullReader) CopyTo(w Writer) error {
	return nil
}

// Open starts reading hash cache content.
func Open(r io.Reader) Reader {
	if r == nil {
		return &nullReader{}
	}

	jsr, err := jsonstream.NewReader(bufio.NewReader(r), hashCacheStreamType, nil)
	if err != nil {
		return &nullReader{}
	}
	var hcr reader
	hcr.reader = jsr
	hcr.nextEntry = nil
	hcr.first = true
	hcr.readahead()
	log.Debugf("nextEntry: %v", hcr.nextEntry)
	return &hcr
}
