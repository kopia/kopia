package fs

import (
	"encoding/json"
	"fmt"
	"io"
)

type manifestEntry struct {
	Name     string `json:"name"`
	Hash     uint64 `json:"hash,omitempty,string"`
	ObjectID string `json:"oid"`
}

type uploadManifestReader struct {
	reader    io.Reader
	decoder   *json.Decoder
	nextEntry *manifestEntry
}

func (hcr *uploadManifestReader) Open(r io.Reader) error {
	hcr.decoder = json.NewDecoder(r)

	if err := ensureDelimiter(hcr.decoder, json.Delim('{')); err != nil {
		return invalidDirectoryError(err)
	}

	if err := ensureStringToken(hcr.decoder, "format"); err != nil {
		return invalidDirectoryError(err)
	}

	// Parse format and trailing comma
	var format directoryFormat
	if err := hcr.decoder.Decode(&format); err != nil {
		return invalidDirectoryError(err)
	}

	if format.Version != 1 {
		return invalidDirectoryError(fmt.Errorf("unsupported version: %v", format.Version))
	}

	if err := ensureStringToken(hcr.decoder, "entries"); err != nil {
		return invalidDirectoryError(err)
	}

	if err := ensureDelimiter(hcr.decoder, json.Delim('[')); err != nil {
		return invalidDirectoryError(err)
	}

	hcr.readahead()
	return nil
}

func (hcr *uploadManifestReader) GetEntry(relativeName string) (*manifestEntry, int) {
	skipCount := 0
	//log.Printf("looking for %v", relativeName)
	for hcr.nextEntry != nil && isLess(hcr.nextEntry.Name, relativeName) {
		hcr.readahead()
		skipCount++
	}

	if hcr.nextEntry != nil && relativeName == hcr.nextEntry.Name {
		//log.Printf("*** found hashcache entry: %v", relativeName)
		e := hcr.nextEntry
		hcr.nextEntry = nil
		hcr.readahead()
		return e, skipCount
	}

	// if hcr.reader != nil {
	// 	log.Printf("*** not found hashcache entry: %v", relativeName)
	// }

	return nil, skipCount
}

func (hcr *uploadManifestReader) readahead() {
	if hcr.reader != nil {
		hcr.nextEntry = nil
		if hcr.decoder.More() {
			var me manifestEntry
			if err := hcr.decoder.Decode(&me); err == nil {
				hcr.nextEntry = &me
			} else {
				hcr.reader = nil
			}
		}
	}
}

func newManifestReader(r io.Reader) (*uploadManifestReader, error) {
	dr := &uploadManifestReader{
		decoder: json.NewDecoder(r),
	}

	if err := ensureDelimiter(dr.decoder, json.Delim('{')); err != nil {
		return nil, invalidDirectoryError(err)
	}

	if err := ensureStringToken(dr.decoder, "format"); err != nil {
		return nil, invalidDirectoryError(err)
	}

	// Parse format and trailing comma
	var format directoryFormat
	if err := dr.decoder.Decode(&format); err != nil {
		return nil, invalidDirectoryError(err)
	}

	if format.Version != 1 {
		return nil, invalidDirectoryError(fmt.Errorf("unsupported version: %v", format.Version))
	}

	if err := ensureStringToken(dr.decoder, "entries"); err != nil {
		return nil, invalidDirectoryError(err)
	}

	if err := ensureDelimiter(dr.decoder, json.Delim('[')); err != nil {
		return nil, invalidDirectoryError(err)
	}

	return dr, nil
}

type uploadManifestWriter struct {
	writer    io.Writer
	buf       []byte
	separator []byte

	lastNameWritten string
}

func (dw *uploadManifestWriter) WriteEntry(e manifestEntry) error {
	if dw.lastNameWritten != "" {
		if isLessOrEqual(e.Name, dw.lastNameWritten) {
			return fmt.Errorf("out-of-order directory entry, previous '%v' current '%v'", dw.lastNameWritten, e.Name)
		}
		dw.lastNameWritten = e.Name
	}

	v, _ := json.Marshal(&e)

	dw.writer.Write(dw.separator)
	dw.writer.Write(v)
	dw.separator = []byte(",\n  ")

	return nil
}

func (dw *uploadManifestWriter) Close() error {
	return nil
}
