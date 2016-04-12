package fs

import "io"

type hashcacheReader struct {
	reader       *directoryReader
	nextEntry    *Entry
	skippedCount int
}

func (hcr *hashcacheReader) Open(dr *directoryReader) {
	hcr.reader = dr
	hcr.nextEntry = nil
	hcr.readahead()
}

func (hcr *hashcacheReader) GetEntry(relativeName string) *Entry {
	//log.Printf("looking for %v", relativeName)
	for hcr.nextEntry != nil && isLess(hcr.nextEntry.Name, relativeName) {
		hcr.skippedCount++
		hcr.readahead()
	}

	if hcr.nextEntry != nil && relativeName == hcr.nextEntry.Name {
		//log.Printf("*** found hashcache entry: %v", relativeName)
		e := hcr.nextEntry
		hcr.nextEntry = nil
		hcr.readahead()
		return e
	}

	// if hcr.reader != nil {
	// 	log.Printf("*** not found hashcache entry: %v", relativeName)
	// }

	return nil
}

func (hcr *hashcacheReader) SkippedCount() int {
	return hcr.skippedCount
}

func (hcr *hashcacheReader) readahead() {
	if hcr.reader != nil {
		next, err := hcr.reader.ReadNext()
		hcr.nextEntry = next
		if err == io.EOF {
			hcr.reader = nil
		}
	}
}
