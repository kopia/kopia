package repo

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/kopia/kopia/internal/config"
	"github.com/kopia/kopia/internal/jsonstream"
)

// ObjectReader allows reading, seeking, getting the length of and closing of a repository object.
type ObjectReader interface {
	io.Reader
	io.Seeker
	io.Closer
	Length() int64
}

// ObjectManager implements a content-addressable storage on top of blob storage.
type ObjectManager struct {
	format config.RepositoryObjectFormat

	verbose  bool
	blockMgr *BlockManager

	async              bool
	writeBackWG        sync.WaitGroup
	writeBackSemaphore semaphore

	trace func(message string, args ...interface{})

	newSplitter func() objectSplitter
}

// Close closes the connection to the underlying blob storage and releases any resources.
func (om *ObjectManager) Close() error {
	om.writeBackWG.Wait()
	return om.Flush()
}

// Optimize performs object optimizations to improve performance of future operations.
// The operation will not affect objects written after cutoffTime to prevent race conditions.
func (om *ObjectManager) Optimize(cutoffTime time.Time, inUseBlocks map[string]bool) error {
	if err := om.blockMgr.CompactIndexes(cutoffTime, inUseBlocks); err != nil {
		return err
	}

	return nil
}

// NewWriter creates an ObjectWriter for writing to the repository.
func (om *ObjectManager) NewWriter(opt WriterOptions) ObjectWriter {
	w := &objectWriter{
		repo:        om,
		splitter:    om.newSplitter(),
		description: opt.Description,
		prefix:      opt.BlockNamePrefix,
		packGroup:   opt.PackGroup,
	}

	if opt.splitter != nil {
		w.splitter = opt.splitter
	}

	return w
}

// Open creates new ObjectReader for reading given object from a repository.
func (om *ObjectManager) Open(objectID ObjectID) (ObjectReader, error) {
	// log.Printf("Repository::Open %v", objectID.String())
	// defer log.Printf("finished Repository::Open() %v", objectID.String())

	// Flush any pending writes.
	om.writeBackWG.Wait()

	if objectID.Section != nil {
		baseReader, err := om.Open(objectID.Section.Base)
		if err != nil {
			return nil, fmt.Errorf("cannot create base reader: %+v %v", objectID.Section.Base, err)
		}

		return newObjectSectionReader(objectID.Section.Start, objectID.Section.Length, baseReader)
	}

	if objectID.Indirect != nil {
		rd, err := om.Open(*objectID.Indirect)
		if err != nil {
			return nil, err
		}
		defer rd.Close()

		seekTable, err := om.flattenListChunk(rd)
		if err != nil {
			return nil, err
		}

		totalLength := seekTable[len(seekTable)-1].endOffset()

		return &objectReader{
			repo:        om,
			seekTable:   seekTable,
			totalLength: totalLength,
		}, nil
	}

	return om.newRawReader(objectID)
}

// VerifyObject ensures that all objects backing ObjectID are present in the repository
// and returns the total length of the object and storage blocks of which it is composed.
func (om *ObjectManager) VerifyObject(oid ObjectID) (int64, []string, error) {
	// Flush any pending writes.
	om.writeBackWG.Wait()

	blocks := &blockTracker{}
	l, err := om.verifyObjectInternal(oid, blocks)
	if err != nil {
		return 0, nil, err
	}

	return l, blocks.blockIDs(), nil
}

func (om *ObjectManager) verifyObjectInternal(oid ObjectID, blocks *blockTracker) (int64, error) {
	//log.Printf("verifyObjectInternal %v", oid)
	if oid.Section != nil {
		l, err := om.verifyObjectInternal(oid.Section.Base, blocks)
		if err != nil {
			return 0, err
		}

		if oid.Section.Length >= 0 && oid.Section.Start+oid.Section.Length <= l {
			return oid.Section.Length, nil
		}

		return 0, fmt.Errorf("section object %q not within parent object size of %v", oid, l)
	}

	if oid.Indirect != nil {
		if _, err := om.verifyObjectInternal(*oid.Indirect, blocks); err != nil {
			return 0, fmt.Errorf("unable to read index: %v", err)
		}
		rd, err := om.Open(*oid.Indirect)
		if err != nil {
			return 0, err
		}
		defer rd.Close()

		seekTable, err := om.flattenListChunk(rd)
		if err != nil {
			return 0, err
		}

		for i, m := range seekTable {
			l, err := om.verifyObjectInternal(m.Object, blocks)
			if err != nil {
				return 0, err
			}

			if l != m.Length {
				return 0, fmt.Errorf("unexpected length of part %#v of indirect object %q: %v %v, expected %v", i, oid, m.Object, l, m.Length)
			}
		}

		totalLength := seekTable[len(seekTable)-1].endOffset()
		return totalLength, nil
	}

	p, isPacked, err := om.blockMgr.blockIDToPackSection(oid.StorageBlock)
	if err != nil {
		return 0, err
	}

	if isPacked {
		blocks.addBlock(oid.StorageBlock)
		l, err := om.verifyObjectInternal(p.Base, blocks)
		if err != nil {
			return 0, err
		}

		if p.Length >= 0 && p.Start+p.Length <= l {
			return p.Length, nil
		}

		return 0, fmt.Errorf("packed object %v does not fit within its parent pack %v (pack length %v)", oid, p, l)
	}

	l, err := om.blockMgr.BlockSize(oid.StorageBlock)
	if err != nil {
		return 0, fmt.Errorf("unable to read %q: %v", oid.StorageBlock, err)
	}
	blocks.addBlock(oid.StorageBlock)

	return l, nil
}

// Flush closes any pending pack files. Once this method returns, ObjectIDs returned by ObjectManager are
// ok to be used.
func (om *ObjectManager) Flush() error {
	om.writeBackWG.Wait()
	return om.blockMgr.Flush()
}

func nullTrace(message string, args ...interface{}) {
}

// newObjectManager creates an ObjectManager with the specified block manager and options.
func newObjectManager(bm *BlockManager, f config.RepositoryObjectFormat, opts *Options) (*ObjectManager, error) {
	if err := validateFormat(&f); err != nil {
		return nil, err
	}

	om := &ObjectManager{
		blockMgr: bm,
		format:   f,
		trace:    nullTrace,
	}

	os := objectSplitterFactories[applyDefaultString(f.Splitter, "FIXED")]
	if os == nil {
		return nil, fmt.Errorf("unsupported splitter %q", f.Splitter)
	}

	om.newSplitter = func() objectSplitter {
		return os(&f)
	}

	if opts != nil {
		if opts.TraceObjectManager != nil {
			om.trace = opts.TraceObjectManager
		} else {
			om.trace = nullTrace
		}
		if opts.WriteBack > 0 {
			om.async = true
			om.writeBackSemaphore = make(semaphore, opts.WriteBack)
		}
	}

	return om, nil
}

func (om *ObjectManager) flattenListChunk(rawReader io.Reader) ([]indirectObjectEntry, error) {
	pr, err := jsonstream.NewReader(bufio.NewReader(rawReader), indirectStreamType)
	if err != nil {
		return nil, err
	}
	var seekTable []indirectObjectEntry

	for {
		var oe indirectObjectEntry

		err := pr.Read(&oe)
		if err == io.EOF {
			break
		}

		if err != nil {
			log.Printf("Failed to read indirect object: %v", err)
			return nil, err
		}

		seekTable = append(seekTable, oe)
	}

	return seekTable, nil
}

func (om *ObjectManager) newRawReader(objectID ObjectID) (ObjectReader, error) {
	payload, err := om.blockMgr.GetBlock(objectID.StorageBlock)
	if err != nil {
		return nil, err
	}

	return newObjectReaderWithData(payload), nil
}

type readerWithData struct {
	io.ReadSeeker
	length int64
}

func (rwd *readerWithData) Close() error {
	return nil
}

func (rwd *readerWithData) Length() int64 {
	return rwd.length
}

func newObjectReaderWithData(data []byte) ObjectReader {
	return &readerWithData{
		ReadSeeker: bytes.NewReader(data),
		length:     int64(len(data)),
	}
}
