package repo

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kopia/kopia/blob"
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
	stats   Stats
	storage blob.Storage

	verbose   bool
	format    config.RepositoryObjectFormat
	formatter objectFormatter

	packMgr *packManager

	async              bool
	writeBackWG        sync.WaitGroup
	writeBackSemaphore semaphore

	trace func(message string, args ...interface{})

	newSplitter func() objectSplitter
}

// Close closes the connection to the underlying blob storage and releases any resources.
func (r *ObjectManager) Close() error {
	r.writeBackWG.Wait()
	return r.Flush()
}

// Optimize performs object optimizations to improve performance of future operations.
// The opeartion will not affect objects written after cutoffTime to prevent race conditions.
func (r *ObjectManager) Optimize(cutoffTime time.Time) error {
	if err := r.packMgr.Compact(cutoffTime); err != nil {
		return err
	}

	return nil
}

// NewWriter creates an ObjectWriter for writing to the repository.
func (r *ObjectManager) NewWriter(opt WriterOptions) ObjectWriter {
	w := &objectWriter{
		repo:        r,
		splitter:    r.newSplitter(),
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
func (r *ObjectManager) Open(objectID ObjectID) (ObjectReader, error) {
	// log.Printf("Repository::Open %v", objectID.String())
	// defer log.Printf("finished Repository::Open() %v", objectID.String())

	// Flush any pending writes.
	r.writeBackWG.Wait()

	if objectID.Section != nil {
		baseReader, err := r.Open(objectID.Section.Base)
		if err != nil {
			return nil, fmt.Errorf("cannot create base reader: %+v %v", objectID.Section.Base, err)
		}

		return newObjectSectionReader(objectID.Section.Start, objectID.Section.Length, baseReader)
	}

	if objectID.Indirect != nil {
		rd, err := r.Open(*objectID.Indirect)
		if err != nil {
			return nil, err
		}
		defer rd.Close()

		seekTable, err := r.flattenListChunk(rd)
		if err != nil {
			return nil, err
		}

		totalLength := seekTable[len(seekTable)-1].endOffset()

		return &objectReader{
			repo:        r,
			seekTable:   seekTable,
			totalLength: totalLength,
		}, nil
	}

	return r.newRawReader(objectID)
}

// VerifyObject ensures that all objects backing ObjectID are present in the repository
// and returns the total length of the object and storage blocks of which it is composed.
func (r *ObjectManager) VerifyObject(oid ObjectID) (int64, []string, error) {
	// Flush any pending writes.
	r.writeBackWG.Wait()

	blocks := &blockTracker{}
	l, err := r.verifyObjectInternal(oid, blocks)
	if err != nil {
		return 0, nil, err
	}

	return l, blocks.blockIDs(), nil
}

func (r *ObjectManager) verifyObjectInternal(oid ObjectID, blocks *blockTracker) (int64, error) {
	//log.Printf("verifyObjectInternal %v", oid)
	if oid.Section != nil {
		l, err := r.verifyObjectInternal(oid.Section.Base, blocks)
		if err != nil {
			return 0, err
		}

		if oid.Section.Length >= 0 && oid.Section.Start+oid.Section.Length <= l {
			return oid.Section.Length, nil
		}

		return 0, fmt.Errorf("section object %q not within parent object size of %v", oid, l)
	}

	if oid.Indirect != nil {
		if _, err := r.verifyObjectInternal(*oid.Indirect, blocks); err != nil {
			return 0, fmt.Errorf("unable to read index: %v", err)
		}
		rd, err := r.Open(*oid.Indirect)
		if err != nil {
			return 0, err
		}
		defer rd.Close()

		seekTable, err := r.flattenListChunk(rd)
		if err != nil {
			return 0, err
		}

		for i, m := range seekTable {
			l, err := r.verifyObjectInternal(m.Object, blocks)
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

	p, isPacked, err := r.packMgr.blockIDToPackSection(oid.StorageBlock)
	if err != nil {
		return 0, err
	}

	if isPacked {
		l, err := r.verifyObjectInternal(p.Base, blocks)
		if err != nil {
			return 0, err
		}

		if p.Length >= 0 && p.Start+p.Length <= l {
			return p.Length, nil
		}

		return 0, fmt.Errorf("packed object %v does not fit within its parent pack %v (pack length %v)", oid, p, l)
	}

	l, err := r.packMgr.blockSize(oid.StorageBlock)
	if err != nil {
		return 0, fmt.Errorf("unable to read %q: %v", oid.StorageBlock, err)
	}

	blocks.addBlock(oid.StorageBlock)
	return l, nil
}

// Flush closes any pending pack files. Once this method returns, ObjectIDs returned by ObjectManager are
// ok to be used.
func (r *ObjectManager) Flush() error {
	r.writeBackWG.Wait()
	return r.packMgr.Flush()
}

func nullTrace(message string, args ...interface{}) {
}

// newObjectManager creates an ObjectManager with the specified storage, format and options.
func newObjectManager(s blob.Storage, f config.RepositoryObjectFormat, opts *Options) (*ObjectManager, error) {
	if err := validateFormat(&f); err != nil {
		return nil, err
	}

	sf := objectFormatterFactories[f.ObjectFormat]
	r := &ObjectManager{
		storage: s,
		format:  f,
		trace:   nullTrace,
	}

	os := objectSplitterFactories[applyDefaultString(f.Splitter, "FIXED")]
	if os == nil {
		return nil, fmt.Errorf("unsupported splitter %q", f.Splitter)
	}

	r.newSplitter = func() objectSplitter {
		return os(&r.format)
	}

	var err error
	r.formatter, err = sf(&r.format)
	if err != nil {
		return nil, err
	}

	if opts != nil {
		if opts.TraceObjectManager != nil {
			r.trace = opts.TraceObjectManager
		} else {
			r.trace = nullTrace
		}
		if opts.WriteBack > 0 {
			r.async = true
			r.writeBackSemaphore = make(semaphore, opts.WriteBack)
		}
	}

	r.packMgr = newPackManager(r)

	return r, nil
}

// hashEncryptAndWrite computes hash of a given buffer, optionally encrypts and writes it to storage.
// The write is not guaranteed to complete synchronously in case write-back is used, but by the time
// Repository.Close() returns all writes are guaranteed be over.
func (r *ObjectManager) hashEncryptAndWrite(packGroup string, buffer *bytes.Buffer, prefix string, isPackInternalObject bool) (ObjectID, error) {
	var data []byte
	if buffer != nil {
		data = buffer.Bytes()
	}

	// Hash the block and compute encryption key.
	objectID := r.formatter.ComputeObjectID(data)
	objectID.StorageBlock = prefix + objectID.StorageBlock
	atomic.AddInt32(&r.stats.HashedBlocks, 1)
	atomic.AddInt64(&r.stats.HashedBytes, int64(len(data)))

	if !isPackInternalObject {
		if r.format.MaxPackedContentLength > 0 && len(data) <= r.format.MaxPackedContentLength {
			packOID, err := r.packMgr.AddToPack(packGroup, objectID.StorageBlock, data)
			return packOID, err
		}

		// Before performing encryption, check if the block is already there.
		blockSize, err := r.packMgr.blockSize(objectID.StorageBlock)
		atomic.AddInt32(&r.stats.CheckedBlocks, int32(1))
		if err == nil && blockSize == int64(len(data)) {
			atomic.AddInt32(&r.stats.PresentBlocks, int32(1))
			// Block already exists in storage, correct size, return without uploading.
			return objectID, nil
		}

		if err != nil && err != blob.ErrBlockNotFound {
			// Don't know whether block exists in storage.
			return NullObjectID, err
		}
	}

	// Encrypt the block in-place.
	atomic.AddInt64(&r.stats.EncryptedBytes, int64(len(data)))
	data, err := r.formatter.Encrypt(data, objectID, 0)
	if err != nil {
		return NullObjectID, err
	}

	atomic.AddInt32(&r.stats.WrittenBlocks, int32(1))
	atomic.AddInt64(&r.stats.WrittenBytes, int64(len(data)))

	if err := r.storage.PutBlock(objectID.StorageBlock, data); err != nil {
		return NullObjectID, err
	}

	if !isPackInternalObject {
		r.packMgr.RegisterUnpackedBlock(objectID.StorageBlock, int64(len(data)))
	}
	return objectID, nil
}

func (r *ObjectManager) flattenListChunk(rawReader io.Reader) ([]indirectObjectEntry, error) {
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

func (r *ObjectManager) newRawReader(objectID ObjectID) (ObjectReader, error) {
	var payload []byte
	var err error
	underlyingObjectID := objectID
	var decryptSkip int

	p, ok, err := r.packMgr.blockIDToPackSection(objectID.StorageBlock)
	if err != nil {
		return nil, err
	}
	if ok {
		payload, err = r.storage.GetBlock(p.Base.StorageBlock, p.Start, p.Length)
		underlyingObjectID = p.Base
		decryptSkip = int(p.Start)
	} else {
		payload, err = r.storage.GetBlock(objectID.StorageBlock, 0, -1)
	}

	if err != nil {
		return nil, err
	}

	atomic.AddInt32(&r.stats.ReadBlocks, 1)
	atomic.AddInt64(&r.stats.ReadBytes, int64(len(payload)))

	payload, err = r.formatter.Decrypt(payload, underlyingObjectID, decryptSkip)
	atomic.AddInt64(&r.stats.DecryptedBytes, int64(len(payload)))
	if err != nil {
		return nil, err
	}

	// Since the encryption key is a function of data, we must be able to generate exactly the same key
	// after decrypting the content. This serves as a checksum.
	if err := r.verifyChecksum(payload, objectID.StorageBlock); err != nil {
		return nil, err
	}

	return newObjectReaderWithData(payload), nil
}

func (r *ObjectManager) verifyChecksum(data []byte, blockID string) error {
	expected := r.formatter.ComputeObjectID(data)
	if !strings.HasSuffix(blockID, expected.StorageBlock) {
		atomic.AddInt32(&r.stats.InvalidBlocks, 1)
		return fmt.Errorf("invalid checksum for blob: '%v', expected %v", blockID, expected.StorageBlock)
	}

	atomic.AddInt32(&r.stats.ValidBlocks, 1)
	return nil
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
