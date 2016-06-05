package repo

import (
	"bufio"
	"bytes"
	"crypto/cipher"
	"crypto/hmac"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/kopia/kopia/blob"
)

const (
	minLocatorSizeBytes = 16
)

// Since we never share keys, using constant IV is fine.
// Instead of using all-zero, we use this one.
var constantIV = []byte("kopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopia")

// Repository objects addressed by their content allows reading and writing them.
type Repository interface {
	// NewWriter opens an ObjectWriter for writing new content to the blob.
	NewWriter(options ...WriterOption) ObjectWriter

	// Open creates an io.ReadSeeker for reading object with a specified ID.
	Open(objectID ObjectID) (io.ReadSeeker, error)

	Flush() error
	Storage() blob.Storage
	Close()

	ResetStats()
	Stats() RepositoryStats
}

// RepositoryStats exposes statistics about Repository operation
type RepositoryStats struct {
	HashedBytes  int64
	HashedBlocks int32

	BlocksReadFromStorage int32
	BytesReadFromStorage  int64

	BlocksWrittenToStorage int32
	BytesWrittenToStorage  int64

	EncryptedBytes int64
	DecryptedBytes int64

	InvalidBlocks int32
	ValidBlocks   int32
}

type keygenFunc func([]byte) (blockIDBytes []byte, key []byte)

type repository struct {
	storage       blob.Storage
	verbose       bool
	bufferManager *bufferManager
	stats         RepositoryStats

	format   Format
	idFormat ObjectIDFormat
}

func (mgr *repository) Close() {
	mgr.Flush()
	mgr.bufferManager.close()
}

func (mgr *repository) Flush() error {
	return mgr.storage.Flush()
}

func (mgr *repository) ResetStats() {
	mgr.stats = RepositoryStats{}
}

func (mgr *repository) Stats() RepositoryStats {
	return mgr.stats
}

func (mgr *repository) Storage() blob.Storage {
	return mgr.storage
}

func (mgr *repository) NewWriter(options ...WriterOption) ObjectWriter {
	result := newObjectWriter(
		objectWriterConfig{
			mgr:        mgr,
			putOptions: blob.PutOptions{},
		},
		ObjectIDTypeStored)

	for _, option := range options {
		option(result)
	}

	return result
}

func (mgr *repository) Open(objectID ObjectID) (io.ReadSeeker, error) {
	r, err := mgr.newRawReader(objectID)
	if err != nil {
		return nil, err
	}

	if objectID.Type() == ObjectIDTypeList {
		seekTable := make([]seekTableEntry, 0, 100)

		seekTable, err = mgr.flattenListChunk(seekTable, objectID, r)
		if err != nil {
			return nil, err
		}

		totalLength := seekTable[len(seekTable)-1].endOffset()

		return &objectReader{
			storage:     mgr.storage,
			seekTable:   seekTable,
			totalLength: totalLength,
		}, nil
	}
	return r, err
}

// RepositoryOption controls the behavior of Repository.
type RepositoryOption func(o *repository) error

// WriteBack is an RepositoryOption that enables asynchronous writes to the storage using the pool
// of goroutines.
func WriteBack(workerCount int) RepositoryOption {
	return func(o *repository) error {
		o.storage = blob.NewWriteBackWrapper(o.storage, workerCount)
		return nil
	}
}

// WriteLimit is an RepositoryOption that sets the limit on the number of bytes that can be written
// to the storage in this Repository session. Once the limit is reached, the storage will
// return ErrWriteLimitExceeded.
func WriteLimit(maxBytes int64) RepositoryOption {
	return func(o *repository) error {
		o.storage = blob.NewWriteLimitWrapper(o.storage, maxBytes)
		return nil
	}
}

// EnableLogging is an RepositoryOption that causes all storage access to be logged.
func EnableLogging() RepositoryOption {
	return func(o *repository) error {
		o.storage = blob.NewLoggingWrapper(o.storage)
		return nil
	}
}

func hmacFunc(key []byte, hf func() hash.Hash) func() hash.Hash {
	return func() hash.Hash {
		return hmac.New(hf, key)
	}
}

// NewRepository creates new Repository with the specified storage, options, and key provider.
func NewRepository(
	r blob.Storage,
	f *Format,
	options ...RepositoryOption,
) (Repository, error) {
	if f.Version != "1" {
		return nil, fmt.Errorf("unsupported storage version: %v", f.Version)
	}
	if f.MaxBlobSize < 100 {
		return nil, fmt.Errorf("MaxBlobSize is not set")
	}

	sf := SupportedFormats.Find(f.ObjectFormat)
	if sf == nil {
		return nil, fmt.Errorf("unknown object format: %v", f.ObjectFormat)
	}

	mgr := &repository{
		storage: r,
		format:  *f,
	}

	mgr.idFormat = *sf

	for _, o := range options {
		if err := o(mgr); err != nil {
			mgr.Close()
			return nil, err
		}
	}

	mgr.bufferManager = newBufferManager(mgr.format.MaxBlobSize)

	return mgr, nil
}

func (mgr *repository) hashBuffer(data []byte) ([]byte, []byte) {
	return mgr.idFormat.hashBuffer(data, mgr.format.Secret)
}

func (mgr *repository) hashBufferForWriting(buffer *bytes.Buffer, prefix string) (ObjectID, blob.BlockReader, error) {
	var data []byte
	if buffer != nil {
		data = buffer.Bytes()
	}

	var blockCipher cipher.Block

	contentHash, cryptoKey := mgr.hashBuffer(data)
	if cryptoKey != nil {
		var err error
		blockCipher, err = mgr.idFormat.createCipher(cryptoKey)
		if err != nil {
			return "", nil, err
		}
	}

	var objectID ObjectID
	if len(cryptoKey) > 0 {
		objectID = ObjectID(prefix + hex.EncodeToString(contentHash) + objectIDEncryptionInfoSeparator + hex.EncodeToString(cryptoKey))
	} else {
		objectID = ObjectID(prefix + hex.EncodeToString(contentHash))
	}

	atomic.AddInt32(&mgr.stats.HashedBlocks, 1)
	atomic.AddInt64(&mgr.stats.HashedBytes, int64(len(data)))

	if buffer == nil {
		return objectID, blob.NewBlockReader(bytes.NewBuffer(nil)), nil
	}

	blockReader := mgr.bufferManager.returnBufferOnClose(buffer)
	blockReader = newCountingReader(blockReader, &mgr.stats.BytesWrittenToStorage)

	if len(cryptoKey) > 0 {
		// Since we're not sharing the key, all-zero IV is ok.
		// We don't need to worry about separate MAC either, since hashing content produces object ID.
		ctr := cipher.NewCTR(blockCipher, constantIV[0:blockCipher.BlockSize()])

		blockReader = newCountingReader(
			newEncryptingReader(blockReader, ctr),
			&mgr.stats.EncryptedBytes)
	}

	return objectID, blockReader, nil
}

func (mgr *repository) flattenListChunk(
	seekTable []seekTableEntry,
	listObjectID ObjectID,
	rawReader io.Reader) ([]seekTableEntry, error) {

	scanner := bufio.NewScanner(rawReader)

	for scanner.Scan() {
		c := scanner.Text()
		comma := strings.Index(c, ",")
		if comma <= 0 {
			return nil, fmt.Errorf("unsupported entry '%v' in list '%s'", c, listObjectID)
		}

		length, err := strconv.ParseInt(c[0:comma], 10, 64)

		objectID, err := ParseObjectID(c[comma+1:])
		if err != nil {
			return nil, fmt.Errorf("unsupported entry '%v' in list '%s': %#v", c, listObjectID, err)
		}

		switch objectID.Type() {
		case ObjectIDTypeList:
			subreader, err := mgr.newRawReader(objectID)
			if err != nil {
				return nil, err
			}

			seekTable, err = mgr.flattenListChunk(seekTable, objectID, subreader)
			if err != nil {
				return nil, err
			}

		case ObjectIDTypeStored:
			var startOffset int64
			if len(seekTable) > 0 {
				startOffset = seekTable[len(seekTable)-1].endOffset()
			} else {
				startOffset = 0
			}

			seekTable = append(
				seekTable,
				seekTableEntry{
					blockID:     objectID.BlockID(),
					startOffset: startOffset,
					length:      length,
				})

		default:
			return nil, fmt.Errorf("unsupported entry '%v' in list '%v'", objectID, listObjectID)

		}
	}

	return seekTable, nil
}

func (mgr *repository) newRawReader(objectID ObjectID) (io.ReadSeeker, error) {
	inline := objectID.InlineData()
	if inline != nil {
		return bytes.NewReader(inline), nil
	}

	blockID := objectID.BlockID()
	payload, err := mgr.storage.GetBlock(blockID)
	if err != nil {
		return nil, err
	}

	atomic.AddInt32(&mgr.stats.BlocksReadFromStorage, 1)
	atomic.AddInt64(&mgr.stats.BytesReadFromStorage, int64(len(payload)))

	if objectID.EncryptionInfo() == NoEncryption {
		if err := mgr.verifyChecksum(payload, objectID.BlockID()); err != nil {
			return nil, err
		}
		return bytes.NewReader(payload), nil
	}

	cryptoKey, err := hex.DecodeString(string(objectID.EncryptionInfo()))
	if err != nil {
		return nil, errors.New("malformed encryption key")
	}

	blockCipher, err := mgr.idFormat.createCipher(cryptoKey)
	if err != nil {
		return nil, errors.New("cannot create cipher")
	}

	iv := constantIV[0:blockCipher.BlockSize()]
	ctr := cipher.NewCTR(blockCipher, iv)
	ctr.XORKeyStream(payload, payload)

	// Since the encryption key is a function of data, we must be able to generate exactly the same key
	// after decrypting the content. This serves as a checksum.
	atomic.AddInt64(&mgr.stats.DecryptedBytes, int64(len(payload)))

	if err := mgr.verifyChecksum(payload, objectID.BlockID()); err != nil {
		return nil, err
	}

	return bytes.NewReader(payload), nil
}

func (mgr *repository) verifyChecksum(data []byte, blockID string) error {
	payloadHash, _ := mgr.hashBuffer(data)
	checksum := hex.EncodeToString(payloadHash)
	if !strings.HasSuffix(string(blockID), checksum) {
		atomic.AddInt32(&mgr.stats.InvalidBlocks, 1)
		return fmt.Errorf("invalid checksum for blob: '%v'", blockID)
	}

	atomic.AddInt32(&mgr.stats.ValidBlocks, 1)
	return nil
}
