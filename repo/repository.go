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
	"io/ioutil"
	"log"
	"strings"
	"sync/atomic"

	"github.com/kopia/kopia/internal"

	"github.com/kopia/kopia/storage"
	"github.com/kopia/kopia/storage/logging"
)

const (
	minLocatorSizeBytes = 16
)

// ObjectReader allows reading, seeking and closing of a repository object.
type ObjectReader interface {
	io.Reader
	io.Seeker
	io.Closer
	Length() int64
}

// Since we never share keys, using constant IV is fine.
// Instead of using all-zero, we use this one.
var constantIV = []byte("kopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopia")

// Repository objects addressed by their content allows reading and writing them.
type Repository interface {
	// NewWriter opens an ObjectWriter for writing new content to the storage.
	NewWriter(options ...WriterOption) ObjectWriter

	// Open creates an io.ReadSeeker for reading object with a specified ID.
	Open(objectID ObjectID) (ObjectReader, error)

	Flush() error
	Storage() storage.Storage
	Close()

	ResetStats()
	Stats() RepositoryStats
}

// RepositoryStats exposes statistics about Repository operation
type RepositoryStats struct {
	// Keep int64 fields first to ensure they get aligned to at least 64-bit boundaries
	// which is required for atomic access on ARM and x86-32.
	HashedBytes           int64
	BytesReadFromStorage  int64
	BytesWrittenToStorage int64
	EncryptedBytes        int64
	DecryptedBytes        int64

	HashedBlocks           int32
	BlocksReadFromStorage  int32
	BlocksWrittenToStorage int32
	InvalidBlocks          int32
	ValidBlocks            int32
}

type keygenFunc func([]byte) (blockIDBytes []byte, key []byte)

type repository struct {
	storage       storage.Storage
	verbose       bool
	bufferManager *bufferManager
	stats         *RepositoryStats

	format   Format
	idFormat ObjectIDFormatInfo
}

func (repo *repository) Close() {
	repo.Flush()
	repo.bufferManager.close()
}

func (repo *repository) Flush() error {
	if f, ok := repo.storage.(storage.Flusher); ok {
		return f.Flush()
	}

	return nil
}

func (repo *repository) ResetStats() {
	repo.stats = &RepositoryStats{}
}

func (repo *repository) Stats() RepositoryStats {
	return *repo.stats
}

func (repo *repository) Storage() storage.Storage {
	return repo.storage
}

func (repo *repository) NewWriter(options ...WriterOption) ObjectWriter {
	result := &objectWriter{
		repo:         repo,
		blockTracker: &blockTracker{},
	}

	for _, option := range options {
		option(result)
	}

	return result
}

func (repo *repository) Open(objectID ObjectID) (ObjectReader, error) {
	// log.Printf("Repository::Open %v", objectID.String())
	// defer log.Printf("finished Repository::Open() %v", objectID.String())

	if objectID.Section != nil {
		if objectID.Section.Base == nil {
			return nil, fmt.Errorf("invalid section base object")
		}

		baseReader, err := repo.Open(*objectID.Section.Base)
		if err != nil {
			return nil, fmt.Errorf("cannot create base reader: %v", err)
		}

		return newObjectSectionReader(objectID.Section.Start, objectID.Section.Length, baseReader)
	}

	if objectID.Indirect > 0 {
		r, err := repo.Open(removeIndirection(objectID))
		if err != nil {
			return nil, err
		}
		defer r.Close()

		seekTable, err := repo.flattenListChunk(r)
		if err != nil {
			return nil, err
		}

		totalLength := seekTable[len(seekTable)-1].endOffset()

		return &objectReader{
			repo:        repo,
			seekTable:   seekTable,
			totalLength: totalLength,
		}, nil
	}

	return repo.newRawReader(objectID)
}

func dumpObject(repo *repository, oid ObjectID) {
	oid.Indirect = 0
	log.Printf("  dumping %v", oid.String())
	defer log.Printf("  end of %v", oid.String())
	r, err := repo.Open(oid)
	if err != nil {
		log.Printf("failed to open %v: %v", oid, err)
		return
	}
	defer r.Close()

	b, err := ioutil.ReadAll(r)
	if err != nil {
		log.Printf("failed to read %v: %v", oid, err)
		return
	}
	log.Printf("%x", b)
}

// RepositoryOption controls the behavior of Repository.
type RepositoryOption func(o *repository) error

// WriteBack is an RepositoryOption that enables asynchronous writes to the storage using the pool
// of goroutines.
func WriteBack(workerCount int) RepositoryOption {
	return func(o *repository) error {
		o.storage = storage.NewWriteBackWrapper(o.storage, workerCount)
		return nil
	}
}

// EnableLogging is an RepositoryOption that causes all storage access to be logged.
func EnableLogging() RepositoryOption {
	return func(o *repository) error {
		o.storage = logging.NewWrapper(o.storage)
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
	r storage.Storage,
	f *Format,
	options ...RepositoryOption,
) (Repository, error) {
	if f.MaxBlobSize < 100 {
		return nil, fmt.Errorf("MaxBlobSize is not set")
	}

	sf := SupportedFormats[f.ObjectFormat]
	if sf == nil {
		return nil, fmt.Errorf("unknown object format: %v", f.ObjectFormat)
	}

	repo := &repository{
		storage: r,
		format:  *f,
		stats:   &RepositoryStats{},
	}

	repo.idFormat = sf

	for _, o := range options {
		if err := o(repo); err != nil {
			repo.Close()
			return nil, err
		}
	}

	repo.bufferManager = newBufferManager(int(repo.format.MaxBlobSize))

	return repo, nil
}

func (repo *repository) hashBuffer(data []byte) ([]byte, []byte) {
	return repo.idFormat.HashBuffer(data, repo.format.Secret)
}

func (repo *repository) hashBufferForWriting(buffer *bytes.Buffer, prefix string) (ObjectID, storage.ReaderWithLength, error) {
	var data []byte
	if buffer != nil {
		data = buffer.Bytes()
	}

	var blockCipher cipher.Block

	contentHash, cryptoKey := repo.hashBuffer(data)
	if cryptoKey != nil {
		var err error
		blockCipher, err = repo.idFormat.CreateCipher(cryptoKey)
		if err != nil {
			return NullObjectID, nil, err
		}
	}

	objectID := ObjectID{
		StorageBlock: prefix + hex.EncodeToString(contentHash),
	}

	if len(cryptoKey) > 0 {
		objectID.Encryption = cryptoKey
	}

	atomic.AddInt32(&repo.stats.HashedBlocks, 1)
	atomic.AddInt64(&repo.stats.HashedBytes, int64(len(data)))

	if buffer == nil {
		return objectID, storage.NewReader(bytes.NewBuffer(nil)), nil
	}

	blockReader := repo.bufferManager.returnBufferOnClose(buffer)
	blockReader = newCountingReader(blockReader, &repo.stats.BytesWrittenToStorage)

	if len(cryptoKey) > 0 {
		// Since we're not sharing the key, all-zero IV is ok.
		// We don't need to worry about separate MAC either, since hashing content produces object ID.
		ctr := cipher.NewCTR(blockCipher, constantIV[0:blockCipher.BlockSize()])

		blockReader = newCountingReader(
			newEncryptingReader(blockReader, ctr),
			&repo.stats.EncryptedBytes)
	}

	return objectID, blockReader, nil
}

func (repo *repository) flattenListChunk(rawReader io.Reader) ([]IndirectObjectEntry, error) {

	r := bufio.NewReader(rawReader)
	pr := internal.NewProtoStreamReader(r, internal.ProtoStreamTypeIndirect)
	var seekTable []IndirectObjectEntry

	for {
		var oe IndirectObjectEntry

		err := pr.Read(&oe)
		if err == io.EOF {
			break
		}

		if err != nil {
			log.Printf("Failed to read proto: %v", err)
			return nil, err
		}

		seekTable = append(seekTable, oe)
	}

	return seekTable, nil
}

func removeIndirection(o ObjectID) ObjectID {
	if o.Indirect <= 0 {
		panic("removeIndirection() called on a direct object")
	}
	o.Indirect--
	return o
}

func (repo *repository) newRawReader(objectID ObjectID) (ObjectReader, error) {
	if objectID.Content != nil {
		return newObjectReaderWithData(objectID.Content), nil
	}

	blockID := objectID.StorageBlock
	payload, err := repo.storage.GetBlock(blockID)
	if err != nil {
		return nil, err
	}

	atomic.AddInt32(&repo.stats.BlocksReadFromStorage, 1)
	atomic.AddInt64(&repo.stats.BytesReadFromStorage, int64(len(payload)))

	if objectID.Encryption == nil {
		if err := repo.verifyChecksum(payload, objectID.StorageBlock); err != nil {
			return nil, err
		}
		return newObjectReaderWithData(payload), nil
	}

	blockCipher, err := repo.idFormat.CreateCipher(objectID.Encryption)
	if err != nil {
		return nil, errors.New("cannot create cipher")
	}

	iv := constantIV[0:blockCipher.BlockSize()]
	ctr := cipher.NewCTR(blockCipher, iv)
	ctr.XORKeyStream(payload, payload)

	// Since the encryption key is a function of data, we must be able to generate exactly the same key
	// after decrypting the content. This serves as a checksum.
	atomic.AddInt64(&repo.stats.DecryptedBytes, int64(len(payload)))

	if err := repo.verifyChecksum(payload, objectID.StorageBlock); err != nil {
		return nil, err
	}

	return newObjectReaderWithData(payload), nil
}

func (repo *repository) verifyChecksum(data []byte, blockID string) error {
	payloadHash, _ := repo.hashBuffer(data)
	checksum := hex.EncodeToString(payloadHash)
	if !strings.HasSuffix(string(blockID), checksum) {
		atomic.AddInt32(&repo.stats.InvalidBlocks, 1)
		return fmt.Errorf("invalid checksum for blob: '%v'", blockID)
	}

	atomic.AddInt32(&repo.stats.ValidBlocks, 1)
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
