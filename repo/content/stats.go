package content

import (
	"sync/atomic"
)

// Stats exposes statistics about content operation.
type Stats struct {
	// Keep int64 fields first to ensure they get aligned to at least 64-bit
	// boundaries, which is required for atomic access on ARM and x86-32.
	readBytes      int64
	writtenBytes   int64
	decryptedBytes int64
	encryptedBytes int64
	hashedBytes    int64

	readContents    uint32
	writtenContents uint32
	hashedContents  uint32
	invalidContents uint32
	validContents   uint32
}

// Reset clears all content statistics.
func (s *Stats) Reset() {
	// while not atomic, it ensures values are propagated
	atomic.StoreInt64(&s.readBytes, 0)
	atomic.StoreInt64(&s.writtenBytes, 0)
	atomic.StoreInt64(&s.decryptedBytes, 0)
	atomic.StoreInt64(&s.encryptedBytes, 0)
	atomic.StoreInt64(&s.hashedBytes, 0)
	atomic.StoreUint32(&s.readContents, 0)
	atomic.StoreUint32(&s.writtenContents, 0)
	atomic.StoreUint32(&s.hashedContents, 0)
	atomic.StoreUint32(&s.invalidContents, 0)
	atomic.StoreUint32(&s.validContents, 0)
}

// ReadContent returns the approximate read content count and their total size in bytes.
func (s *Stats) ReadContent() (count uint32, bytes int64) {
	return readCountSum(&s.readContents, &s.readBytes)
}

// WrittenContent returns the approximate written content count and their total size in bytes.
func (s *Stats) WrittenContent() (count uint32, bytes int64) {
	return readCountSum(&s.writtenContents, &s.writtenBytes)
}

// HashedContent returns the approximate hashed content count and their total size in bytes.
func (s *Stats) HashedContent() (count uint32, bytes int64) {
	return readCountSum(&s.hashedContents, &s.hashedBytes)
}

// DecryptedBytes returns the approximate total number of decrypted bytes.
func (s *Stats) DecryptedBytes() int64 {
	return atomic.LoadInt64(&s.decryptedBytes)
}

// EncryptedBytes returns the approximate total number of decrypted bytes.
func (s *Stats) EncryptedBytes() int64 {
	return atomic.LoadInt64(&s.encryptedBytes)
}

// InvalidContents returns the approximate count of invalid contents found.
func (s *Stats) InvalidContents() uint32 {
	return atomic.LoadUint32(&s.invalidContents)
}

// ValidContents returns the approximate count of valid contents found.
func (s *Stats) ValidContents() uint32 {
	return atomic.LoadUint32(&s.validContents)
}

func (s *Stats) decrypted(size int) int64 {
	return atomic.AddInt64(&s.decryptedBytes, int64(size))
}

func (s *Stats) encrypted(size int) int64 {
	return atomic.AddInt64(&s.encryptedBytes, int64(size))
}

func (s *Stats) readContent(size int) (count uint32, sum int64) {
	return updateCountSum(&s.readContents, &s.readBytes, size)
}

func (s *Stats) wroteContent(size int) (count uint32, sum int64) {
	return updateCountSum(&s.writtenContents, &s.writtenBytes, size)
}

func (s *Stats) hashedContent(size int) (count uint32, sum int64) {
	return updateCountSum(&s.hashedContents, &s.hashedBytes, size)
}

func (s *Stats) foundValidContent() uint32 {
	return atomic.AddUint32(&s.validContents, 1)
}

func (s *Stats) foundInvalidContent() uint32 {
	return atomic.AddUint32(&s.invalidContents, 1)
}

func updateCountSum(count *uint32, sum *int64, delta int) (updatedCount uint32, updatedSum int64) {
	return atomic.AddUint32(count, 1), atomic.AddInt64(sum, int64(delta))
}

func readCountSum(count *uint32, sum *int64) (c uint32, s int64) {
	return atomic.LoadUint32(count), atomic.LoadInt64(sum)
}
