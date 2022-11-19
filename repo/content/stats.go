package content

import (
	"sync/atomic"
)

// Stats exposes statistics about content operation.
type Stats struct {
	readBytes       atomic.Int64
	writtenBytes    atomic.Int64
	decryptedBytes  atomic.Int64
	encryptedBytes  atomic.Int64
	hashedBytes     atomic.Int64
	readContents    atomic.Uint32
	writtenContents atomic.Uint32
	hashedContents  atomic.Uint32
	invalidContents atomic.Uint32
	validContents   atomic.Uint32
}

// Reset clears all content statistics.
func (s *Stats) Reset() {
	// while not atomic, it ensures values are propagated
	s.readBytes.Store(0)
	s.writtenBytes.Store(0)
	s.decryptedBytes.Store(0)
	s.encryptedBytes.Store(0)
	s.hashedBytes.Store(0)
	s.readContents.Store(0)
	s.writtenContents.Store(0)
	s.hashedContents.Store(0)
	s.invalidContents.Store(0)
	s.validContents.Store(0)
}

// ReadContent returns the approximate read content count and their total size in bytes.
func (s *Stats) ReadContent() (count uint32, bytes int64) {
	return s.readContents.Load(), s.readBytes.Load()
}

// WrittenContent returns the approximate written content count and their total size in bytes.
func (s *Stats) WrittenContent() (count uint32, bytes int64) {
	return s.writtenContents.Load(), s.writtenBytes.Load()
}

// HashedContent returns the approximate hashed content count and their total size in bytes.
func (s *Stats) HashedContent() (count uint32, bytes int64) {
	return s.hashedContents.Load(), s.hashedBytes.Load()
}

// DecryptedBytes returns the approximate total number of decrypted bytes.
func (s *Stats) DecryptedBytes() int64 {
	return s.decryptedBytes.Load()
}

// EncryptedBytes returns the approximate total number of decrypted bytes.
func (s *Stats) EncryptedBytes() int64 {
	return s.encryptedBytes.Load()
}

// InvalidContents returns the approximate count of invalid contents found.
func (s *Stats) InvalidContents() uint32 {
	return s.invalidContents.Load()
}

// ValidContents returns the approximate count of valid contents found.
func (s *Stats) ValidContents() uint32 {
	return s.validContents.Load()
}

func (s *Stats) decrypted(size int) int64 {
	return s.decryptedBytes.Add(int64(size))
}

func (s *Stats) encrypted(size int) int64 {
	return s.encryptedBytes.Add(int64(size))
}

func (s *Stats) readContent(size int) (count uint32, sum int64) {
	return s.readContents.Add(1), s.readBytes.Add(int64(size))
}

func (s *Stats) wroteContent(size int) (count uint32, sum int64) {
	return s.writtenContents.Add(1), s.writtenBytes.Add(int64(size))
}

func (s *Stats) hashedContent(size int) (count uint32, sum int64) {
	return s.hashedContents.Add(1), s.hashedBytes.Add(int64(size))
}

func (s *Stats) foundValidContent() uint32 {
	return s.validContents.Add(1)
}

func (s *Stats) foundInvalidContent() uint32 {
	return s.invalidContents.Add(1)
}
