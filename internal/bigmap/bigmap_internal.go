// Package bigmap implements a custom hashmap data structure where keys and values are binary
// and keys are meant to be well-distributed hashes, such as content IDs, object IDs, etc.
//
// Unlike regular maps this map is limited to adding and getting elements but does not support
// any iteration or deletion, but is much more efficient in terms of memory usage.
//
// Data for the hash table is stored in large contiguous memory blocks (for small sets)
// and automatically spills over to memory-mapped files for larger sets using only 8 bytes
// per key in RAM.
package bigmap

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"sync"

	"github.com/edsrzf/mmap-go"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/tempfile"
	"github.com/kopia/kopia/repo/logging"
)

const (
	defaultNumMemorySegments    = 8          // number of segments to keep in RAM
	defaultMemorySegmentSize    = 18 * 1e6   // 18MB enough to store >1M 16-17-byte keys
	defaultFileSegmentSize      = 1024 << 20 // 1 GiB
	defaultInitialSizeLogarithm = 20

	// grow hash table above this percentage utilization, higher values (close to 100) will be very slow,
	// smaller values will waste memory.
	defaultLoadFactorPercentage = 75

	minKeyLength = 4 // first 4 bytes of each key are used as uint32 hash value
	maxKeyLength = 255

	minSizeLogarithm = 8
)

var log = logging.Module("bigmap")

// Options provides options for the internalMap.
type Options struct {
	LoadFactorPercentage int   // grow the size of the hash table when this percentage full
	NumMemorySegments    int   // number of segments to keep in RAM
	MemorySegmentSize    int64 // size of a single memory segment
	FileSegmentSize      int   // size of a single file segment, defaults to 1 GiB

	InitialSizeLogarithm int // logarithm of the initial size of the hash table, default - 20
}

// internalMap is a custom hashtable implementation using https://en.wikipedia.org/wiki/Double_hashing
// that stores all (key,value) pairs densely packed in segments of fixed size to minimize data
// fragmentation.
type internalMap struct {
	hasValues bool    // +checklocksignore
	opts      Options // +checklocksignore

	mu sync.RWMutex
	// +checklocks:mu
	tableSizeIndex int // index into tableSizesPrimes == len(slots)
	// +checklocks:mu
	segments []mmap.MMap // ([keyLength][key][valueLength][value])*
	// +checklocks:mu
	slots []entry // current hash table slots

	// +checklocks:mu
	count int // number of elements in the hash tables
	// +checklocks:mu
	h2Prime uint64 // prime < len(slots)

	// +checklocks:mu
	cleanups []func()
}

// The list of prime numbers close to 2^N from https://primes.utm.edu/lists/2small/0bit.html

//nolint:gochecknoglobals
var tableSizesPrimes = []uint64{
	1<<8 - 59, // minSizeLogarithm
	1<<9 - 55,
	1<<10 - 57,
	1<<11 - 61,
	1<<12 - 77,
	1<<13 - 91,
	1<<14 - 111,
	1<<15 - 135,
	1<<16 - 123,
	1<<17 - 99,
	1<<18 - 93,
	1<<19 - 87,
	1<<20 - 185, // initial H2 prime
	1<<21 - 129, // initial number of hashtable slots
	1<<22 - 123,
	1<<23 - 159,
	1<<24 - 167,
	1<<25 - 183,
	1<<26 - 135,
	1<<27 - 235,
	1<<28 - 273,
	1<<29 - 133,
	1<<30 - 173,
	1<<31 - 171,
	1<<32 - 267,
	1<<33 - 355,
	1<<34 - 281,
	1<<35 - 325,
	1<<36 - 233,
	1<<37 - 375,
	1<<38 - 257,
	1<<39 - 301,
	1<<40 - 437,
	1<<41 - 139,
	1<<42 - 227,
	1<<43 - 369,
	1<<44 - 377,
	1<<45 - 229,
	1<<46 - 311,
	1<<47 - 649,
	1<<48 - 257,
	1<<49 - 339,
	1<<50 - 233,
	1<<51 - 439,
	1<<52 - 395,
	1<<53 - 421,
	1<<54 - 327,
	1<<55 - 267,
	1<<56 - 195,
	1<<57 - 423,
	1<<58 - 251,
	1<<59 - 871,
	1<<60 - 453,
	1<<61 - 465,
	1<<62 - 273,
	1<<63 - 471,
}

type entry struct {
	segment uint32 // 0-empty, otherwise index into internalMap.segments+1
	offset  uint32 // offset within a segment
}

// Contains returns true if the provided key is in the map.
func (m *internalMap) Contains(key []byte) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	slot := m.findSlot(key)

	return m.slots[slot].segment != 0
}

// Get gets the value associated with a given key. It is appended to the provided buffer.
func (m *internalMap) Get(buf, key []byte) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	slot := m.findSlot(key)
	e := m.slots[slot]

	if e.segment == 0 {
		return nil, false
	}

	if !m.hasValues {
		return nil, true
	}

	data := m.segments[e.segment-1] // 1-indexed
	koff := e.offset

	off := koff + uint32(data[koff]) + 1
	vlen, vlenLen := binary.Uvarint(data[off:])
	start := off + uint32(vlenLen) //nolint:gosec

	//nolint:gosec
	return append(buf, data[start:start+uint32(vlen)]...), true
}

func (m *internalMap) hashValue(key []byte) uint64 {
	if len(key) < 8 { //nolint:mnd
		return uint64(binary.BigEndian.Uint32(key))
	}

	return binary.BigEndian.Uint64(key)
}

// h2 returns the secondary hash value used for double hashing.
func (m *internalMap) h2(key []byte) uint64 {
	if len(key) < 16 { //nolint:mnd
		// use linear scan.
		return 1
	}

	return binary.BigEndian.Uint64(key[8:])
}

// +checklocksread:m.mu
func (m *internalMap) keyEquals(e entry, key []byte) bool {
	data := m.segments[e.segment-1] // 1-indexed
	koff := e.offset
	keyLen := uint32(data[koff])

	return bytes.Equal(key, data[koff+1:koff+1+keyLen])
}

// +checklocksread:m.mu
func (m *internalMap) findSlotInSlice(key []byte, slots []entry, h2Prime uint64) uint64 {
	slot := m.hashValue(key) % uint64(len(slots))

	delta := m.h2(key) % h2Prime
	if delta == 0 {
		delta = 1
	}

	for slots[slot].segment != 0 && !m.keyEquals(slots[slot], key) {
		slot = (slot + delta) % uint64(len(slots))
	}

	return slot
}

// +checklocksread:m.mu
func (m *internalMap) findSlot(key []byte) uint64 {
	return m.findSlotInSlice(key, m.slots, m.h2Prime)
}

// +checklocks:m.mu
func (m *internalMap) growLocked(newSize uint64) {
	newSlots := make([]entry, newSize)
	newH2Prime := uint64(len(m.slots))

	for segNum, seg := range m.segments {
		p := 0

		for p < len(seg) && seg[p] != 0 {
			koff := p
			key := seg[p+1 : p+1+int(seg[p])]
			p += len(key) + 1

			if m.hasValues {
				vlen, vlenLen := binary.Uvarint(seg[p:])
				p += vlenLen + int(vlen) //nolint:gosec
			}

			slot := m.findSlotInSlice(key, newSlots, newH2Prime)
			newSlots[slot] = entry{segment: uint32(segNum) + 1, offset: uint32(koff)} //nolint:gosec
		}
	}

	m.h2Prime = newH2Prime
	m.slots = newSlots
}

// PutIfAbsent conditionally adds the provided (key, value) to the map if the provided key is absent.
func (m *internalMap) PutIfAbsent(ctx context.Context, key, value []byte) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(key) < minKeyLength {
		panic("key too short")
	}

	if len(key) > maxKeyLength {
		panic("key too long")
	}

	if !m.hasValues && len(value) > 0 {
		panic("values are not enabled")
	}

	slot := m.findSlot(key)
	if m.slots[slot].segment != 0 {
		return false
	}

	if m.count >= len(m.slots)*m.opts.LoadFactorPercentage/100 {
		m.tableSizeIndex++
		m.growLocked(tableSizesPrimes[m.tableSizeIndex])
		slot = m.findSlot(key)
	}

	payloadLength := 1 + len(key) + binary.MaxVarintLen32 + len(value)

	current := m.segments[len(m.segments)-1]

	// ensure that the key we're adding fits in the segment, start a new segment if not enough room.
	if len(current)+payloadLength > cap(current) {
		current = m.newSegment(ctx)
		m.segments = append(m.segments, current)
	}

	koff := uint32(len(current)) //nolint:gosec

	current = append(current, byte(len(key)))
	current = append(current, key...)

	// append the value
	if m.hasValues {
		var vlen [binary.MaxVarintLen32]byte

		n := binary.PutUvarint(vlen[:], uint64(len(value)))

		current = append(current, vlen[0:n]...)
		current = append(current, value...)
	}

	m.segments[len(m.segments)-1] = current
	m.count++

	m.slots[slot] = entry{
		//nolint:gosec
		segment: uint32(len(m.segments)), // this is 1-based, 0==empty slot
		offset:  koff,
	}

	return true
}

// +checklocks:m.mu
func (m *internalMap) newMemoryMappedSegment(ctx context.Context) (mmap.MMap, error) {
	flags := 0

	f, err := m.maybeCreateMappedFile(ctx)
	if err != nil {
		return nil, err
	}

	s, err := mmap.MapRegion(f, m.opts.FileSegmentSize, mmap.RDWR, flags, 0)
	if err != nil {
		return nil, errors.Wrap(err, "unable to map region")
	}

	m.cleanups = append(m.cleanups, func() {
		if err := s.Unmap(); err != nil {
			log(ctx).Warnf("unable to unmap memory region: %v", err)
		}
	})

	return s[:0], nil
}

// +checklocks:m.mu
func (m *internalMap) maybeCreateMappedFile(ctx context.Context) (*os.File, error) {
	f, err := tempfile.Create("")
	if err != nil {
		return nil, errors.Wrap(err, "unable to create memory-mapped file")
	}

	if err := f.Truncate(int64(m.opts.FileSegmentSize)); err != nil {
		closeFile(ctx, f)

		return nil, errors.Wrap(err, "unable to truncate memory-mapped file")
	}

	m.cleanups = append(m.cleanups, func() {
		closeFile(ctx, f)
	})

	return f, nil
}

func closeFile(ctx context.Context, f *os.File) {
	if err := f.Close(); err != nil {
		log(ctx).Warnf("unable to close segment file: %v", err)
	}
}

// +checklocks:m.mu
func (m *internalMap) newSegment(ctx context.Context) mmap.MMap {
	var s mmap.MMap

	if len(m.segments) >= m.opts.NumMemorySegments {
		var err error

		s, err = m.newMemoryMappedSegment(ctx)
		if err != nil {
			log(ctx).Warnf("unable to create memory-mapped segment: %v", err)
		}
	}

	if s == nil {
		s = make([]byte, 0, m.opts.MemorySegmentSize)
	}

	return s
}

// Close releases all resources associated with a map.
func (m *internalMap) Close(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i := len(m.cleanups) - 1; i >= 0; i-- {
		m.cleanups[i]()
	}

	m.cleanups = nil
	m.segments = nil
}

// newInternalMap creates new internalMap.
func newInternalMap(ctx context.Context) (*internalMap, error) {
	return newInternalMapWithOptions(ctx, true, nil)
}

// newInternalMapWithOptions creates a new instance of internalMap.
func newInternalMapWithOptions(ctx context.Context, hasValues bool, opts *Options) (*internalMap, error) {
	if opts == nil {
		opts = &Options{}
	}

	if opts.LoadFactorPercentage == 0 {
		opts.LoadFactorPercentage = defaultLoadFactorPercentage
	}

	if opts.MemorySegmentSize == 0 {
		opts.MemorySegmentSize = defaultMemorySegmentSize
	}

	if opts.NumMemorySegments == 0 {
		opts.NumMemorySegments = defaultNumMemorySegments
	}

	if opts.FileSegmentSize == 0 {
		opts.FileSegmentSize = defaultFileSegmentSize
	}

	if opts.InitialSizeLogarithm == 0 {
		opts.InitialSizeLogarithm = defaultInitialSizeLogarithm
	}

	tablewSizeIndex := opts.InitialSizeLogarithm - minSizeLogarithm

	if tablewSizeIndex < 1 {
		return nil, errors.New("invalid initial size")
	}

	m := &internalMap{
		hasValues:      hasValues,
		opts:           *opts,
		tableSizeIndex: tablewSizeIndex,
		h2Prime:        tableSizesPrimes[tablewSizeIndex-1], // h2 prime < number of slots
		slots:          make([]entry, tableSizesPrimes[tablewSizeIndex]),
	}

	m.mu.Lock()
	m.segments = append(m.segments, m.newSegment(ctx))
	m.mu.Unlock()

	return m, nil
}
