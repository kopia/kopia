package index

import (
	"crypto/rand"
	"hash/fnv"
	"io"
	"runtime"
	"sort"
	"sync"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/gather"
)

const randomSuffixSize = 32 // number of random bytes to append at the end to make the index blob unique

// Builder prepares and writes content index.
type Builder map[ID]Info

// BuilderCreator is an interface for caller to add indexes to builders.
type BuilderCreator interface {
	Add(info Info)
}

// Clone returns a deep Clone of the Builder.
func (b Builder) Clone() Builder {
	if b == nil {
		return nil
	}

	r := Builder{}

	for k, v := range b {
		r[k] = v
	}

	return r
}

// Add adds a new entry to the builder or conditionally replaces it if the timestamp is greater.
func (b Builder) Add(i Info) {
	cid := i.ContentID

	old, found := b[cid]
	if !found || contentInfoGreaterThanStruct(&i, &old) {
		b[cid] = i
	}
}

// base36Value stores a base-36 reverse lookup such that ASCII character corresponds to its
// base-36 value ('0'=0..'9'=9, 'a'=10, 'b'=11, .., 'z'=35).
//
//nolint:gochecknoglobals
var base36Value [256]byte

func init() {
	for i := range 10 {
		base36Value['0'+i] = byte(i)
	}

	for i := range 26 {
		base36Value['a'+i] = byte(i + 10) //nolint:mnd
		base36Value['A'+i] = byte(i + 10) //nolint:mnd
	}
}

// sortedContents returns the list of []Info sorted lexicographically using bucket sort
// sorting is optimized based on the format of content IDs (optional single-character
// alphanumeric prefix (0-9a-z), followed by hexadecimal digits (0-9a-f).
func (b Builder) sortedContents() []*Info {
	var buckets [36 * 16][]*Info

	// phase 1 - bucketize into 576 (36 *16) separate lists
	// by first [0-9a-z] and second character [0-9a-f].
	for cid, v := range b {
		first := int(base36Value[cid.prefix])
		second := int(cid.data[0] >> 4) //nolint:mnd

		// first: 0..35, second: 0..15
		buck := first<<4 + second //nolint:mnd

		buckets[buck] = append(buckets[buck], &v)
	}

	// phase 2 - sort each non-empty bucket in parallel using goroutines
	// this is much faster than sorting one giant list.
	var wg sync.WaitGroup

	numWorkers := runtime.NumCPU()
	for worker := range numWorkers {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for i := range buckets {
				if i%numWorkers == worker {
					buck := buckets[i]

					sort.Slice(buck, func(i, j int) bool {
						return buck[i].ContentID.less(buck[j].ContentID)
					})
				}
			}
		}()
	}

	wg.Wait()

	// Phase 3 - merge results from all buckets.
	result := make([]*Info, 0, len(b))

	for i := range len(buckets) { //nolint:intrange
		result = append(result, buckets[i]...)
	}

	return result
}

// Build writes the pack index to the provided output.
func (b Builder) Build(output io.Writer, version int) error {
	if err := b.BuildStable(output, version); err != nil {
		return err
	}

	randomSuffix := make([]byte, randomSuffixSize)

	if _, err := rand.Read(randomSuffix); err != nil {
		return errors.Wrap(err, "error getting random bytes for suffix")
	}

	if _, err := output.Write(randomSuffix); err != nil {
		return errors.Wrap(err, "error writing extra random suffix to ensure indexes are always globally unique")
	}

	return nil
}

// BuildStable writes the pack index to the provided output.
func (b Builder) BuildStable(output io.Writer, version int) error {
	return buildSortedContents(b.sortedContents(), output, version)
}

func buildSortedContents(items []*Info, output io.Writer, version int) error {
	switch version {
	case Version1:
		return buildV1(items, output)

	case Version2:
		return buildV2(items, output)

	default:
		return errors.Errorf("unsupported index version: %v", version)
	}
}

func (b Builder) shard(maxShardSize int) []Builder {
	numShards := (len(b) + maxShardSize - 1) / maxShardSize
	if numShards <= 1 {
		if len(b) == 0 {
			return []Builder{}
		}

		return []Builder{b}
	}

	result := make([]Builder, numShards)
	for i := range result {
		result[i] = make(Builder)
	}

	for k, v := range b {
		h := fnv.New32a()
		io.WriteString(h, k.String()) //nolint:errcheck

		shard := h.Sum32() % uint32(numShards) //nolint:gosec

		result[shard][k] = v
	}

	var nonEmpty []Builder

	for _, r := range result {
		if len(r) > 0 {
			nonEmpty = append(nonEmpty, r)
		}
	}

	return nonEmpty
}

// BuildShards builds the set of index shards ensuring no more than the provided number of contents are in each index.
// Returns shard bytes and function to clean up after the shards have been written.
func (b Builder) BuildShards(indexVersion int, stable bool, shardSize int) ([]gather.Bytes, func(), error) {
	if shardSize == 0 {
		return nil, nil, errors.New("invalid shard size")
	}

	var (
		shardedBuilders = b.shard(shardSize)
		dataShardsBuf   []*gather.WriteBuffer
		dataShards      []gather.Bytes
		randomSuffix    [32]byte
	)

	closeShards := func() {
		for _, ds := range dataShardsBuf {
			ds.Close()
		}
	}

	for _, s := range shardedBuilders {
		buf := gather.NewWriteBuffer()

		dataShardsBuf = append(dataShardsBuf, buf)

		if err := s.BuildStable(buf, indexVersion); err != nil {
			closeShards()

			return nil, nil, errors.Wrap(err, "error building index shard")
		}

		if !stable {
			if _, err := rand.Read(randomSuffix[:]); err != nil {
				closeShards()

				return nil, nil, errors.Wrap(err, "error getting random bytes for suffix")
			}

			if _, err := buf.Write(randomSuffix[:]); err != nil {
				closeShards()

				return nil, nil, errors.Wrap(err, "error writing extra random suffix to ensure indexes are always globally unique")
			}
		}

		dataShards = append(dataShards, buf.Bytes())
	}

	return dataShards, closeShards, nil
}
