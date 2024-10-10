package index

import (
	"crypto/rand"
	"hash/fnv"
	"io"

	"github.com/pkg/errors"

	"github.com/petar/GoLLRB/llrb"

	"github.com/kopia/kopia/internal/gather"
)

// Less compares with another *Info by their ContentID and return true if the current one is smaller.
func (i *Info) Less(other llrb.Item) bool {
	return i.ContentID.less(other.(*Info).ContentID) //nolint:forcetypeassert
}

// OneUseBuilder prepares and writes content index for epoch index compaction.
type OneUseBuilder struct {
	indexStore *llrb.LLRB
}

// NewOneUseBuilder create a new instance of OneUseBuilder.
func NewOneUseBuilder() *OneUseBuilder {
	return &OneUseBuilder{
		indexStore: llrb.New(),
	}
}

// Add adds a new entry to the builder or conditionally replaces it if the timestamp is greater.
func (b *OneUseBuilder) Add(i Info) {
	found := b.indexStore.Get(&i)
	if found == nil || contentInfoGreaterThanStruct(&i, found.(*Info)) { //nolint:forcetypeassert
		_ = b.indexStore.ReplaceOrInsert(&i)
	}
}

// Length returns the number of indexes in the current builder.
func (b *OneUseBuilder) Length() int {
	return b.indexStore.Len()
}

func (b *OneUseBuilder) sortedContents() []*Info {
	result := []*Info{}

	for b.indexStore.Len() > 0 {
		item := b.indexStore.DeleteMin()
		result = append(result, item.(*Info)) //nolint:forcetypeassert
	}

	return result
}

func (b *OneUseBuilder) shard(maxShardSize int) [][]*Info {
	numShards := (b.Length() + maxShardSize - 1) / maxShardSize
	if numShards <= 1 {
		if b.Length() == 0 {
			return [][]*Info{}
		}

		return [][]*Info{b.sortedContents()}
	}

	result := make([][]*Info, numShards)

	for b.indexStore.Len() > 0 {
		item := b.indexStore.DeleteMin()

		h := fnv.New32a()
		io.WriteString(h, item.(*Info).ContentID.String()) //nolint:errcheck,forcetypeassert

		shard := h.Sum32() % uint32(numShards) //nolint:gosec

		result[shard] = append(result[shard], item.(*Info)) //nolint:forcetypeassert
	}

	var nonEmpty [][]*Info

	for _, r := range result {
		if len(r) > 0 {
			nonEmpty = append(nonEmpty, r)
		}
	}

	return nonEmpty
}

// BuildStable writes the pack index to the provided output.
func (b *OneUseBuilder) BuildStable(output io.Writer, version int) error {
	return buildSortedContents(b.sortedContents(), output, version)
}

// BuildShards builds the set of index shards ensuring no more than the provided number of contents are in each index.
// Returns shard bytes and function to clean up after the shards have been written.
func (b *OneUseBuilder) BuildShards(indexVersion int, stable bool, shardSize int) ([]gather.Bytes, func(), error) {
	if shardSize == 0 {
		return nil, nil, errors.Errorf("invalid shard size")
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

		if err := buildSortedContents(s, buf, indexVersion); err != nil {
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
