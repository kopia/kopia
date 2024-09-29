package index

import (
	"crypto/rand"
	"hash/fnv"
	"io"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
	"github.com/petar/GoLLRB/llrb"
)

func (ic *InfoCompact) Less(other llrb.Item) bool {
	return ic.ContentID.less(other.(*InfoCompact).ContentID) //nolint:forcetypeassert
}

type blobIDWrap struct {
	id *blob.ID
}

func (b blobIDWrap) Less(other llrb.Item) bool {
	return *b.id < *other.(blobIDWrap).id //nolint:forcetypeassert
}

type oneUseBuilder struct {
	indexStore  *llrb.LLRB
	packBlobIDs *llrb.LLRB
}

func NewOneUseBuilder() *oneUseBuilder {
	return &oneUseBuilder{
		indexStore:  llrb.New(),
		packBlobIDs: llrb.New(),
	}
}

// Add adds a new entry to the builder or conditionally replaces it if the timestamp is greater.
func (b *oneUseBuilder) Add(i Info) {
	cid := i.ContentID

	found := b.indexStore.Get(&InfoCompact{ContentID: cid})
	if found == nil || contentInfoGreaterThanStruct(&i, &Info{
		PackBlobID:       *found.(*InfoCompact).PackBlobID,      //nolint:forcetypeassert
		TimestampSeconds: found.(*InfoCompact).TimestampSeconds, //nolint:forcetypeassert
		Deleted:          found.(*InfoCompact).Deleted,          //nolint:forcetypeassert
	}) {
		id := new(blob.ID)
		if item := b.packBlobIDs.Get(blobIDWrap{&i.PackBlobID}); item == nil {
			*id = i.PackBlobID
			_ = b.packBlobIDs.ReplaceOrInsert(blobIDWrap{id})
		} else {
			id = item.(blobIDWrap).id
		}

		_ = b.indexStore.ReplaceOrInsert(&InfoCompact{
			PackBlobID:          id,
			ContentID:           cid,
			TimestampSeconds:    i.TimestampSeconds,
			OriginalLength:      i.OriginalLength,
			PackedLength:        i.PackedLength,
			PackOffset:          i.PackOffset,
			CompressionHeaderID: i.CompressionHeaderID,
			Deleted:             i.Deleted,
			FormatVersion:       i.FormatVersion,
			EncryptionKeyID:     i.EncryptionKeyID,
		})
	}
}

func (b *oneUseBuilder) Length() int {
	return b.indexStore.Len()
}

func (b *oneUseBuilder) sortedContents() []BuilderItem {
	result := []BuilderItem{}

	for b.indexStore.Len() > 0 {
		item := b.indexStore.DeleteMin()
		result = append(result, item.(*InfoCompact))
	}

	return result
}

func (b *oneUseBuilder) shard(maxShardSize int) [][]BuilderItem {
	numShards := (b.Length() + maxShardSize - 1) / maxShardSize
	if numShards <= 1 {
		if b.Length() == 0 {
			return [][]BuilderItem{}
		}

		return [][]BuilderItem{b.sortedContents()}
	}

	result := make([][]BuilderItem, numShards)

	for b.indexStore.Len() > 0 {
		item := b.indexStore.DeleteMin()

		h := fnv.New32a()
		io.WriteString(h, item.(*InfoCompact).ContentID.String()) //nolint:errcheck

		shard := h.Sum32() % uint32(numShards) //nolint:gosec

		result[shard] = append(result[shard], item.(*InfoCompact))
	}

	var nonEmpty [][]BuilderItem

	for _, r := range result {
		if len(r) > 0 {
			nonEmpty = append(nonEmpty, r)
		}
	}

	return nonEmpty
}

// BuildShards builds the set of index shards ensuring no more than the provided number of contents are in each index.
// Returns shard bytes and function to clean up after the shards have been written.
func (b *oneUseBuilder) BuildShards(indexVersion int, stable bool, shardSize int) ([]gather.Bytes, func(), error) {
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
