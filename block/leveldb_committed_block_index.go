package block

import (
	"encoding/json"
	"fmt"

	"github.com/kopia/kopia/storage"
	"github.com/rs/zerolog/log"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type levelDBCommittedBlockIndex struct {
	db *leveldb.DB
}

func (b *levelDBCommittedBlockIndex) getBlock(blockID ContentID) (Info, error) {
	v, err := b.db.Get([]byte("block-"+blockID), nil)
	if err == leveldb.ErrNotFound {
		return Info{}, storage.ErrBlockNotFound
	}

	var i Info
	if err := json.Unmarshal(v, &i); err != nil {
		return Info{}, fmt.Errorf("unable to unmarshal: %v", err)
	}
	return i, nil
}

func processedKey(indexBlockID PhysicalBlockID) []byte {
	return []byte("processed-" + indexBlockID)
}

func (b *levelDBCommittedBlockIndex) hasIndexBlockID(indexBlockID PhysicalBlockID) (bool, error) {
	_, err := b.db.Get(processedKey(indexBlockID), nil)
	if err == nil {
		return true, nil
	}

	if err == leveldb.ErrNotFound {
		return false, nil
	}

	return false, err
}

func (b *levelDBCommittedBlockIndex) commit(indexBlockID PhysicalBlockID, infos map[ContentID]Info) {
}

func (b *levelDBCommittedBlockIndex) load(indexBlockID PhysicalBlockID, indexes []packIndex) (int, error) {
	has, err := b.hasIndexBlockID(indexBlockID)
	if err != nil {
		return 0, err
	}
	if has {
		// already processed
		return 0, nil
	}

	var batch leveldb.Batch

	for _, ndx := range indexes {
		err := ndx.iterate(func(i Info) error {
			payload, err := json.Marshal(i)
			if err != nil {
				return err
			}
			batch.Put([]byte("block-"+string(i.BlockID)), payload)
			return nil
		})
		if err != nil {
			return 0, err
		}
	}
	batch.Put(processedKey(indexBlockID), []byte{1})
	log.Printf("applying batch of %v from %v", batch.Len(), indexBlockID)
	if err := b.db.Write(&batch, nil); err != nil {
		return 0, err
	}

	return 0, nil
}

func (b *levelDBCommittedBlockIndex) listBlocks(prefix ContentID, cb func(i Info) error) error {
	iter := b.db.NewIterator(util.BytesPrefix([]byte("block-"+prefix)), nil)
	defer iter.Release()

	for iter.Next() {
		val := iter.Value()

		var i Info
		if err := json.Unmarshal(val, &i); err != nil {
			return fmt.Errorf("unable to unmarshal: %v", i)
		}

		if i.Deleted {
			continue
		}

		if err := cb(i); err != nil {
			return err
		}

	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("unable to iterate cache: %v", err)
	}

	return nil
}

func newLevelDBCommittedBlockIndex(dirname string) (committedBlockIndex, error) {
	db, err := leveldb.OpenFile(dirname, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to open committed block index")
	}
	return &levelDBCommittedBlockIndex{db}, nil
}
