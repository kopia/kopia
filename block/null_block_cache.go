package block

import "github.com/kopia/kopia/storage"

type nullBlockCache struct {
	st storage.Storage
}

func (c nullBlockCache) getBlock(blockID string, offset, length int64) ([]byte, error) {
	return c.st.GetBlock(blockID, offset, length)
}

func (c nullBlockCache) putBlock(blockID string, data []byte) error {
	return c.st.PutBlock(blockID, data)
}

func (c nullBlockCache) listIndexBlocks() ([]Info, error) {
	ch, cancel := c.st.ListBlocks(packBlockPrefix)
	defer cancel()

	var results []Info
	for it := range ch {
		if it.Error != nil {
			return nil, it.Error
		}

		results = append(results, Info{
			BlockID:   it.BlockID,
			Timestamp: it.TimeStamp,
			Length:    it.Length,
		})
	}

	return results, nil
}
