package block

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/kopia/kopia/storage"
)

const (
	sweepCacheFrequency = 1 * time.Minute
	cachedSuffix        = ".cached"
)

type diskBlockCache struct {
	st                storage.Storage
	directory         string
	maxSizeBytes      int64
	listCacheDuration time.Duration
	hmacSecret        []byte

	closed chan struct{}
}

func (c *diskBlockCache) getBlock(blockID string, offset, length int64) ([]byte, error) {
	fn := c.cachedItemName(blockID)

	b, err := ioutil.ReadFile(fn)
	if err == nil {
		b, err := c.verifyHMAC(b)
		if err == nil {
			// retrieved from blockCache and HMAC valid
			return applyOffsetAndLength(b, offset, length)
		}

		// ignore malformed blocks
		log.Printf("warning: malformed block %v: %v", blockID, err)
	} else if !os.IsNotExist(err) {
		log.Printf("warning: unable to read blockCache file %v: %v", fn, err)
	}

	b, err = c.st.GetBlock(blockID, 0, -1)
	if err == storage.ErrBlockNotFound {
		// not found in underlying storage
		return nil, err
	}

	if err == nil {
		//log.Printf("adding %v to blockCache", blockID)
		if err := c.writeFileAtomic(fn, c.appendHMAC(b)); err != nil {
			log.Printf("warning: unable to write file %v: %v", fn, err)
		}
	}

	return applyOffsetAndLength(b, offset, length)
}

func applyOffsetAndLength(b []byte, offset, length int64) ([]byte, error) {
	if offset > int64(len(b)) {
		return nil, fmt.Errorf("offset of bounds (offset=%v, length=%v, actual length=%v)", offset, length, len(b))
	}

	if length < 0 {
		return b[offset:], nil
	}

	if offset+length > int64(len(b)) {
		return nil, fmt.Errorf("length of bounds (offset=%v, length=%v, actual length=%v)", offset, length, len(b))
	}

	return b[offset : offset+length], nil
}

func (c *diskBlockCache) putBlock(blockID string, data []byte) error {
	err := c.st.PutBlock(blockID, data)
	if err != nil {
		return err
	}

	c.writeFileAtomic(filepath.Join(c.directory, blockID)+cachedSuffix, c.appendHMAC(data))
	c.deleteListCache()
	return nil
}

func (c *diskBlockCache) listIndexBlocks(full bool) ([]Info, error) {
	var cachedListFile string

	if full {
		cachedListFile = c.cachedItemName("list-full")
	} else {
		cachedListFile = c.cachedItemName("list-active")
	}

	f, err := os.Open(cachedListFile)
	if err == nil {
		defer f.Close()

		st, err := f.Stat()
		if err == nil {
			expirationTime := st.ModTime().UTC().Add(c.listCacheDuration)
			if time.Now().UTC().Before(expirationTime) {
				log.Debug().Bool("full", full).Msg("listing index blocks from cache")
				return c.readBlocksFromCacheFile(f)
			}
		}
	}

	log.Debug().Bool("full", full).Msg("listing index blocks from source")
	blocks, err := listIndexBlocksFromStorage(c.st, full)
	if err == nil {
		log.Debug().Bool("full", full).Msg("saving index blocks to cache")
		// save to blockCache
		if data, err := json.Marshal(blocks); err == nil {
			if err := c.writeFileAtomic(cachedListFile, c.appendHMAC(data)); err != nil {
				log.Printf("warning: can't save list: %v", err)
			}
		}
	}

	return blocks, err
}

func (c *diskBlockCache) cachedItemName(name string) string {
	return filepath.Join(c.directory, name+cachedSuffix)
}

func (c *diskBlockCache) readBlocksFromCacheFile(f *os.File) ([]Info, error) {
	var blocks []Info
	data, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}

	data, err = c.verifyHMAC(data)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(data, &blocks); err != nil {
		return nil, fmt.Errorf("can't unmarshal cached list results: %v", err)
	}

	return blocks, nil

}

func (c *diskBlockCache) readBlocksFromSource(maxCompactions int) ([]Info, error) {
	var blocks []Info
	ch, cancel := c.st.ListBlocks(indexBlockPrefix)
	defer cancel()

	numCompactions := 0
	for e := range ch {
		log.Printf("found block %+v", e)
		if e.Error != nil {
			return nil, e.Error
		}

		blocks = append(blocks, Info{
			BlockID:   e.BlockID,
			Length:    e.Length,
			Timestamp: e.TimeStamp,
		})

		if _, ok := getCompactedTimestamp(e.BlockID); ok {
			numCompactions++
			log.Printf("found compaction %v / %v", numCompactions, maxCompactions)
			if numCompactions >= maxCompactions {
				break
			}
		}
	}
	return blocks, nil
}

func (c *diskBlockCache) appendHMAC(data []byte) []byte {
	h := hmac.New(sha256.New, c.hmacSecret)
	h.Write(data)
	validSignature := h.Sum(nil)
	return append(append([]byte(nil), data...), validSignature...)
}

func (c *diskBlockCache) verifyHMAC(b []byte) ([]byte, error) {
	if len(b) < sha256.Size {
		return nil, errors.New("invalid data - too short")
	}

	p := len(b) - sha256.Size
	data := b[0:p]
	signature := b[p:]
	h := hmac.New(sha256.New, c.hmacSecret)
	h.Write(data)
	validSignature := h.Sum(nil)
	if len(signature) != len(validSignature) {
		return nil, errors.New("invalid signature length")
	}
	if hmac.Equal(validSignature, signature) {
		return data, nil
	}

	return nil, errors.New("invalid data - corrupted")
}

func (c *diskBlockCache) writeFileAtomic(fname string, contents []byte) error {
	tn := filepath.Join(c.directory, fmt.Sprintf("tmp-%v.%v"+cachedSuffix, time.Now().UnixNano(), rand.Int63()))
	if err := ioutil.WriteFile(tn, contents, 0600); err != nil {
		if !os.IsNotExist(err) {
			return err
		}

		// create blockCache directory, and retry write
		os.MkdirAll(c.directory, 0700)
		if err := ioutil.WriteFile(tn, contents, 0600); err != nil {
			return err
		}
	}

	if err := os.Rename(tn, fname); err != nil {
		os.Remove(tn)
		return err
	}

	return nil
}

func (c *diskBlockCache) close() error {
	close(c.closed)
	return nil
}

func (c *diskBlockCache) sweepDirectoryPeriodically() {
	for {
		select {
		case <-c.closed:
			return

		case <-time.After(sweepCacheFrequency):
			if err := c.sweepDirectory(); err != nil {
				log.Printf("warning: blockCache sweep failed: %v", err)
			}
		}
	}
}

func (c *diskBlockCache) sweepDirectory() error {
	items, err := ioutil.ReadDir(c.directory)
	if os.IsNotExist(err) {
		// blockCache not found, that's ok
		return nil
	}
	if err != nil {
		return err
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].ModTime().Before(items[j].ModTime())
	})

	var totalSize int64
	for _, it := range items {
		if !strings.HasSuffix(it.Name(), cachedSuffix) {
			continue
		}
		if totalSize > c.maxSizeBytes {
			fn := filepath.Join(c.directory, it.Name())
			if err := os.Remove(fn); err != nil {
				log.Printf("warning: unable to remove %v: %v", fn, err)
			}
		}
		totalSize += it.Size()
	}
	return nil
}

func (c *diskBlockCache) deleteListCache() {
	os.Remove(c.cachedItemName("list-full"))
	os.Remove(c.cachedItemName("list-active"))
}
