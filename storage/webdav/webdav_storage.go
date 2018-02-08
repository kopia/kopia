// Package webdav implements WebDAV-based Storage.
package webdav

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"

	"github.com/kopia/kopia/storage"
)

const (
	davStorageType       = "webdav"
	fsStorageChunkSuffix = ".f"
)

var (
	fsDefaultShards = []int{3, 3}
)

// davStorage implements blob.Storage on top of remove WebDAV repository.
// It is very similar to File storage, except uses HTTP URLs instead of local files.
// Storage formats are compatible (both use sharded directory structure), so a repository
// may be accessed using WebDAV or File interchangeably.
type davStorage struct {
	clientNonceCount int32
	Options

	Client *http.Client // HTTP client used when making all calls, may be overridden to use custom auth
}

func (d *davStorage) GetBlock(blockID string, offset, length int64) ([]byte, error) {
	_, urlStr := d.getCollectionAndFileURL(blockID)

	req, err := http.NewRequest("GET", urlStr, nil)
	if err != nil {
		return nil, err
	}

	if length > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%v-%v", offset, offset+length-1))
	}

	resp, err := d.executeRequest(req, blockInfoRequest)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNotFound:
		return nil, storage.ErrBlockNotFound
	case http.StatusOK, http.StatusPartialContent:
		return ioutil.ReadAll(resp.Body)
	default:
		return nil, fmt.Errorf("unsupported response code %v during GET %q", resp.StatusCode, urlStr)
	}
}

func getstringFromFileName(name string) (string, bool) {
	if strings.HasSuffix(name, fsStorageChunkSuffix) {
		return string(name[0 : len(name)-len(fsStorageChunkSuffix)]), true
	}

	return string(""), false
}

func makeFileName(blockID string) string {
	return string(blockID) + fsStorageChunkSuffix
}

func (d *davStorage) ListBlocks(prefix string) (<-chan storage.BlockMetadata, storage.CancelFunc) {
	result := make(chan storage.BlockMetadata)
	cancelled := make(chan bool)

	prefixString := string(prefix)

	var walkDir func(string, string)

	walkDir = func(urlStr string, currentPrefix string) {
		if entries, err := d.propFindChildren(urlStr); err == nil {
			for _, e := range entries {
				if e.isCollection {
					newPrefix := currentPrefix + e.name
					var match bool

					if len(prefixString) > len(newPrefix) {
						match = strings.HasPrefix(prefixString, newPrefix)
					} else {
						match = strings.HasPrefix(newPrefix, prefixString)
					}

					if match {
						walkDir(urlStr+"/"+e.name, currentPrefix+e.name)
					}
				} else if fullID, ok := getstringFromFileName(currentPrefix + e.name); ok {
					if strings.HasPrefix(string(fullID), prefixString) {
						select {
						case <-cancelled:
							return
						case result <- storage.BlockMetadata{
							BlockID:   fullID,
							Length:    e.length,
							TimeStamp: e.modTime,
						}:
						}
					}
				}
			}
		}
	}

	walkDirAndClose := func(urlStr string) {
		walkDir(urlStr, "")
		close(result)
	}

	go walkDirAndClose(d.URL)
	return result, func() {
		close(cancelled)
	}
}

func (d *davStorage) makeCollectionAll(urlStr string) error {
	err := d.makeCollection(urlStr)
	switch err {
	case nil:
		return nil

	case storage.ErrBlockNotFound:
		parent := getParentURL(urlStr)
		if parent == "" {
			return fmt.Errorf("can't create %q", urlStr)
		}
		if err := d.makeCollectionAll(parent); err != nil {
			return err
		}

		return d.makeCollection(urlStr)

	default:
		return err
	}
}

func (d *davStorage) makeCollection(urlStr string) error {
	req, err := http.NewRequest("MKCOL", urlStr, nil)
	if err != nil {
		return err
	}

	resp, err := d.executeRequest(req, nil)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusConflict:
		return storage.ErrBlockNotFound
	case http.StatusOK, http.StatusCreated:
		return nil
	default:
		return fmt.Errorf("unhandled status code %v when MKCOL %q", resp.StatusCode, urlStr)
	}
}

func getParentURL(u string) string {
	p := strings.LastIndex(u, "/")
	if p >= 0 {
		return u[0:p]
	}

	return ""
}

func (d *davStorage) delete(urlStr string) error {
	req, err := http.NewRequest("DELETE", urlStr, nil)
	if err != nil {
		return err
	}

	resp, err := d.executeRequest(req, nil)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK, http.StatusNotFound:
		return nil
	default:
		return fmt.Errorf("unhandled status code %v during DELETE %q", resp.StatusCode, urlStr)
	}
}

func (d *davStorage) move(urlOld, urlNew string) error {
	req, err := http.NewRequest("MOVE", urlOld, nil)
	if err != nil {
		return err
	}
	req.Header.Add("Destination", urlNew)
	req.Header.Add("Overwrite", "T")

	resp, err := d.executeRequest(req, nil)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK, http.StatusCreated, http.StatusNoContent:
		return nil

	default:
		return fmt.Errorf("unhandled status code %v during MOVE %q to %q", resp.StatusCode, urlOld, urlNew)
	}
}

func (d *davStorage) putBlockInternal(urlStr string, data []byte) error {
	req, err := http.NewRequest("PUT", urlStr, nil)
	if err != nil {
		return err
	}

	resp, err := d.executeRequest(req, data)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK, http.StatusCreated:
		return nil

	case http.StatusNotFound:
		return storage.ErrBlockNotFound

	default:
		return fmt.Errorf("invalid response from webdav server: %v", resp.StatusCode)
	}
}

func (d *davStorage) PutBlock(blockID string, data []byte) error {
	shardPath, url := d.getCollectionAndFileURL(blockID)

	tmpURL := url + "-" + makeClientNonce()
	err := d.putBlockInternal(tmpURL, data)

	if err == storage.ErrBlockNotFound {
		if err := d.makeCollectionAll(shardPath); err != nil {
			return err
		}

		err = d.putBlockInternal(tmpURL, data)
	}

	if err != nil {
		return err
	}

	if err := d.move(tmpURL, url); err != nil {
		d.delete(tmpURL)
		return err
	}

	return nil
}

func (d *davStorage) DeleteBlock(blockID string) error {
	_, url := d.getCollectionAndFileURL(blockID)
	err := os.Remove(url)
	if err == nil || os.IsNotExist(err) {
		return nil
	}

	return err
}

func (d *davStorage) getCollectionURL(blockID string) (string, string) {
	shardPath := d.URL
	blockIDString := string(blockID)
	if len(blockIDString) < 20 {
		return shardPath, blockID
	}
	for _, size := range d.shards() {
		shardPath = shardPath + "/" + blockIDString[0:size]
		blockIDString = blockIDString[size:]
	}

	return shardPath, string(blockIDString)
}

func (d *davStorage) getCollectionAndFileURL(blockID string) (string, string) {
	shardURL, blockID := d.getCollectionURL(blockID)
	result := shardURL + "/" + makeFileName(blockID)
	return shardURL, result
}

func (d *davStorage) ConnectionInfo() storage.ConnectionInfo {
	return storage.ConnectionInfo{
		Type:   davStorageType,
		Config: &d.Options,
	}
}

func (d *davStorage) Close() error {
	return nil
}

// New creates new WebDAV-backed storage in a specified URL.
func New(ctx context.Context, opts *Options) (storage.Storage, error) {
	r := &davStorage{
		Options: *opts,
		Client:  http.DefaultClient,
	}

	for _, s := range r.shards() {
		if s == 0 {
			return nil, fmt.Errorf("invalid shard spec: %v", opts.DirectoryShards)
		}
	}

	r.Options.URL = strings.TrimSuffix(r.Options.URL, "/")
	return r, nil
}

func init() {
	storage.AddSupportedStorage(
		davStorageType,
		func() interface{} { return &Options{} },
		func(ctx context.Context, o interface{}) (storage.Storage, error) {
			return New(ctx, o.(*Options))
		})
}
