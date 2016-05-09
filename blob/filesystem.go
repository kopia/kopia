package blob

import (
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	fsStorageType        = "filesystem"
	fsStorageChunkSuffix = ".f"
)

var (
	fsDefaultShards               = []int{1, 3, 3}
	fsDefaultFileMode os.FileMode = 0664
	fsDefaultDirMode  os.FileMode = 0775
)

type fsStorage struct {
	FSStorageOptions
}

// FSStorageOptions defines options for Filesystem-backed blob.
type FSStorageOptions struct {
	Path string `json:"path"`

	DirectoryShards []int `json:"dirShards,omitempty"`

	FileMode      os.FileMode `json:"fileMode,omitempty"`
	DirectoryMode os.FileMode `json:"dirMode,omitempty"`

	FileUID *int `json:"uid,omitempty"`
	FileGID *int `json:"gid,omitempty"`
}

func (fso *FSStorageOptions) fileMode() os.FileMode {
	if fso.FileMode == 0 {
		return fsDefaultFileMode
	}

	return fso.FileMode
}

func (fso *FSStorageOptions) dirMode() os.FileMode {
	if fso.DirectoryMode == 0 {
		return fsDefaultDirMode
	}

	return fso.DirectoryMode
}

func (fso *FSStorageOptions) shards() []int {
	if fso.DirectoryShards == nil {
		return fsDefaultShards
	}

	return fso.DirectoryShards
}

// ParseURL parses the given URL into FSStorageOptions.
func (fso *FSStorageOptions) ParseURL(u *url.URL) error {
	if u.Scheme != fsStorageType {
		return fmt.Errorf("invalid scheme, expected 'file'")
	}

	//log.Printf("u.Upaque: %v u.Path: %v", u.Opaque, u.Path)

	if u.Opaque != "" {
		fso.Path = u.Opaque
	} else {
		fso.Path = u.Path
	}
	fso.FileUID = getIntPtrValue(u, "uid", 10)
	fso.FileGID = getIntPtrValue(u, "gid", 10)
	fso.FileMode = getFileModeValue(u, "filemode", 0)
	fso.DirectoryMode = getFileModeValue(u, "dirmode", 0)
	if s := u.Query().Get("shards"); s != "" {
		parts := strings.Split(s, ".")
		shards := make([]int, len(parts))
		for i, p := range parts {
			var err error
			shards[i], err = strconv.Atoi(p)
			if err != nil {
				return err
			}
		}
		fso.DirectoryShards = shards
	}
	return nil
}

// ToURL converts the FSStorageOptions to URL.
func (fso *FSStorageOptions) ToURL() *url.URL {
	u := &url.URL{}
	u.Scheme = "filesystem"
	u.Opaque = fso.Path
	q := u.Query()
	if fso.FileUID != nil {
		q.Add("uid", strconv.Itoa(*fso.FileUID))
	}
	if fso.FileGID != nil {
		q.Add("gid", strconv.Itoa(*fso.FileGID))
	}
	if fso.FileMode != 0 {
		q.Add("filemode", strconv.FormatUint(uint64(fso.FileMode), 8))
	}
	if fso.DirectoryMode != 0 {
		q.Add("dirmode", strconv.FormatUint(uint64(fso.DirectoryMode), 8))
	}
	if fso.DirectoryShards != nil {
		shardsString := ""
		for i, s := range fso.DirectoryShards {
			if i > 0 {
				shardsString += "."
			}
			shardsString += fmt.Sprintf("%v", s)
		}
		q.Add("shards", shardsString)
	}
	u.RawQuery = q.Encode()
	return u
}

func (fs *fsStorage) BlockExists(blockID string) (bool, error) {
	_, path := fs.getShardedPathAndFilePath(blockID)
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}

	if os.IsNotExist(err) {
		return false, nil
	}

	return false, err
}

func (fs *fsStorage) GetBlock(blockID string) ([]byte, error) {
	_, path := fs.getShardedPathAndFilePath(blockID)
	d, err := ioutil.ReadFile(path)
	if err == nil {
		return d, err
	}

	if os.IsNotExist(err) {
		return nil, ErrBlockNotFound
	}

	return nil, err
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

func (fs *fsStorage) ListBlocks(prefix string) chan (BlockMetadata) {
	result := make(chan (BlockMetadata))

	prefixString := string(prefix)

	var walkDir func(string, string)

	walkDir = func(directory string, currentPrefix string) {
		if entries, err := ioutil.ReadDir(directory); err == nil {
			//log.Println("Walking", directory, "looking for", prefix)

			for _, e := range entries {
				if e.IsDir() {
					newPrefix := currentPrefix + e.Name()
					var match bool

					if len(prefixString) > len(newPrefix) {
						match = strings.HasPrefix(prefixString, newPrefix)
					} else {
						match = strings.HasPrefix(newPrefix, prefixString)
					}

					if match {
						walkDir(directory+"/"+e.Name(), currentPrefix+e.Name())
					}
				} else if fullID, ok := getstringFromFileName(currentPrefix + e.Name()); ok {
					if strings.HasPrefix(string(fullID), prefixString) {
						result <- BlockMetadata{
							BlockID:   fullID,
							Length:    uint64(e.Size()),
							TimeStamp: e.ModTime(),
						}
					}
				}
			}
		}
	}

	walkDirAndClose := func(directory string) {
		walkDir(directory, "")
		close(result)
	}

	go walkDirAndClose(fs.Path)
	return result
}

func (fs *fsStorage) PutBlock(blockID string, data io.ReadCloser, options PutOptions) error {
	// Close the data reader regardless of whether we use it or not.
	defer data.Close()

	shardPath, path := fs.getShardedPathAndFilePath(blockID)

	// Open temporary file, create dir if required.
	tempFile := fmt.Sprintf("%s.tmp.%d", path, rand.Int())
	flags := os.O_CREATE | os.O_WRONLY | os.O_EXCL
	f, err := os.OpenFile(tempFile, flags, fs.fileMode())
	if os.IsNotExist(err) {
		if err = os.MkdirAll(shardPath, fs.dirMode()); err != nil {
			return fmt.Errorf("cannot create directory: %v", err)
		}
		f, err = os.OpenFile(tempFile, flags, fs.fileMode())
	}

	if err != nil {
		return fmt.Errorf("cannot create temporary file: %v", err)
	}

	// Copy data to the temporary file.
	io.Copy(f, data)
	f.Close()

	err = os.Rename(tempFile, path)
	if err != nil {
		os.Remove(tempFile)
		return err
	}

	if fs.FileUID != nil && fs.FileGID != nil && os.Geteuid() == 0 {
		os.Chown(path, *fs.FileUID, *fs.FileGID)
	}

	return nil
}

func (fs *fsStorage) DeleteBlock(blockID string) error {
	_, path := fs.getShardedPathAndFilePath(blockID)
	err := os.Remove(path)
	if err == nil || os.IsNotExist(err) {
		return nil
	}

	return err
}

func (fs *fsStorage) Flush() error {
	return nil
}

func (fs *fsStorage) getShardDirectory(blockID string) (string, string) {
	shardPath := fs.Path
	blockIDString := string(blockID)
	if len(blockIDString) < 20 {
		return shardPath, blockID
	}
	for _, size := range fs.shards() {
		shardPath = filepath.Join(shardPath, blockIDString[0:size])
		blockIDString = blockIDString[size:]
	}

	return shardPath, string(blockIDString)
}

func (fs *fsStorage) getShardedPathAndFilePath(blockID string) (string, string) {
	shardPath, blockID := fs.getShardDirectory(blockID)
	result := filepath.Join(shardPath, makeFileName(blockID))
	return shardPath, result
}

func parseShardString(shardString string) ([]int, error) {
	if shardString == "" {
		// By default Xabcdefghijklmnop is stored in 'X/abc/def/Xabcdefghijklmnop'
		return fsDefaultShards, nil
	}

	result := make([]int, 0, 4)
	for _, value := range strings.Split(shardString, ",") {
		shardLength, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid shard specification: '%s'", value)
		}
		result = append(result, int(shardLength))
	}
	return result, nil
}

func (fs *fsStorage) Configuration() StorageConfiguration {
	return StorageConfiguration{
		fsStorageType,
		&fs.FSStorageOptions,
	}
}

// NewFSStorage creates new fs-backed storage in a specified directory.
func NewFSStorage(options *FSStorageOptions) (Storage, error) {
	var err error

	if _, err = os.Stat(options.Path); err != nil {
		return nil, fmt.Errorf("cannot access storage path: %v", err)
	}

	r := &fsStorage{
		FSStorageOptions: *options,
	}

	return r, nil
}

func init() {
	AddSupportedStorage(
		fsStorageType,
		func() StorageOptions { return &FSStorageOptions{} },
		func(o StorageOptions) (Storage, error) {
			return NewFSStorage(o.(*FSStorageOptions))
		})
}
