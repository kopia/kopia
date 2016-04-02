package blob

import (
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	fsStorageType = "fs"

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

	DirectoryShards []int `json:"dirShards"`

	FileMode      os.FileMode `json:"fileMode"`
	DirectoryMode os.FileMode `json:"dirMode"`

	FileUID *int `json:"uid,omitempty"`
	FileGID *int `json:"gid,omitempty"`
}

func (fs *fsStorage) BlockExists(blockID BlockID) (bool, error) {
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

func (fs *fsStorage) GetBlock(blockID BlockID) ([]byte, error) {
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

func getBlockIDFromFileName(name string) (BlockID, bool) {
	if strings.HasSuffix(name, fsStorageChunkSuffix) {
		return BlockID(name[0 : len(name)-len(fsStorageChunkSuffix)]), true
	}

	return BlockID(""), false
}

func makeFileName(blockID BlockID) string {
	return string(blockID) + fsStorageChunkSuffix
}

func (fs *fsStorage) ListBlocks(prefix BlockID) chan (BlockMetadata) {
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
				} else if fullID, ok := getBlockIDFromFileName(currentPrefix + e.Name()); ok {
					if strings.HasPrefix(string(fullID), prefixString) {
						result <- BlockMetadata{
							BlockID:   BlockID(fullID),
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

func (fs *fsStorage) PutBlock(blockID BlockID, data io.ReadCloser, options PutOptions) error {
	// Close the data reader regardless of whether we use it or not.
	defer data.Close()

	shardPath, path := fs.getShardedPathAndFilePath(blockID)

	// Open temporary file, create dir if required.
	tempFile := fmt.Sprintf("%s.tmp.%d", path, rand.Int())
	flags := os.O_CREATE | os.O_WRONLY | os.O_EXCL
	f, err := os.OpenFile(tempFile, flags, fs.FileMode)
	if os.IsNotExist(err) {
		if err = os.MkdirAll(shardPath, fs.DirectoryMode); err != nil {
			return fmt.Errorf("cannot create directory: %v", err)
		}
		f, err = os.OpenFile(tempFile, flags, fs.FileMode)
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

func (fs *fsStorage) DeleteBlock(blockID BlockID) error {
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

func (fs *fsStorage) getShardDirectory(blockID BlockID) (string, BlockID) {
	shardPath := fs.Path
	blockIDString := string(blockID)
	if len(blockIDString) < 20 {
		return shardPath, blockID
	}
	for _, size := range fs.DirectoryShards {
		shardPath = filepath.Join(shardPath, blockIDString[0:size])
		blockIDString = blockIDString[size:]
	}

	return shardPath, BlockID(blockIDString)
}

func (fs *fsStorage) getShardedPathAndFilePath(blockID BlockID) (string, string) {
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

	if r.DirectoryShards == nil {
		r.DirectoryShards = fsDefaultShards
	}

	if r.DirectoryMode == 0 {
		r.DirectoryMode = fsDefaultDirMode
	}

	if r.FileMode == 0 {
		r.FileMode = fsDefaultFileMode
	}

	return r, nil
}

func init() {
	AddSupportedStorage(
		fsStorageType,
		func() interface{} { return &FSStorageOptions{} },
		func(cfg interface{}) (Storage, error) {
			return NewFSStorage(cfg.(*FSStorageOptions))
		})
}
