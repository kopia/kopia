package filesystem

import "os"

// Options defines options for Filesystem-backed storage.
type Options struct {
	Path string `json:"path"`

	DirectoryShards []int `json:"dirShards"`

	FileMode      os.FileMode `json:"fileMode,omitempty"`
	DirectoryMode os.FileMode `json:"dirMode,omitempty"`

	FileUID *int `json:"uid,omitempty"`
	FileGID *int `json:"gid,omitempty"`
}

func (fso *Options) fileMode() os.FileMode {
	if fso.FileMode == 0 {
		return fsDefaultFileMode
	}

	return fso.FileMode
}

func (fso *Options) dirMode() os.FileMode {
	if fso.DirectoryMode == 0 {
		return fsDefaultDirMode
	}

	return fso.DirectoryMode
}

func (fso *Options) shards() []int {
	if fso.DirectoryShards == nil {
		return fsDefaultShards
	}

	return fso.DirectoryShards
}
