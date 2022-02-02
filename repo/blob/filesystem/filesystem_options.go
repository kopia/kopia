package filesystem

import (
	"os"

	"github.com/kopia/kopia/repo/blob/sharded"
	"github.com/kopia/kopia/repo/blob/throttling"
)

// Options defines options for Filesystem-backed storage.
type Options struct {
	Path string `json:"path"`

	FileMode      os.FileMode `json:"fileMode,omitempty"`
	DirectoryMode os.FileMode `json:"dirMode,omitempty"`

	FileUID *int `json:"uid,omitempty"`
	FileGID *int `json:"gid,omitempty"`

	sharded.Options
	throttling.Limits

	osInterfaceOverride osInterface
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
