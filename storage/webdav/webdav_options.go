package webdav

// Options defines options for Filesystem-backed storage.
type Options struct {
	URL             string `json:"url"`
	DirectoryShards []int  `json:"dirShards,omitempty"`
	Username        string `json:"username,omitempty"`
	Password        string `json:"password,omitempty"`
}

func (fso *Options) shards() []int {
	if fso.DirectoryShards == nil {
		return fsDefaultShards
	}

	return fso.DirectoryShards
}
