package sftp

// Options defines options for sftp-backed storage.
type Options struct {
	Path string `json:"path"`

	Host       string `json:"host"`
	Port       int    `json:"port"`
	Username   string `json:"username"`
	Keyfile    string `json:"keyfile,omitempty"`
	KnownHosts string `json:"-"`

	DirectoryShards []int `json:"dirShards"`
}

func (sftpo *Options) shards() []int {
	if sftpo.DirectoryShards == nil {
		return sftpDefaultShards
	}

	return sftpo.DirectoryShards
}

func (sftpo *Options) knownHosts() string {
	if sftpo.KnownHosts == "" {
		return sftpDefaultKnownHosts
	}

	return sftpo.KnownHosts
}
