package sftp

import (
	"os"
	"path/filepath"
)

// Options defines options for sftp-backed storage.
type Options struct {
	Path string `json:"path"`

	Host           string `json:"host"`
	Port           int    `json:"port"`
	Username       string `json:"username"`
	Keyfile        string `json:"keyfile,omitempty"`
	KeyData        string `json:"keyData,omitempty" kopia:"sensitive"`
	KnownHostsFile string `json:"knownHostsFile,omitempty"`
	KnownHostsData string `json:"knownHostsData,omitempty"`

	DirectoryShards []int `json:"dirShards"`
}

func (sftpo *Options) shards() []int {
	if sftpo.DirectoryShards == nil {
		return sftpDefaultShards
	}

	return sftpo.DirectoryShards
}

func (sftpo *Options) knownHostsFile() string {
	if sftpo.KnownHostsFile == "" {
		d, _ := os.UserHomeDir()

		return filepath.Join(d, ".ssh", "known_hosts")
	}

	return sftpo.KnownHostsFile
}
