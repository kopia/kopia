package sftp

import (
	"os"
	"path/filepath"

	"github.com/kopia/kopia/repo/blob/sharded"
	"github.com/kopia/kopia/repo/blob/throttling"
)

// Options defines options for sftp-backed storage.
type Options struct {
	Path string `json:"path"`

	Host     string `json:"host"`
	Port     int    `json:"port"`
	Username string `json:"username"`
	// if password is specified Keyfile/Keydata is ignored.
	Password       string `json:"password"                 kopia:"sensitive"`
	Keyfile        string `json:"keyfile,omitempty"`
	KeyData        string `json:"keyData,omitempty"        kopia:"sensitive"`
	KnownHostsFile string `json:"knownHostsFile,omitempty"`
	KnownHostsData string `json:"knownHostsData,omitempty"`

	ExternalSSH  bool   `json:"externalSSH"`
	SSHCommand   string `json:"sshCommand,omitempty"` // default "ssh"
	SSHArguments string `json:"sshArguments,omitempty"`

	sharded.Options
	throttling.Limits
}

func (sftpo *Options) knownHostsFile() string {
	if sftpo.KnownHostsFile == "" {
		d, _ := os.UserHomeDir()

		return filepath.Join(d, ".ssh", "known_hosts")
	}

	return sftpo.KnownHostsFile
}
