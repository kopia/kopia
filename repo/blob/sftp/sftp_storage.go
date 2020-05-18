// Package sftp implements blob storage provided for SFTP/SSH.
package sftp

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"

	"github.com/pkg/errors"
	psftp "github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/sharded"
)

const (
	sftpStorageType      = "sftp"
	fsStorageChunkSuffix = ".f"

	packetSize = 1 << 15
)

var (
	sftpDefaultShards = []int{3, 3}
)

// sftpStorage implements blob.Storage on top of sftp.
type sftpStorage struct {
	sharded.Storage
}

type sftpImpl struct {
	Options

	conn *ssh.Client
	cli  *psftp.Client
}

func (s *sftpImpl) GetBlobFromPath(ctx context.Context, dirPath, fullPath string, offset, length int64) ([]byte, error) {
	r, err := s.cli.Open(fullPath)
	if isNotExist(err) {
		return nil, blob.ErrBlobNotFound
	}

	if err != nil {
		return nil, err
	}
	defer r.Close() //nolint:errcheck

	// pkg/sftp doesn't have a `ioutil.Readall`, so we WriteTo to a buffer
	// and either return it all or return the offset/length bytes
	buf := new(bytes.Buffer)
	n, err := r.WriteTo(buf)

	if err != nil {
		return nil, err
	}

	if length < 0 {
		return buf.Bytes(), nil
	}

	if offset > n || offset < 0 {
		return nil, errors.New("invalid offset")
	}

	data := buf.Bytes()[offset:]
	if int(length) > len(data) {
		return nil, errors.New("invalid length")
	}

	return data[0:length], nil
}

func (s *sftpImpl) GetMetadataFromPath(ctx context.Context, dirPath, fullPath string) (blob.Metadata, error) {
	fi, err := s.cli.Stat(fullPath)
	if isNotExist(err) {
		return blob.Metadata{}, blob.ErrBlobNotFound
	}

	if err != nil {
		return blob.Metadata{}, err
	}

	return blob.Metadata{
		Length:    fi.Size(),
		Timestamp: fi.ModTime(),
	}, nil
}

func (s *sftpImpl) PutBlobInPath(ctx context.Context, dirPath, fullPath string, data blob.Bytes) error {
	randSuffix := make([]byte, 8)
	if _, err := rand.Read(randSuffix); err != nil {
		return errors.Wrap(err, "can't get random bytes")
	}

	progressCallback := blob.ProgressCallback(ctx)
	combinedLength := data.Length()

	if progressCallback != nil {
		progressCallback(fullPath, 0, int64(combinedLength))
		defer progressCallback(fullPath, int64(combinedLength), int64(combinedLength))
	}

	tempFile := fmt.Sprintf("%s.tmp.%x", fullPath, randSuffix)

	f, err := s.createTempFileAndDir(tempFile)
	if err != nil {
		return errors.Wrap(err, "cannot create temporary file")
	}

	if _, err = data.WriteTo(f); err != nil {
		return errors.Wrap(err, "can't write temporary file")
	}

	if err = f.Close(); err != nil {
		return errors.Wrap(err, "can't close temporary file")
	}

	err = s.cli.PosixRename(tempFile, fullPath)
	if err != nil {
		if removeErr := s.cli.Remove(tempFile); removeErr != nil {
			fmt.Printf("warning: can't remove temp file: %v", removeErr)
		}

		return err
	}

	return nil
}

func (s *sftpImpl) createTempFileAndDir(tempFile string) (*psftp.File, error) {
	flags := os.O_CREATE | os.O_WRONLY | os.O_EXCL

	f, err := s.cli.OpenFile(tempFile, flags)
	if isNotExist(err) {
		parentDir := path.Dir(tempFile)
		if err = s.cli.MkdirAll(parentDir); err != nil {
			return nil, errors.Wrap(err, "cannot create directory")
		}

		return s.cli.OpenFile(tempFile, flags)
	}

	return f, err
}

func isNotExist(err error) bool {
	if err == nil {
		return false
	}

	return strings.Contains(err.Error(), "does not exist")
}

func (s *sftpImpl) DeleteBlobInPath(ctx context.Context, dirPath, fullPath string) error {
	err := s.cli.Remove(fullPath)
	if err == nil || isNotExist(err) {
		return nil
	}

	return err
}

func (s *sftpImpl) ReadDir(ctx context.Context, dirname string) ([]os.FileInfo, error) {
	return s.cli.ReadDir(dirname)
}

func (s *sftpStorage) ConnectionInfo() blob.ConnectionInfo {
	return blob.ConnectionInfo{
		Type:   sftpStorageType,
		Config: &s.Impl.(*sftpImpl).Options,
	}
}

func (s *sftpStorage) Close(ctx context.Context) error {
	if err := s.Impl.(*sftpImpl).cli.Close(); err != nil {
		return errors.Wrap(err, "closing SFTP client")
	}

	if err := s.Impl.(*sftpImpl).conn.Close(); err != nil {
		return errors.Wrap(err, "closing SFTP connection")
	}

	return nil
}

// example host strings: [localhost]:2222, [xyz.example.com], [192.168.1.1]:2210, 192.168.1.1
func cleanup(host string) string {
	if index := strings.Index(host, ":"); index > 0 {
		host = host[:index]
	}

	host = strings.ReplaceAll(host, "[", "")
	host = strings.ReplaceAll(host, "]", "")

	return host
}

// given a list of hosts from a known_hosts entry, determine if the host is referenced
func hostExists(host string, hosts []string) bool {
	for _, entry := range hosts {
		if host == cleanup(entry) {
			return true
		}
	}

	return false
}

// getHostKey parses OpenSSH known_hosts file for a public key that matches the host
// The known_hosts file format is documented in the sshd(8) manual page
func getHostKey(opt *Options) (ssh.PublicKey, error) {
	var reader io.Reader

	if opt.KnownHostsData != "" {
		reader = strings.NewReader(opt.KnownHostsData)
	} else {
		file, err := os.Open(opt.knownHostsFile()) //nolint:gosec
		if err != nil {
			return nil, err
		}
		defer file.Close() //nolint:errcheck

		reader = file
	}

	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		_, hosts, hostKey, _, _, err := ssh.ParseKnownHosts(scanner.Bytes())
		if err != nil {
			return nil, errors.Wrapf(err, "error parsing %s", scanner.Text())
		}

		if hostExists(opt.Host, hosts) {
			return hostKey, nil
		}
	}

	return nil, errors.Errorf("no hostkey found for %s", opt.Host)
}

// getSigner parses and returns a signer for the user-entered private key
func getSigner(opts *Options) (ssh.Signer, error) {
	if opts.Keyfile == "" && opts.KeyData == "" {
		return nil, errors.New("must specify the location of the ssh server private key or the key data")
	}

	var privateKeyData []byte

	if opts.KeyData != "" {
		privateKeyData = []byte(opts.KeyData)
	} else {
		var err error

		privateKeyData, err = ioutil.ReadFile(opts.Keyfile) //nolint:gosec
		if err != nil {
			return nil, err
		}
	}

	key, err := ssh.ParsePrivateKey(privateKeyData)
	if err != nil {
		return nil, err
	}

	return key, nil
}

func createSSHConfig(opts *Options) (*ssh.ClientConfig, error) {
	hostKey, err := getHostKey(opts)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to getHostKey: %s", opts.Host)
	}

	signer, err := getSigner(opts)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to getSigner")
	}

	config := &ssh.ClientConfig{
		User: opts.Username,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		},
		HostKeyCallback: ssh.FixedHostKey(hostKey),
	}

	return config, nil
}

// New creates new ssh-backed storage in a specified host.
func New(ctx context.Context, opts *Options) (blob.Storage, error) {
	config, err := createSSHConfig(opts)
	if err != nil {
		return nil, err
	}

	addr := fmt.Sprintf("%s:%d", opts.Host, opts.Port)

	conn, err := ssh.Dial("tcp", addr, config)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to dial [%s]: %+v", addr, config)
	}

	c, err := psftp.NewClient(conn, psftp.MaxPacket(packetSize))
	if err != nil {
		return nil, errors.Wrapf(err, "unable to create sftp client")
	}

	if _, err = c.Stat(opts.Path); err != nil {
		if isNotExist(err) {
			if err = c.MkdirAll(opts.Path); err != nil {
				return nil, errors.Wrap(err, "cannot create path")
			}
		} else {
			return nil, errors.Wrapf(err, "path doesn't exist: %s", opts.Path)
		}
	}

	r := &sftpStorage{
		sharded.Storage{
			Impl: &sftpImpl{
				Options: *opts,
				conn:    conn,
				cli:     c,
			},
			RootPath: opts.Path,
			Suffix:   fsStorageChunkSuffix,
			Shards:   opts.shards(),
		},
	}

	return r, nil
}

func init() {
	blob.AddSupportedStorage(
		sftpStorageType,
		func() interface{} { return &Options{} },
		func(ctx context.Context, o interface{}) (blob.Storage, error) {
			return New(ctx, o.(*Options))
		})
}
