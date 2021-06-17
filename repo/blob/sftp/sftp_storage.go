// Package sftp implements blob storage provided for SFTP/SSH.
package sftp

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/retrying"
	"github.com/kopia/kopia/repo/blob/sharded"
	"github.com/kopia/kopia/repo/logging"
)

var log = logging.GetContextLoggerFunc("sftp")

const (
	sftpStorageType         = "sftp"
	fsStorageChunkSuffix    = ".f"
	tempFileRandomSuffixLen = 8

	packetSize = 1 << 15
)

var sftpDefaultShards = []int{3, 3}

type closeFunc func() error

// sftpStorage implements blob.Storage on top of sftp.
type sftpStorage struct {
	sharded.Storage
}

type sftpImpl struct {
	Options

	closeFunc func() error
	cli       *sftp.Client
}

func (s *sftpImpl) GetBlobFromPath(ctx context.Context, dirPath, fullPath string, offset, length int64) ([]byte, error) {
	r, err := s.cli.Open(fullPath)
	if isNotExist(err) {
		return nil, blob.ErrBlobNotFound
	}

	if err != nil {
		return nil, errors.Wrapf(err, "unrecognized error when opening SFTP file %v", fullPath)
	}
	defer r.Close() //nolint:errcheck

	if length < 0 {
		// read entire blob
		// nolint:wrapcheck
		return ioutil.ReadAll(r)
	}

	// parial read, seek to the provided offset and read given number of bytes.
	if _, err = r.Seek(offset, io.SeekStart); err != nil {
		return nil, errors.Wrapf(blob.ErrInvalidRange, "seek error: %v", err)
	}

	b := make([]byte, length)

	if _, err := r.Read(b); err != nil {
		var se *sftp.StatusError

		if errors.As(err, &se) {
			return nil, blob.ErrInvalidRange
		}

		if errors.Is(err, io.EOF) {
			return nil, blob.ErrInvalidRange
		}

		return nil, errors.Wrap(err, "read error")
	}

	// nolint:wrapcheck
	return blob.EnsureLengthExactly(b, length)
}

func (s *sftpImpl) GetMetadataFromPath(ctx context.Context, dirPath, fullPath string) (blob.Metadata, error) {
	fi, err := s.cli.Stat(fullPath)
	if isNotExist(err) {
		return blob.Metadata{}, blob.ErrBlobNotFound
	}

	if err != nil {
		return blob.Metadata{}, errors.Wrapf(err, "unrecognized error when calling stat() on SFTP file %v", fullPath)
	}

	return blob.Metadata{
		Length:    fi.Size(),
		Timestamp: fi.ModTime(),
	}, nil
}

func (s *sftpImpl) PutBlobInPath(ctx context.Context, dirPath, fullPath string, data blob.Bytes) error {
	randSuffix := make([]byte, tempFileRandomSuffixLen)
	if _, err := rand.Read(randSuffix); err != nil {
		return errors.Wrap(err, "can't get random bytes")
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
			log(ctx).Errorf("warning: can't remove temp file: %v", removeErr)
		}

		return errors.Wrap(err, "unexpected error renaming file on SFTP")
	}

	return nil
}

func (s *sftpImpl) SetTimeInPath(ctx context.Context, dirPath, fullPath string, n time.Time) error {
	// nolint:wrapcheck
	return s.cli.Chtimes(fullPath, n, n)
}

func (s *sftpImpl) createTempFileAndDir(tempFile string) (*sftp.File, error) {
	flags := os.O_CREATE | os.O_WRONLY | os.O_EXCL

	f, err := s.cli.OpenFile(tempFile, flags)
	if isNotExist(err) {
		parentDir := path.Dir(tempFile)
		if err = s.cli.MkdirAll(parentDir); err != nil {
			return nil, errors.Wrap(err, "cannot create directory")
		}

		// nolint:wrapcheck
		return s.cli.OpenFile(tempFile, flags)
	}

	return f, errors.Wrapf(err, "unrecognized error when creating temp file on SFTP: %v", tempFile)
}

func isNotExist(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, os.ErrNotExist) {
		return true
	}

	return strings.Contains(err.Error(), "does not exist")
}

func (s *sftpImpl) DeleteBlobInPath(ctx context.Context, dirPath, fullPath string) error {
	err := s.cli.Remove(fullPath)
	if err == nil || isNotExist(err) {
		return nil
	}

	return errors.Wrapf(err, "error deleting SFTP file %v", fullPath)
}

func (s *sftpImpl) ReadDir(ctx context.Context, dirname string) ([]os.FileInfo, error) {
	// nolint:wrapcheck
	return s.cli.ReadDir(dirname)
}

func (s *sftpStorage) ConnectionInfo() blob.ConnectionInfo {
	return blob.ConnectionInfo{
		Type:   sftpStorageType,
		Config: &s.Impl.(*sftpImpl).Options,
	}
}

func (s *sftpStorage) DisplayName() string {
	o := s.Impl.(*sftpImpl).Options
	return fmt.Sprintf("SFTP %v@%v", o.Username, o.Host)
}

func (s *sftpStorage) Close(ctx context.Context) error {
	if err := s.Impl.(*sftpImpl).cli.Close(); err != nil {
		return errors.Wrap(err, "closing SFTP client")
	}

	if err := s.Impl.(*sftpImpl).closeFunc(); err != nil {
		return errors.Wrap(err, "closing SFTP connection")
	}

	return nil
}

func writeKnownHostsDataStringToTempFile(data string) (string, error) {
	tf, err := ioutil.TempFile("", "kopia-known-hosts")
	if err != nil {
		return "", errors.Wrap(err, "error creating temp file")
	}

	defer tf.Close() //nolint:errcheck,gosec

	if _, err := io.WriteString(tf, data); err != nil {
		return "", errors.Wrap(err, "error writing temporary file")
	}

	return tf.Name(), nil
}

func (s *sftpStorage) FlushCaches(ctx context.Context) error {
	return nil
}

// getHostKeyCallback returns a HostKeyCallback that validates the connected host based on KnownHostsFile or KnownHostsData.
func getHostKeyCallback(opt *Options) (ssh.HostKeyCallback, error) {
	if opt.KnownHostsData != "" {
		// if known hosts data is provided, it takes precedence of KnownHostsFile
		// We need to write to temporary file so we can parse, unfortunately knownhosts.New() only accepts
		// file names, but known_hosts data is not really sensitive so it can be briefly written to disk.
		tmpFile, err := writeKnownHostsDataStringToTempFile(opt.KnownHostsData)
		if err != nil {
			return nil, err
		}

		// this file is no longer needed after `knownhosts.New` returns, so we can delete it.
		defer os.Remove(tmpFile) // nolint:errcheck

		// nolint:wrapcheck
		return knownhosts.New(tmpFile)
	}

	if f := opt.knownHostsFile(); !filepath.IsAbs(f) {
		return nil, errors.Errorf("known hosts path must be absolute")
	}

	// nolint:wrapcheck
	return knownhosts.New(opt.knownHostsFile())
}

// getSigner parses and returns a signer for the user-entered private key.
func getSigner(opts *Options) (ssh.Signer, error) {
	if opts.Keyfile == "" && opts.KeyData == "" {
		return nil, errors.New("must specify the location of the ssh server private key or the key data")
	}

	var privateKeyData []byte

	if opts.KeyData != "" {
		privateKeyData = []byte(opts.KeyData)
	} else {
		var err error

		if f := opts.Keyfile; !filepath.IsAbs(f) {
			return nil, errors.Errorf("key file path must be absolute")
		}

		privateKeyData, err = ioutil.ReadFile(opts.Keyfile)
		if err != nil {
			return nil, errors.Wrap(err, "error reading private key file")
		}
	}

	key, err := ssh.ParsePrivateKey(privateKeyData)
	if err != nil {
		return nil, errors.Wrap(err, "error parsing private key")
	}

	return key, nil
}

func createSSHConfig(ctx context.Context, opts *Options) (*ssh.ClientConfig, error) {
	log(ctx).Debugf("using built-in SSH connection")

	hostKeyCallback, err := getHostKeyCallback(opts)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to getHostKey: %s", opts.Host)
	}

	signer, err := getSigner(opts)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to getSigner")
	}

	return &ssh.ClientConfig{
		User: opts.Username,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		},
		HostKeyCallback: hostKeyCallback,
	}, nil
}

func getSFTPClientExternal(ctx context.Context, opt *Options) (*sftp.Client, closeFunc, error) {
	var cmdArgs []string

	if opt.SSHArguments != "" {
		cmdArgs = append(cmdArgs, strings.Split(opt.SSHArguments, " ")...)
	}

	cmdArgs = append(
		cmdArgs,
		opt.Username+"@"+opt.Host,
		"-s", "sftp",
	)

	sshCommand := opt.SSHCommand
	if sshCommand == "" {
		sshCommand = "ssh"
	}

	log(ctx).Debugf("launching external SSH process %v %v", sshCommand, strings.Join(cmdArgs, " "))

	cmd := exec.Command(sshCommand, cmdArgs...) //nolint:gosec

	// send errors from ssh to stderr
	cmd.Stderr = os.Stderr

	// get stdin and stdout
	wr, err := cmd.StdinPipe()
	if err != nil {
		return nil, nil, errors.Wrap(err, "error opening SSH stdin pipe")
	}

	rd, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, errors.Wrap(err, "error opening SSH stdout pipe")
	}

	if err = cmd.Start(); err != nil {
		return nil, nil, errors.Wrap(err, "error starting SSH")
	}

	closeFunc := func() error {
		p := cmd.Process
		if p != nil {
			p.Kill() // nolint:errcheck
		}

		return nil
	}

	// open the SFTP session
	c, err := sftp.NewClientPipe(rd, wr)
	if err != nil {
		closeFunc() // nolint:errcheck

		return nil, nil, errors.Wrap(err, "error creating sftp client pipe")
	}

	return c, closeFunc, nil
}

func getSFTPClient(ctx context.Context, opt *Options) (*sftp.Client, closeFunc, error) {
	if opt.ExternalSSH {
		return getSFTPClientExternal(ctx, opt)
	}

	config, err := createSSHConfig(ctx, opt)
	if err != nil {
		return nil, nil, err
	}

	addr := fmt.Sprintf("%s:%d", opt.Host, opt.Port)

	conn, err := ssh.Dial("tcp", addr, config)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "unable to dial [%s]: %+v", addr, config)
	}

	c, err := sftp.NewClient(conn, sftp.MaxPacket(packetSize))
	if err != nil {
		conn.Close() // nolint:errcheck
		return nil, nil, errors.Wrapf(err, "unable to create sftp client")
	}

	return c, conn.Close, nil
}

// New creates new ssh-backed storage in a specified host.
func New(ctx context.Context, opts *Options) (blob.Storage, error) {
	c, closeFunc, err := getSFTPClient(ctx, opts)
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
				Options:   *opts,
				cli:       c,
				closeFunc: closeFunc,
			},
			RootPath: opts.Path,
			Suffix:   fsStorageChunkSuffix,
			Shards:   opts.shards(),
		},
	}

	return retrying.NewWrapper(r), nil
}

func init() {
	blob.AddSupportedStorage(
		sftpStorageType,
		func() interface{} { return &Options{} },
		func(ctx context.Context, o interface{}) (blob.Storage, error) {
			return New(ctx, o.(*Options))
		})
}
