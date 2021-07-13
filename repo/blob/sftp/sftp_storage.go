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
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"

	"github.com/kopia/kopia/internal/retry"
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

// sftpStorage implements blob.Storage on top of sftp.
type sftpStorage struct {
	sharded.Storage
}

type sftpImpl struct {
	Options

	cond          sync.Cond
	connectionID  int
	availableConn []*sftpConnection
	allConn       []*sftpConnection
}

type sftpConnection struct {
	id            int
	closeFunc     func() error
	currentClient *sftp.Client
	closed        bool
}

func (c *sftpConnection) close(ctx context.Context) {
	if err := c.currentClient.Close(); err != nil {
		log(ctx).Errorf("error closing SFTP client: %v", err)
	}

	if err := c.closeFunc(); err != nil {
		log(ctx).Errorf("error closing SFTP connection: %v", err)
	}

	c.closed = true
}

func (s *sftpImpl) getPooledConnection(ctx context.Context) (*sftpConnection, error) {
	s.cond.L.Lock()
	defer s.cond.L.Unlock()

	maxConn := s.maxConnections()

	for {
		if n := len(s.availableConn); n > 0 {
			conn := s.availableConn[n-1]
			s.availableConn = s.availableConn[0 : n-1]

			return conn, nil
		}

		if len(s.allConn) < maxConn {
			s.connectionID++

			log(ctx).Debugf("establishing new SFTP connection %v/%v...", len(s.allConn)+1, maxConn)

			conn, err := getSFTPClient(ctx, &s.Options)
			if err != nil {
				return nil, errors.Wrap(err, "error establishing SFTP connecting")
			}

			conn.id = s.connectionID

			s.allConn = append(s.allConn, conn)

			return conn, nil
		}

		log(ctx).Debugf("all (%v) available connections are in use, waiting for idle connection...", maxConn)

		// wait for condition to change, when another connection is returned
		s.cond.Wait()
	}
}

func (s *sftpImpl) returnConnection(ctx context.Context, conn *sftpConnection) {
	s.cond.L.Lock()
	defer s.cond.L.Unlock()

	if conn.closed {
		// connection was closed, don't reuse, remove from 'allConn'
		log(ctx).Debugf("removing closed connection %v", conn.id)
		s.allConn = removeConn(s.allConn, conn)
	} else {
		// connection is available again
		s.availableConn = append(s.availableConn, conn)
	}

	// notify whoever is waiting for it
	s.cond.Signal()
}

func removeConn(s []*sftpConnection, v *sftpConnection) []*sftpConnection {
	var result []*sftpConnection

	for _, it := range s {
		if it == v {
			continue
		}

		result = append(result, it)
	}

	return result
}

func isConnectionClosedError(err error) bool {
	if errors.Is(err, sftp.ErrSshFxConnectionLost) {
		return true
	}

	if errors.Is(err, sftp.ErrSSHFxNoConnection) {
		return true
	}

	if errors.Is(err, io.EOF) {
		return true
	}

	return false
}

func (s *sftpImpl) usingClient(ctx context.Context, desc string, cb func(cli *sftp.Client) (interface{}, error)) (interface{}, error) {
	// nolint:wrapcheck
	return retry.WithExponentialBackoff(ctx, desc, func() (interface{}, error) {
		conn, err := s.getPooledConnection(ctx)
		if err != nil {
			if isConnectionClosedError(err) {
				log(ctx).Errorf("SFTP connection failed: %v, will retry", err)
			}

			return nil, errors.Wrap(err, "error opening SFTP client")
		}

		defer s.returnConnection(ctx, conn)

		v, err := cb(conn.currentClient)
		if err != nil {
			if isConnectionClosedError(err) {
				log(ctx).Errorf("SFTP connection failed: %v, will retry", err)
				conn.close(ctx)
			}
		}

		return v, err
	}, isConnectionClosedError)
}

func (s *sftpImpl) usingClientNoResult(ctx context.Context, desc string, cb func(cli *sftp.Client) error) error {
	_, err := s.usingClient(ctx, desc, func(cli *sftp.Client) (interface{}, error) {
		return nil, cb(cli)
	})

	return err
}

func (s *sftpImpl) closeAllConnections(ctx context.Context) {
	s.cond.L.Lock()
	defer s.cond.L.Unlock()

	for _, c := range s.allConn {
		c.close(ctx)
	}

	s.allConn = nil
}

func (s *sftpImpl) GetBlobFromPath(ctx context.Context, dirPath, fullPath string, offset, length int64) ([]byte, error) {
	v, err := s.usingClient(ctx, "GetBlobFromPath", func(cli *sftp.Client) (interface{}, error) {
		r, err := cli.Open(fullPath)
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
	})
	if err != nil {
		return nil, err
	}

	return v.([]byte), nil
}

func (s *sftpImpl) GetMetadataFromPath(ctx context.Context, dirPath, fullPath string) (blob.Metadata, error) {
	v, err := s.usingClient(ctx, "GetMetadataFromPath", func(cli *sftp.Client) (interface{}, error) {
		fi, err := cli.Stat(fullPath)
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
	})
	if err != nil {
		return blob.Metadata{}, err
	}

	return v.(blob.Metadata), nil
}

func (s *sftpImpl) PutBlobInPath(ctx context.Context, dirPath, fullPath string, data blob.Bytes) error {
	return s.usingClientNoResult(ctx, "PutBlobInPath", func(cli *sftp.Client) error {
		randSuffix := make([]byte, tempFileRandomSuffixLen)
		if _, err := rand.Read(randSuffix); err != nil {
			return errors.Wrap(err, "can't get random bytes")
		}

		tempFile := fmt.Sprintf("%s.tmp.%x", fullPath, randSuffix)

		f, err := s.createTempFileAndDir(cli, tempFile)
		if err != nil {
			return errors.Wrap(err, "cannot create temporary file")
		}

		if _, err = data.WriteTo(f); err != nil {
			return errors.Wrap(err, "can't write temporary file")
		}

		if err = f.Close(); err != nil {
			return errors.Wrap(err, "can't close temporary file")
		}

		err = cli.PosixRename(tempFile, fullPath)
		if err != nil {
			if removeErr := cli.Remove(tempFile); removeErr != nil {
				log(ctx).Errorf("warning: can't remove temp file: %v", removeErr)
			}

			return errors.Wrap(err, "unexpected error renaming file on SFTP")
		}

		return nil
	})
}

func (s *sftpImpl) SetTimeInPath(ctx context.Context, dirPath, fullPath string, n time.Time) error {
	return s.usingClientNoResult(ctx, "SetTimeInPath", func(cli *sftp.Client) error {
		// nolint:wrapcheck
		return cli.Chtimes(fullPath, n, n)
	})
}

func (s *sftpImpl) createTempFileAndDir(cli *sftp.Client, tempFile string) (*sftp.File, error) {
	flags := os.O_CREATE | os.O_WRONLY | os.O_EXCL

	f, err := cli.OpenFile(tempFile, flags)
	if isNotExist(err) {
		parentDir := path.Dir(tempFile)
		if err = cli.MkdirAll(parentDir); err != nil {
			return nil, errors.Wrap(err, "cannot create directory")
		}

		// nolint:wrapcheck
		return cli.OpenFile(tempFile, flags)
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
	return s.usingClientNoResult(ctx, "DeleteBlobInPath", func(cli *sftp.Client) error {
		err := cli.Remove(fullPath)
		if err == nil || isNotExist(err) {
			return nil
		}

		return errors.Wrapf(err, "error deleting SFTP file %v", fullPath)
	})
}

func (s *sftpImpl) ReadDir(ctx context.Context, dirname string) ([]os.FileInfo, error) {
	v, err := s.usingClient(ctx, "ReadDir", func(cli *sftp.Client) (interface{}, error) {
		// nolint:wrapcheck
		return cli.ReadDir(dirname)
	})
	if err != nil {
		return nil, err
	}

	return v.([]os.FileInfo), nil
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
	s.Impl.(*sftpImpl).closeAllConnections(ctx)
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
func getSigner(opt *Options) (ssh.Signer, error) {
	if opt.Keyfile == "" && opt.KeyData == "" {
		return nil, errors.New("must specify the location of the ssh server private key or the key data")
	}

	var privateKeyData []byte

	if opt.KeyData != "" {
		privateKeyData = []byte(opt.KeyData)
	} else {
		var err error

		if f := opt.Keyfile; !filepath.IsAbs(f) {
			return nil, errors.Errorf("key file path must be absolute")
		}

		privateKeyData, err = ioutil.ReadFile(opt.Keyfile)
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

func createSSHConfig(ctx context.Context, opt *Options) (*ssh.ClientConfig, error) {
	log(ctx).Debugf("using internal SSH client")

	hostKeyCallback, err := getHostKeyCallback(opt)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to getHostKey: %s", opt.Host)
	}

	signer, err := getSigner(opt)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to getSigner")
	}

	return &ssh.ClientConfig{
		User: opt.Username,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		},
		HostKeyCallback: hostKeyCallback,
	}, nil
}

func getSFTPClientExternal(ctx context.Context, opt *Options) (*sftpConnection, error) {
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
		return nil, errors.Wrap(err, "error opening SSH stdin pipe")
	}

	rd, err := cmd.StdoutPipe()
	if err != nil {
		return nil, errors.Wrap(err, "error opening SSH stdout pipe")
	}

	if err = cmd.Start(); err != nil {
		return nil, errors.Wrap(err, "error starting SSH")
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

		return nil, errors.Wrap(err, "error creating sftp client pipe")
	}

	return &sftpConnection{
		currentClient: c,
		closeFunc:     closeFunc,
	}, nil
}

func getSFTPClient(ctx context.Context, opt *Options) (*sftpConnection, error) {
	if opt.ExternalSSH {
		return getSFTPClientExternal(ctx, opt)
	}

	config, err := createSSHConfig(ctx, opt)
	if err != nil {
		return nil, err
	}

	addr := fmt.Sprintf("%s:%d", opt.Host, opt.Port)

	conn, err := ssh.Dial("tcp", addr, config)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to dial [%s]: %#v", addr, config)
	}

	c, err := sftp.NewClient(conn,
		sftp.MaxPacket(packetSize),
		sftp.UseConcurrentWrites(true),
		sftp.UseConcurrentReads(true),
	)
	if err != nil {
		conn.Close() // nolint:errcheck
		return nil, errors.Wrapf(err, "unable to create sftp client")
	}

	return &sftpConnection{
		currentClient: c,
		closeFunc:     conn.Close,
	}, nil
}

// New creates new ssh-backed storage in a specified host.
func New(ctx context.Context, opts *Options) (blob.Storage, error) {
	impl := &sftpImpl{
		Options: *opts,

		cond: sync.Cond{
			L: &sync.Mutex{},
		},
	}

	r := &sftpStorage{
		sharded.Storage{
			Impl:     impl,
			RootPath: opts.Path,
			Suffix:   fsStorageChunkSuffix,
			Shards:   opts.shards(),
		},
	}

	if err := impl.usingClientNoResult(ctx, "OpenSFTP", func(cli *sftp.Client) error {
		if _, err := cli.Stat(opts.Path); err != nil {
			if isNotExist(err) {
				if err = cli.MkdirAll(opts.Path); err != nil {
					return errors.Wrap(err, "cannot create path")
				}
			} else {
				return errors.Wrapf(err, "path doesn't exist: %s", opts.Path)
			}
		}

		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "unable to open SFTP storage")
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
