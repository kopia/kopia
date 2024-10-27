// Package sftp implements blob storage provided for SFTP/SSH.
package sftp

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path"
	"strings"

	"github.com/pkg/errors"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"

	"github.com/kopia/kopia/internal/connection"
	"github.com/kopia/kopia/internal/dirutil"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/iocopy"
	"github.com/kopia/kopia/internal/ospath"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/retrying"
	"github.com/kopia/kopia/repo/blob/sharded"
	"github.com/kopia/kopia/repo/logging"
)

var log = logging.Module("sftp")

const (
	sftpStorageType         = "sftp"
	tempFileRandomSuffixLen = 8

	packetSize = 1 << 15
)

// sftpStorage implements blob.Storage on top of sftp.
type sftpStorage struct {
	sharded.Storage
	blob.DefaultProviderImplementation
}

type sftpImpl struct {
	Options

	rec *connection.Reconnector
}

type sftpConnection struct {
	closeFunc     func() error
	currentClient *sftp.Client
	closed        bool
}

func (c *sftpConnection) String() string {
	return "SFTP Connection"
}

func (c *sftpConnection) Close() error {
	if err := c.currentClient.Close(); err != nil {
		return errors.Wrap(err, "error closing SFTP client")
	}

	if err := c.closeFunc(); err != nil {
		return errors.Wrap(err, "error closing SFTP connection")
	}

	c.closed = true

	return nil
}

func (s *sftpImpl) NewConnection(ctx context.Context) (connection.Connection, error) {
	conn, err := getSFTPClient(ctx, &s.Options)

	return conn, err
}

func (s *sftpImpl) IsConnectionClosedError(err error) bool {
	var operr *net.OpError

	if errors.As(err, &operr) {
		if operr.Op == "dial" {
			return true
		}
	}

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

func (s *sftpStorage) GetCapacity(ctx context.Context) (blob.Capacity, error) {
	//nolint:forcetypeassert
	return connection.UsingConnection(ctx, s.Impl.(*sftpImpl).rec, "GetCapacity", func(conn connection.Connection) (blob.Capacity, error) {
		stat, err := sftpClientFromConnection(conn).StatVFS(s.RootPath)
		if err != nil {
			return blob.Capacity{}, errors.Wrap(err, "GetCapacity")
		}

		return blob.Capacity{
			SizeB: stat.Blocks * stat.Bsize,
			FreeB: stat.Bfree * stat.Bsize,
		}, err //nolint:wrapcheck
	})
}

func (s *sftpImpl) GetBlobFromPath(ctx context.Context, dirPath, fullPath string, offset, length int64, output blob.OutputBuffer) error {
	_ = dirPath

	//nolint:wrapcheck
	return s.rec.UsingConnectionNoResult(ctx, "GetBlobFromPath", func(conn connection.Connection) error {
		r, err := sftpClientFromConnection(conn).Open(fullPath)
		if isNotExist(err) {
			return blob.ErrBlobNotFound
		}

		if err != nil {
			return errors.Wrapf(err, "unrecognized error when opening SFTP file %v", fullPath)
		}

		defer r.Close() //nolint:errcheck

		if length < 0 {
			// read entire blob
			output.Reset()

			//nolint:wrapcheck
			return iocopy.JustCopy(output, r)
		}

		// partial read, seek to the provided offset and read given number of bytes.
		if _, err = r.Seek(offset, io.SeekStart); err != nil {
			return errors.Wrapf(blob.ErrInvalidRange, "seek error: %v", err)
		}

		if err := iocopy.JustCopy(output, io.LimitReader(r, length)); err != nil {
			var se *sftp.StatusError

			if errors.As(err, &se) {
				return blob.ErrInvalidRange
			}

			if errors.Is(err, io.EOF) {
				return blob.ErrInvalidRange
			}

			return errors.Wrap(err, "read error")
		}

		//nolint:wrapcheck
		return blob.EnsureLengthExactly(output.Length(), length)
	})
}

func (s *sftpImpl) GetMetadataFromPath(ctx context.Context, dirPath, fullPath string) (blob.Metadata, error) {
	_ = dirPath

	return connection.UsingConnection(ctx, s.rec, "GetMetadataFromPath", func(conn connection.Connection) (blob.Metadata, error) {
		fi, err := sftpClientFromConnection(conn).Stat(fullPath)
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
}

func (s *sftpImpl) PutBlobInPath(ctx context.Context, dirPath, fullPath string, data blob.Bytes, opts blob.PutOptions) error {
	_ = dirPath

	switch {
	case opts.HasRetentionOptions():
		return errors.Wrap(blob.ErrUnsupportedPutBlobOption, "blob-retention")
	case opts.DoNotRecreate:
		return errors.Wrap(blob.ErrUnsupportedPutBlobOption, "do-not-recreate")
	}

	// SFTP client Write() does not do any buffering leading to sub-optimal
	// performance of gather writes, so we copy the data to a contiguous
	// temporary buffer first.
	contig := gather.NewWriteBufferMaxContiguous()
	defer contig.Close()

	if _, err := data.WriteTo(contig); err != nil {
		return errors.Wrap(err, "can't write to comtiguous buffer")
	}

	//nolint:wrapcheck
	return s.rec.UsingConnectionNoResult(ctx, "PutBlobInPath", func(conn connection.Connection) error {
		randSuffix := make([]byte, tempFileRandomSuffixLen)
		if _, err := rand.Read(randSuffix); err != nil {
			return errors.Wrap(err, "can't get random bytes")
		}

		tempFile := fmt.Sprintf("%s.tmp.%x", fullPath, randSuffix)

		f, err := s.createTempFileAndDir(sftpClientFromConnection(conn), tempFile)
		if err != nil {
			return errors.Wrap(err, "cannot create temporary file")
		}

		if _, err = contig.Bytes().WriteTo(f); err != nil {
			return errors.Wrap(err, "can't write temporary file")
		}

		if err = f.Close(); err != nil {
			return errors.Wrap(err, "can't close temporary file")
		}

		err = sftpClientFromConnection(conn).PosixRename(tempFile, fullPath)
		if err != nil {
			if removeErr := sftpClientFromConnection(conn).Remove(tempFile); removeErr != nil {
				log(ctx).Warnf("can't remove temp file: %v", removeErr)
			}

			return errors.Wrap(err, "unexpected error renaming file on SFTP")
		}

		if t := opts.SetModTime; !t.IsZero() {
			if chtimesErr := sftpClientFromConnection(conn).Chtimes(fullPath, t, t); chtimesErr != nil {
				return errors.Wrap(chtimesErr, "can't change file times")
			}
		}

		if t := opts.GetModTime; t != nil {
			fi, err := sftpClientFromConnection(conn).Stat(fullPath)
			if err != nil {
				return errors.Wrap(err, "can't get mod time")
			}

			*t = fi.ModTime()
		}

		return nil
	})
}

type osInterface struct {
	cli *sftp.Client
}

func (osInterface) IsExist(err error) bool {
	return errors.Is(err, os.ErrExist)
}

func (osInterface) IsNotExist(err error) bool {
	return errors.Is(err, os.ErrNotExist)
}

func (osInterface) IsPathSeparator(c byte) bool {
	return c == '/'
}

func (osi osInterface) Mkdir(name string, perm os.FileMode) error {
	_ = perm

	//nolint:wrapcheck
	return osi.cli.Mkdir(name)
}

func (s *sftpImpl) createTempFileAndDir(cli *sftp.Client, tempFile string) (*sftp.File, error) {
	flags := os.O_CREATE | os.O_WRONLY | os.O_EXCL

	f, err := cli.OpenFile(tempFile, flags)
	if isNotExist(err) {
		parentDir := path.Dir(tempFile)
		if err = dirutil.MkSubdirAll(osInterface{cli}, s.Path, parentDir, 0); err != nil {
			return nil, errors.Wrap(err, "cannot create directory")
		}

		//nolint:wrapcheck
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
	_ = dirPath

	//nolint:wrapcheck
	return s.rec.UsingConnectionNoResult(ctx, "DeleteBlobInPath", func(conn connection.Connection) error {
		err := sftpClientFromConnection(conn).Remove(fullPath)
		if err == nil || isNotExist(err) {
			return nil
		}

		return errors.Wrapf(err, "error deleting SFTP file %v", fullPath)
	})
}

func (s *sftpImpl) ReadDir(ctx context.Context, dirname string) ([]os.FileInfo, error) {
	return connection.UsingConnection(ctx, s.rec, "ReadDir", func(conn connection.Connection) ([]os.FileInfo, error) {
		return sftpClientFromConnection(conn).ReadDir(dirname)
	})
}

func (s *sftpStorage) ConnectionInfo() blob.ConnectionInfo {
	return blob.ConnectionInfo{
		Type:   sftpStorageType,
		Config: &s.Impl.(*sftpImpl).Options, //nolint:forcetypeassert
	}
}

func (s *sftpStorage) DisplayName() string {
	o := s.Impl.(*sftpImpl).Options //nolint:forcetypeassert
	return fmt.Sprintf("SFTP %v@%v", o.Username, o.Host)
}

func (s *sftpStorage) Close(ctx context.Context) error {
	s.Impl.(*sftpImpl).rec.CloseActiveConnection(ctx) //nolint:forcetypeassert
	return nil
}

func writeKnownHostsDataStringToTempFile(data string) (string, error) {
	tf, err := os.CreateTemp("", "kopia-known-hosts")
	if err != nil {
		return "", errors.Wrap(err, "error creating temp file")
	}

	defer tf.Close() //nolint:errcheck

	if _, err := tf.WriteString(data); err != nil {
		return "", errors.Wrap(err, "error writing temporary file")
	}

	return tf.Name(), nil
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
		defer os.Remove(tmpFile) //nolint:errcheck

		//nolint:wrapcheck
		return knownhosts.New(tmpFile)
	}

	if f := opt.knownHostsFile(); !ospath.IsAbs(f) {
		return nil, errors.New("known hosts path must be absolute")
	}

	//nolint:wrapcheck
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

		if f := opt.Keyfile; !ospath.IsAbs(f) {
			return nil, errors.New("key file path must be absolute")
		}

		privateKeyData, err = os.ReadFile(opt.Keyfile)
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
	log(ctx).Debug("using internal SSH client")

	hostKeyCallback, err := getHostKeyCallback(opt)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to getHostKey: %s", opt.Host)
	}

	var auth []ssh.AuthMethod

	if opt.Password != "" {
		auth = append(auth, ssh.Password(opt.Password))
	} else {
		signer, err := getSigner(opt)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to getSigner")
		}

		auth = append(auth, ssh.PublicKeys(signer))
	}

	return &ssh.ClientConfig{
		User:            opt.Username,
		Auth:            auth,
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
			p.Kill() //nolint:errcheck
		}

		return nil
	}

	// open the SFTP session
	c, err := sftp.NewClientPipe(rd, wr)
	if err != nil {
		closeFunc() //nolint:errcheck

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
		conn.Close() //nolint:errcheck
		return nil, errors.Wrapf(err, "unable to create sftp client")
	}

	return &sftpConnection{
		currentClient: c,
		closeFunc:     conn.Close,
	}, nil
}

// New creates new ssh-backed storage in a specified host.
func New(ctx context.Context, opts *Options, isCreate bool) (blob.Storage, error) {
	impl := &sftpImpl{
		Options: *opts,
	}

	r := &sftpStorage{
		Storage: sharded.New(impl, opts.Path, opts.Options, isCreate),
	}

	impl.rec = connection.NewReconnector(impl)

	conn, err := impl.rec.GetOrOpenConnection(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "unable to open SFTP storage")
	}

	if _, err := sftpClientFromConnection(conn).Stat(opts.Path); err != nil {
		if isNotExist(err) {
			if err = sftpClientFromConnection(conn).MkdirAll(opts.Path); err != nil {
				return nil, errors.Wrap(err, "cannot create path")
			}
		} else {
			return nil, errors.Wrapf(err, "path doesn't exist: %s", opts.Path)
		}
	}

	return retrying.NewWrapper(r), nil
}

func sftpClientFromConnection(conn connection.Connection) *sftp.Client {
	return conn.(*sftpConnection).currentClient //nolint:forcetypeassert
}

func init() {
	blob.AddSupportedStorage(sftpStorageType, Options{}, New)
}
