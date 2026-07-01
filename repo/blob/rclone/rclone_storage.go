//go:build !no_extra_providers

// Package rclone implements blob storage provider proxied by rclone (http://rclone.org)
package rclone

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"time"

	"github.com/foomo/htpasswd"
	"github.com/google/uuid"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/osexec"
	"github.com/kopia/kopia/internal/retry"
	"github.com/kopia/kopia/internal/tlsutil"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/webdav"
	"github.com/kopia/kopia/repo/logging"
)

const (
	rcloneStorageType = "rclone"

	defaultRCloneExe = "rclone"

	// defaultRcloneStartupTimeout is the time we wait for rclone to print the https address it's serving at.
	defaultRcloneStartupTimeout = 15 * time.Second

	// remoteControlRequestTimeout is the overall timeout for a single remote control request,
	// so that a wedged rclone process does not hang storage operations indefinitely.
	remoteControlRequestTimeout = 1 * time.Minute

	// maxStderrLineLength is the maximum length of a single rclone stderr line the scanner can handle.
	maxStderrLineLength = 1 << 20
)

var log = logging.Module("rclone")

// errRCloneExited is returned when the rclone child process is no longer running.
var errRCloneExited = errors.New("rclone process has exited")

type rcloneStorage struct {
	blob.Storage // the underlying WebDAV storage used to implement all methods.

	Options

	cmd          *exec.Cmd // running rclone
	temporaryDir string

	// exited is closed by the process watchdog when rclone exits; exitErr is set before it is closed.
	exited  chan struct{}
	exitErr error

	remoteControlHTTPClient *http.Client
	remoteControlAddr       string
	remoteControlUsername   string
	remoteControlPassword   string
}

// exitError returns an error if the rclone child process is no longer running, nil otherwise.
func (r *rcloneStorage) exitError() error {
	if r.exited == nil {
		return nil
	}

	select {
	case <-r.exited:
		if r.exitErr != nil {
			return errors.Wrapf(errRCloneExited, "%v", r.exitErr)
		}

		return errRCloneExited

	default:
		return nil
	}
}

// waitForExit waits for the rclone process to be reaped after it was killed.
func (r *rcloneStorage) waitForExit() {
	if r.exited != nil {
		<-r.exited
	} else {
		// process watchdog not running, e.g. when rclone failed to start.
		r.cmd.Wait() //nolint:errcheck
	}
}

func (r *rcloneStorage) ListBlobs(ctx context.Context, blobIDPrefix blob.ID, cb func(bm blob.Metadata) error) error {
	// flushing dir cache before listing blobs
	if err := r.forgetVFS(ctx); err != nil {
		return errors.Wrap(err, "error flushing dir cache")
	}

	return r.Storage.ListBlobs(ctx, blobIDPrefix, cb) //nolint:wrapcheck
}

func (r *rcloneStorage) GetMetadata(ctx context.Context, b blob.ID) (blob.Metadata, error) {
	// flushing dir cache before reading blob
	if err := r.forgetVFS(ctx); err != nil {
		return blob.Metadata{}, errors.Wrap(err, "error flushing dir cache")
	}

	//nolint:wrapcheck
	return r.Storage.GetMetadata(ctx, b)
}

func (r *rcloneStorage) GetBlob(ctx context.Context, b blob.ID, offset, length int64, output blob.OutputBuffer) error {
	// flushing dir cache before reading blob
	if err := r.forgetVFS(ctx); err != nil {
		return errors.Wrap(err, "error flushing dir cache")
	}

	//nolint:wrapcheck
	return r.Storage.GetBlob(ctx, b, offset, length, output)
}

func (r *rcloneStorage) ConnectionInfo() blob.ConnectionInfo {
	return blob.ConnectionInfo{
		Type:   rcloneStorageType,
		Config: &r.Options,
	}
}

// Kill kills the rclone process. Used for testing.
func (r *rcloneStorage) Kill() {
	// this will kill rclone process if any
	if r.cmd != nil && r.cmd.Process != nil {
		r.cmd.Process.Kill() //nolint:errcheck
		r.waitForExit()
	}
}

func (r *rcloneStorage) Close(ctx context.Context) error {
	if r.Storage != nil {
		if err := r.Storage.Close(ctx); err != nil {
			return errors.Wrap(err, "error closing webdav connection")
		}
	}

	// this will kill rclone process if any
	if r.cmd != nil && r.cmd.Process != nil {
		log(ctx).Debug("killing rclone")
		r.cmd.Process.Kill() //nolint:errcheck
		r.waitForExit()
	}

	if r.temporaryDir != "" {
		if err := os.RemoveAll(r.temporaryDir); err != nil && !os.IsNotExist(err) {
			log(ctx).Errorf("error deleting temporary dir: %v", err)
		}
	}

	return nil
}

func (r *rcloneStorage) DisplayName() string {
	return "RClone " + r.RemotePath
}

func (r *rcloneStorage) processStderrStatus(ctx context.Context, stderr io.Reader, s *bufio.Scanner) {
	for s.Scan() {
		l := s.Text()

		if r.Debug {
			log(ctx).Debugf("[RCLONE] %v", l)
		}
	}

	// If scanning stopped because of an error (e.g. a line longer than the scanner buffer),
	// keep draining the pipe, otherwise rclone would eventually block writing to stderr
	// and stop responding to all requests.
	if s.Err() != nil {
		io.Copy(io.Discard, stderr) //nolint:errcheck
	}
}

// remoteControlStatusError is returned when the remote control server responds with a non-200 status.
type remoteControlStatusError struct {
	statusCode int
	status     string
}

func (e remoteControlStatusError) Error() string {
	return "RC error: " + e.status
}

// isRetriableRemoteControlError determines whether a failed remote control call is worth
// retrying. Connection-level errors (e.g. EOF when rclone closed a pooled connection) and
// server errors are transient; client errors, cancellations, timeouts and rclone having
// exited are permanent.
func isRetriableRemoteControlError(err error) bool {
	if errors.Is(err, errRCloneExited) {
		return false
	}

	// don't retry when the caller's context was canceled or when the request was killed
	// by the HTTP client timeout: a wedged rclone would fail every attempt the same way,
	// multiplying the stall by the number of attempts.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	var se remoteControlStatusError
	if errors.As(err, &se) {
		return se.statusCode >= http.StatusInternalServerError
	}

	return true
}

func (r *rcloneStorage) remoteControl(ctx context.Context, path string, input, output any) error {
	if err := r.exitError(); err != nil {
		return err
	}

	var reqBuf bytes.Buffer

	if err := json.NewEncoder(&reqBuf).Encode(input); err != nil {
		return errors.Wrap(err, "unable to serialize input")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.remoteControlAddr+path, &reqBuf)
	if err != nil {
		return errors.Wrap(err, "unable to create request")
	}

	req.SetBasicAuth(r.remoteControlUsername, r.remoteControlPassword)

	// Remote control calls are idempotent, which allows the HTTP transport to transparently
	// retry the POST on another connection when a pooled keep-alive connection turns out to
	// have been closed by the server (which otherwise surfaces as immediate EOF).
	req.Header.Set("Idempotency-Key", uuid.New().String())

	resp, err := r.remoteControlHTTPClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "RC error")
	}

	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		return remoteControlStatusError{statusCode: resp.StatusCode, status: resp.Status}
	}

	if err := json.NewDecoder(resp.Body).Decode(&output); err != nil {
		return errors.Errorf("error decoding response: %v", err)
	}

	return nil
}

func (r *rcloneStorage) forgetVFS(ctx context.Context) error {
	return retry.WithExponentialBackoffNoValue(ctx, "ForgetVFS", func() error {
		out := map[string]any{}
		return r.remoteControl(ctx, "vfs/forget", map[string]string{}, &out)
	}, isRetriableRemoteControlError)
}

type rcloneURLs struct {
	webdavAddr        string
	remoteControlAddr string
}

func (r *rcloneStorage) runRCloneAndWaitForServerAddress(ctx context.Context, c *exec.Cmd, startupTimeout time.Duration) (rcloneURLs, error) {
	rcloneAddressChan := make(chan rcloneURLs)
	rcloneErrChan := make(chan error)

	log(ctx).Debugf("starting %v", c.Path)

	go func() {
		stderr, err := c.StderrPipe()
		if err != nil {
			rcloneErrChan <- err
			return
		}

		if err := c.Start(); err != nil {
			rcloneErrChan <- err
			return
		}

		go func() {
			s := bufio.NewScanner(stderr)
			s.Buffer(make([]byte, 0, bufio.MaxScanTokenSize), maxStderrLineLength)

			var lastOutput string

			webdavServerRegexp := regexp.MustCompile(`(?i)WebDav Server started on \[?(https://.+:\d{1,5}/)\]?`)
			remoteControlRegexp := regexp.MustCompile(`(?i)Serving remote control on \[?(https://.+:\d{1,5}/)\]?`)

			var u rcloneURLs

			for s.Scan() {
				l := s.Text()
				lastOutput = l

				if p := webdavServerRegexp.FindStringSubmatch(l); p != nil {
					u.webdavAddr = p[1]
				}

				if p := remoteControlRegexp.FindStringSubmatch(l); p != nil {
					u.remoteControlAddr = p[1]
				}

				if u.webdavAddr != "" && u.remoteControlAddr != "" {
					// return to caller when we've detected both WebDav and remote control addresses.
					rcloneAddressChan <- u

					go r.processStderrStatus(ctx, stderr, s)

					return
				}
			}

			select {
			case rcloneErrChan <- errors.Errorf("rclone server failed to start: %v", lastOutput):
			default:
			}
		}()
	}()

	select {
	case addr := <-rcloneAddressChan:
		return addr, nil

	case err := <-rcloneErrChan:
		return rcloneURLs{}, err

	case <-time.After(startupTimeout):
		return rcloneURLs{}, errors.New("timed out waiting for rclone to start")
	}
}

// New creates new RClone storage with specified options.
//
//nolint:funlen
func New(ctx context.Context, opt *Options, isCreate bool) (blob.Storage, error) {
	log(ctx).Warn("The rclone storage provider is not actively tested, it may cause data loss, use at your own risk")

	// generate directory for all temp files.
	td, err := os.MkdirTemp("", "kopia-rclone")
	if err != nil {
		return nil, errors.Wrap(err, "error getting temporary dir")
	}

	r := &rcloneStorage{
		Options:      *opt,
		temporaryDir: td,
	}

	// TLS key for rclone webdav server.
	temporaryKeyPath := filepath.Join(td, "webdav.key")

	// TLS cert for rclone webdav server.
	temporaryCertPath := filepath.Join(td, "webdav.cert")

	// password file for rclone webdav server.
	temporaryHtpassword := filepath.Join(td, "htpasswd")

	defer func() {
		// if we return this function without setting Storage, make sure to clean everything up.
		if r.Storage == nil {
			r.Close(ctx) //nolint:errcheck
		}
	}()

	// write TLS files.
	//nolint:mnd
	cert, key, err := tlsutil.GenerateServerCertificate(ctx, 2048, 365*24*time.Hour, []string{"127.0.0.1"})
	if err != nil {
		return nil, errors.Wrap(err, "unable to generate server certificate")
	}

	if err = tlsutil.WritePrivateKeyToFile(temporaryKeyPath, key); err != nil {
		return nil, errors.Wrap(err, "unable to write WebDAV key")
	}

	if err = tlsutil.WriteCertificateToFile(temporaryCertPath, cert); err != nil {
		return nil, errors.Wrap(err, "unable to write WebDAV cert")
	}

	// temporary username and password to be used when communicating with rclone
	webdavUsername := "u" + uuid.New().String()
	webdavPassword := "p" + uuid.New().String()

	if err = htpasswd.SetPassword(temporaryHtpassword, webdavUsername, webdavPassword, htpasswd.HashAPR1); err != nil {
		return nil, errors.Wrap(err, "unable to write htpasswd file")
	}

	rcloneExe := defaultRCloneExe
	if opt.RCloneExe != "" {
		rcloneExe = opt.RCloneExe
	}

	statsMarker := "STATS:KOPIA"

	arguments := append([]string{
		"-v",
		"serve", "webdav", opt.RemotePath,
	}, opt.RCloneArgs...)

	if opt.EmbeddedConfig != "" {
		tmpConfigFile := filepath.Join(r.temporaryDir, "rclone.conf")

		//nolint:mnd
		if err = os.WriteFile(tmpConfigFile, []byte(opt.EmbeddedConfig), 0o600); err != nil {
			return nil, errors.Wrap(err, "unable to write config file")
		}

		arguments = append(arguments, "--config", tmpConfigFile)
	}

	// append our mandatory arguments at the end so that they precedence over user-provided
	// arguments.
	arguments = append(arguments,
		"--addr", "127.0.0.1:0", // allocate random port,
		"--rc",
		"--rc-addr", "127.0.0.1:0", // allocate random remote control port
		"--rc-cert", temporaryCertPath,
		"--rc-key", temporaryKeyPath,
		"--rc-htpasswd", temporaryHtpassword,
		"--cert", temporaryCertPath,
		"--key", temporaryKeyPath,
		"--htpasswd", temporaryHtpassword,
		"--stats", "1s",
		"--stats-one-line",
		"--stats-one-line-date-format="+statsMarker,
		"--vfs-write-back=0s", // disable write-back, critical for correctness
	)

	r.cmd = exec.CommandContext(context.WithoutCancel(ctx), rcloneExe, arguments...) //nolint:gosec
	r.cmd.Env = append(r.cmd.Env, opt.RCloneEnv...)

	// https://github.com/kopia/kopia/issues/1934
	osexec.DisableInterruptSignal(r.cmd)

	startupTimeout := defaultRcloneStartupTimeout
	if opt.StartupTimeout.Duration != 0 {
		startupTimeout = opt.StartupTimeout.Duration
	}

	rcloneUrls, err := r.runRCloneAndWaitForServerAddress(ctx, r.cmd, startupTimeout)
	if err != nil {
		return nil, errors.Wrap(err, "unable to start rclone")
	}

	log(ctx).Debugf("detected webdav address: %v RC: %v", rcloneUrls.webdavAddr, rcloneUrls.remoteControlAddr)

	// watch the rclone process so that its unexpected death produces clear errors
	// instead of connection failures on every storage operation.
	r.exited = make(chan struct{})

	go func() {
		r.exitErr = r.cmd.Wait()

		// log before closing the channel: the close unblocks Close()/Kill() and the
		// storage owner may tear down immediately after, invalidating ctx's logger.
		log(ctx).Debugf("rclone process exited: %v", r.exitErr)

		close(r.exited)
	}()

	fingerprintBytes := sha256.Sum256(cert.Raw)
	fingerprintHexString := hex.EncodeToString(fingerprintBytes[:])

	cli := http.Client{
		Timeout:   remoteControlRequestTimeout,
		Transport: tlsutil.TransportTrustingSingleCertificate(fingerprintHexString),
	}

	r.remoteControlHTTPClient = &cli
	r.remoteControlUsername = webdavUsername
	r.remoteControlPassword = webdavPassword
	r.remoteControlAddr = rcloneUrls.remoteControlAddr

	wst, err := webdav.New(ctx, &webdav.Options{
		URL:                                 rcloneUrls.webdavAddr,
		Username:                            webdavUsername,
		Password:                            webdavPassword,
		TrustedServerCertificateFingerprint: fingerprintHexString,
		AtomicWrites:                        opt.AtomicWrites,
		Options:                             opt.Options,
	}, isCreate)
	if err != nil {
		return nil, errors.Wrap(err, "error connecting to webdav storage")
	}

	r.Storage = wst

	return r, nil
}

func init() {
	blob.AddSupportedStorage(rcloneStorageType, Options{}, New)
}
