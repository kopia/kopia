// Package rclone implements blob storage provider proxied by rclone (http://rclone.org)
package rclone

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/foomo/htpasswd"
	"github.com/google/uuid"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/osexec"
	"github.com/kopia/kopia/internal/tlsutil"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/webdav"
	"github.com/kopia/kopia/repo/logging"
)

const (
	rcloneStorageType = "rclone"

	defaultRCloneExe = "rclone"

	// rcloneStartupTimeout is the time we wait for rclone to print the https address it's serving at.
	rcloneStartupTimeout = 15 * time.Second
)

var log = logging.Module("rclone")

type rcloneStorage struct {
	blob.Storage // the underlying WebDAV storage used to implement all methods.

	Options

	cmd          *exec.Cmd // running rclone
	temporaryDir string

	allTransfersComplete atomic.Bool // set to true when rclone process emits "Transferred:*100%"
	hasWrites            atomic.Bool // set to true when we had any writes
}

func (r *rcloneStorage) PutBlob(ctx context.Context, b blob.ID, data blob.Bytes, opts blob.PutOptions) error {
	err := r.Storage.PutBlob(ctx, b, data, opts)
	if err == nil {
		r.hasWrites.Store(true)
		return nil
	}

	return errors.Wrap(err, "error writing blob using WebDAV")
}

func (r *rcloneStorage) ConnectionInfo() blob.ConnectionInfo {
	return blob.ConnectionInfo{
		Type:   rcloneStorageType,
		Config: &r.Options,
	}
}

func (r *rcloneStorage) waitForTransfersToEnd(ctx context.Context) {
	if !r.hasWrites.Load() {
		log(ctx).Debugf("no writes in this session, no need to wait")
		return
	}

	log(ctx).Debugf("waiting for background rclone transfers to complete...")

	for !r.allTransfersComplete.Load() {
		log(ctx).Debugf("still waiting for background rclone transfers to complete...")
		time.Sleep(1 * time.Second)
	}

	log(ctx).Debugf("all background rclone transfers have completed.")
}

// Kill kills the rclone process. Used for testing.
func (r *rcloneStorage) Kill() {
	// this will kill rclone process if any
	if r.cmd != nil && r.cmd.Process != nil {
		r.cmd.Process.Kill() //nolint:errcheck
		r.cmd.Wait()         //nolint:errcheck
	}
}

func (r *rcloneStorage) Close(ctx context.Context) error {
	if !r.Options.NoWaitForTransfers {
		r.waitForTransfersToEnd(ctx)
	}

	if r.Storage != nil {
		if err := r.Storage.Close(ctx); err != nil {
			return errors.Wrap(err, "error closing webdav connection")
		}
	}

	// this will kill rclone process if any
	if r.cmd != nil && r.cmd.Process != nil {
		log(ctx).Debugf("killing rclone")
		r.cmd.Process.Kill() //nolint:errcheck
		r.cmd.Wait()         //nolint:errcheck
	}

	if r.temporaryDir != "" {
		if err := os.RemoveAll(r.temporaryDir); err != nil && !os.IsNotExist(err) {
			log(ctx).Errorf("error deleting temporary dir: %v", err)
		}
	}

	return nil
}

func (r *rcloneStorage) DisplayName() string {
	return "RClone " + r.Options.RemotePath
}

func (r *rcloneStorage) processStderrStatus(ctx context.Context, statsMarker string, s *bufio.Scanner) {
	for s.Scan() {
		l := s.Text()

		if r.Debug {
			log(ctx).Debugf("[RCLONE] %v", l)
		}

		if strings.Contains(l, statsMarker) {
			if strings.Contains(l, " 100%,") || strings.Contains(l, ", -,") {
				r.allTransfersComplete.Store(true)
			} else {
				r.allTransfersComplete.Store(false)
			}
		}
	}
}

func (r *rcloneStorage) runRCloneAndWaitForServerAddress(ctx context.Context, c *exec.Cmd, statsMarker string, startupTimeout time.Duration) (string, error) {
	rcloneAddressChan := make(chan string)
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

			var lastOutput string

			for s.Scan() {
				l := s.Text()
				lastOutput = l

				if p := strings.Index(l, "https://"); p >= 0 {
					rcloneAddressChan <- l[p:]

					go r.processStderrStatus(ctx, statsMarker, s)

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
		return "", err

	case <-time.After(startupTimeout):
		return "", errors.Errorf("timed out waiting for rclone to start")
	}
}

// New creates new RClone storage with specified options.
//
//nolint:funlen
func New(ctx context.Context, opt *Options, isCreate bool) (blob.Storage, error) {
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
	//nolint:gomnd
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

		//nolint:gomnd
		if err = os.WriteFile(tmpConfigFile, []byte(opt.EmbeddedConfig), 0o600); err != nil {
			return nil, errors.Wrap(err, "unable to write config file")
		}

		arguments = append(arguments, "--config", tmpConfigFile)
	}

	// append our mandatory arguments at the end so that they precedence over user-provided
	// arguments.
	arguments = append(arguments,
		"--addr", "127.0.0.1:0", // allocate random port,
		"--cert", temporaryCertPath,
		"--key", temporaryKeyPath,
		"--htpasswd", temporaryHtpassword,
		"--stats", "1s",
		"--stats-one-line",
		"--stats-one-line-date-format="+statsMarker,
		"--vfs-write-back=0s", // disable write-back, critical for correctness
	)

	r.cmd = exec.Command(rcloneExe, arguments...) //nolint:gosec
	r.cmd.Env = append(r.cmd.Env, opt.RCloneEnv...)

	// https://github.com/kopia/kopia/issues/1934
	osexec.DisableInterruptSignal(r.cmd)

	startupTimeout := rcloneStartupTimeout
	if opt.StartupTimeout != 0 {
		startupTimeout = time.Duration(opt.StartupTimeout) * time.Second
	}

	rcloneAddr, err := r.runRCloneAndWaitForServerAddress(ctx, r.cmd, statsMarker, startupTimeout)
	if err != nil {
		return nil, errors.Wrap(err, "unable to start rclone")
	}

	log(ctx).Debugf("detected webdav address: %v", rcloneAddr)

	fingerprintBytes := sha256.Sum256(cert.Raw)

	wst, err := webdav.New(ctx, &webdav.Options{
		URL:                                 rcloneAddr,
		Username:                            webdavUsername,
		Password:                            webdavPassword,
		TrustedServerCertificateFingerprint: hex.EncodeToString(fingerprintBytes[:]),
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
