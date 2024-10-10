package cli

import (
	"bytes"
	"context"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/coreos/go-systemd/v22/activation"

	"github.com/kopia/kopia/internal/tlsutil"
)

const oneDay = 24 * time.Hour

func (c *commandServerStart) generateServerCertificate(ctx context.Context) (*x509.Certificate, *rsa.PrivateKey, error) {
	cert, key, err := tlsutil.GenerateServerCertificate(
		ctx,
		c.serverStartTLSGenerateRSAKeySize,
		time.Duration(c.serverStartTLSGenerateCertValidDays)*oneDay,
		c.serverStartTLSGenerateCertNames)

	return cert, key, errors.Wrap(err, "error generating server certificate")
}

func (c *commandServerStart) startServerWithOptionalTLS(ctx context.Context, httpServer *http.Server) error {
	var l net.Listener

	var err error

	listeners, err := activation.Listeners()
	if err != nil {
		return errors.Wrap(err, "socket-activation error")
	}

	switch len(listeners) {
	case 0:
		if strings.HasPrefix(httpServer.Addr, "unix:") {
			l, err = net.Listen("unix", strings.TrimPrefix(httpServer.Addr, "unix:"))
		} else {
			l, err = net.Listen("tcp", httpServer.Addr)
		}

		if err != nil {
			return errors.Wrap(err, "listen error")
		}
	case 1:
		l = listeners[0]
	default:
		return errors.Errorf("Too many activated sockets found.  Expected 1, got %v", len(listeners))
	}

	defer l.Close() //nolint:errcheck

	httpServer.Addr = l.Addr().String()

	return c.startServerWithOptionalTLSAndListener(ctx, httpServer, l)
}

func (c *commandServerStart) maybeGenerateTLS(ctx context.Context) error {
	if !c.serverStartTLSGenerateCert || c.serverStartTLSCertFile == "" || c.serverStartTLSKeyFile == "" {
		return nil
	}

	if _, err := os.Stat(c.serverStartTLSCertFile); err == nil {
		return errors.Errorf("TLS cert file already exists: %q", c.serverStartTLSCertFile)
	}

	if _, err := os.Stat(c.serverStartTLSKeyFile); err == nil {
		return errors.Errorf("TLS key file already exists: %q", c.serverStartTLSKeyFile)
	}

	cert, key, err := c.generateServerCertificate(ctx)
	if err != nil {
		return errors.Wrap(err, "unable to generate server cert")
	}

	fingerprint := sha256.Sum256(cert.Raw)
	fmt.Fprintf(c.out.stderr(), "SERVER CERT SHA256: %v\n", hex.EncodeToString(fingerprint[:])) //nolint:errcheck

	log(ctx).Infof("writing TLS certificate to %v", c.serverStartTLSCertFile)

	if err := tlsutil.WriteCertificateToFile(c.serverStartTLSCertFile, cert); err != nil {
		return errors.Wrap(err, "unable to write private key")
	}

	log(ctx).Infof("writing TLS private key to %v", c.serverStartTLSKeyFile)

	if err := tlsutil.WritePrivateKeyToFile(c.serverStartTLSKeyFile, key); err != nil {
		return errors.Wrap(err, "unable to write private key")
	}

	return nil
}

func (c *commandServerStart) startServerWithOptionalTLSAndListener(ctx context.Context, httpServer *http.Server, listener net.Listener) error {
	if err := c.maybeGenerateTLS(ctx); err != nil {
		return err
	}

	udsPfx := ""
	if listener.Addr().Network() == "unix" {
		udsPfx = "unix+"
	}

	switch {
	case c.serverStartTLSCertFile != "" && c.serverStartTLSKeyFile != "":
		// PEM files provided
		fmt.Fprintf(c.out.stderr(), "SERVER ADDRESS: %shttps://%v\n", udsPfx, httpServer.Addr) //nolint:errcheck
		c.showServerUIPrompt(ctx)

		return checkErrServerClosed(ctx, httpServer.ServeTLS(listener, c.serverStartTLSCertFile, c.serverStartTLSKeyFile), "error starting TLS server")

	case c.serverStartTLSGenerateCert:
		// PEM files not provided, generate in-memory TLS cert/key but don't persit.
		cert, key, err := c.generateServerCertificate(ctx)
		if err != nil {
			return errors.Wrap(err, "unable to generate server cert")
		}

		httpServer.TLSConfig = &tls.Config{
			MinVersion: tls.VersionTLS13,
			Certificates: []tls.Certificate{
				{
					Certificate: [][]byte{cert.Raw},
					PrivateKey:  key,
				},
			},
		}

		fingerprint := sha256.Sum256(cert.Raw)
		fmt.Fprintf(c.out.stderr(), "SERVER CERT SHA256: %v\n", hex.EncodeToString(fingerprint[:])) //nolint:errcheck

		if c.serverStartTLSPrintFullServerCert {
			// dump PEM-encoded server cert, only used by KopiaUI to securely connect.
			var b bytes.Buffer

			if err := pem.Encode(&b, &pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw}); err != nil {
				return errors.Wrap(err, "Failed to write data")
			}

			fmt.Fprintf(c.out.stderr(), "SERVER CERTIFICATE: %v\n", base64.StdEncoding.EncodeToString(b.Bytes())) //nolint:errcheck
		}

		fmt.Fprintf(c.out.stderr(), "SERVER ADDRESS: %shttps://%v\n", udsPfx, httpServer.Addr) //nolint:errcheck
		c.showServerUIPrompt(ctx)

		return checkErrServerClosed(ctx, httpServer.ServeTLS(listener, "", ""), "error starting TLS server")

	default:
		if !c.serverStartInsecure {
			return errors.New("TLS not configured. To start server without encryption pass --insecure")
		}

		fmt.Fprintf(c.out.stderr(), "SERVER ADDRESS: %shttp://%v\n", udsPfx, httpServer.Addr) //nolint:errcheck
		c.showServerUIPrompt(ctx)

		return checkErrServerClosed(ctx, httpServer.Serve(listener), "error starting server")
	}
}

func (c *commandServerStart) showServerUIPrompt(ctx context.Context) {
	if c.serverStartUI {
		log(ctx).Info("Open the address above in a web browser to use the UI.")
	}
}

func checkErrServerClosed(ctx context.Context, err error, msg string) error {
	if errors.Is(err, http.ErrServerClosed) {
		log(ctx).Debug("HTTP server closed:", err)

		return nil
	}

	return errors.Wrap(err, msg)
}
