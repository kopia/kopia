package cli

import (
	"context"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/tlsutil"
)

const oneDay = 24 * time.Hour

var (
	serverStartTLSGenerateCert          = serverStartCommand.Flag("tls-generate-cert", "Generate TLS certificate").Hidden().Bool()
	serverStartTLSCertFile              = serverStartCommand.Flag("tls-cert-file", "TLS certificate PEM").String()
	serverStartTLSKeyFile               = serverStartCommand.Flag("tls-key-file", "TLS key PEM file").String()
	serverStartTLSGenerateRSAKeySize    = serverStartCommand.Flag("tls-generate-rsa-key-size", "TLS RSA Key size (bits)").Hidden().Default("4096").Int()
	serverStartTLSGenerateCertValidDays = serverStartCommand.Flag("tls-generate-cert-valid-days", "How long should the TLS certificate be valid").Default("3650").Hidden().Int()
	serverStartTLSGenerateCertNames     = serverStartCommand.Flag("tls-generate-cert-name", "Host names/IP addresses to generate TLS certificate for").Default("127.0.0.1").Hidden().Strings()
)

func generateServerCertificate(ctx context.Context) (*x509.Certificate, *rsa.PrivateKey, error) {
	return tlsutil.GenerateServerCertificate(
		ctx,
		*serverStartTLSGenerateRSAKeySize,
		time.Duration(*serverStartTLSGenerateCertValidDays)*oneDay,
		*serverStartTLSGenerateCertNames)
}

func startServerWithOptionalTLS(ctx context.Context, httpServer *http.Server) error {
	l, err := net.Listen("tcp", httpServer.Addr)
	if err != nil {
		return errors.Wrap(err, "listen error")
	}
	defer l.Close() //nolint:errcheck

	httpServer.Addr = l.Addr().String()

	return startServerWithOptionalTLSAndListener(ctx, httpServer, l)
}

func maybeGenerateTLS(ctx context.Context) error {
	if !*serverStartTLSGenerateCert || *serverStartTLSCertFile == "" || *serverStartTLSKeyFile == "" {
		return nil
	}

	if _, err := os.Stat(*serverStartTLSCertFile); err == nil {
		return errors.Errorf("TLS cert file already exists: %q", *serverStartTLSCertFile)
	}

	if _, err := os.Stat(*serverStartTLSKeyFile); err == nil {
		return errors.Errorf("TLS key file already exists: %q", *serverStartTLSKeyFile)
	}

	cert, key, err := generateServerCertificate(ctx)
	if err != nil {
		return errors.Wrap(err, "unable to generate server cert")
	}

	fingerprint := sha256.Sum256(cert.Raw)
	fmt.Fprintf(os.Stderr, "SERVER CERT SHA256: %v\n", hex.EncodeToString(fingerprint[:]))

	log(ctx).Infof("writing TLS certificate to %v", *serverStartTLSCertFile)

	if err := tlsutil.WriteCertificateToFile(*serverStartTLSCertFile, cert); err != nil {
		return errors.Wrap(err, "unable to write private key")
	}

	log(ctx).Infof("writing TLS private key to %v", *serverStartTLSKeyFile)

	if err := tlsutil.WritePrivateKeyToFile(*serverStartTLSKeyFile, key); err != nil {
		return errors.Wrap(err, "unable to write private key")
	}

	return nil
}

func startServerWithOptionalTLSAndListener(ctx context.Context, httpServer *http.Server, listener net.Listener) error {
	if err := maybeGenerateTLS(ctx); err != nil {
		return err
	}

	switch {
	case *serverStartTLSCertFile != "" && *serverStartTLSKeyFile != "":
		// PEM files provided
		fmt.Fprintf(os.Stderr, "SERVER ADDRESS: https://%v\n", httpServer.Addr)
		showServerUIPrompt()

		return httpServer.ServeTLS(listener, *serverStartTLSCertFile, *serverStartTLSKeyFile)

	case *serverStartTLSGenerateCert:
		// PEM files not provided, generate in-memory TLS cert/key but don't persit.
		cert, key, err := generateServerCertificate(ctx)
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
		fmt.Fprintf(os.Stderr, "SERVER CERT SHA256: %v\n", hex.EncodeToString(fingerprint[:]))
		fmt.Fprintf(os.Stderr, "SERVER ADDRESS: https://%v\n", httpServer.Addr)
		showServerUIPrompt()

		return httpServer.ServeTLS(listener, "", "")

	default:
		fmt.Fprintf(os.Stderr, "SERVER ADDRESS: http://%v\n", httpServer.Addr)
		showServerUIPrompt()

		return httpServer.Serve(listener)
	}
}

func showServerUIPrompt() {
	if *serverStartUI {
		printStderr("\nOpen the address above in a web browser to use the UI.\n")
	}
}
