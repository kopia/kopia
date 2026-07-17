// Package tlsutil contains TLS utilities.
package tlsutil

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	stderrors "errors"
	"math/big"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/repo/logging"
)

const (
	privateKeyFileMode  = 0o600
	certificateFileMode = 0o600
)

var log = logging.Module("tls")

// GenerateServerCertificate generates random TLS certificate and key.
func GenerateServerCertificate(ctx context.Context, keySize int, certValid time.Duration, names []string) (*x509.Certificate, *rsa.PrivateKey, error) {
	log(ctx).Debug("generating new TLS certificate")

	priv, err := rsa.GenerateKey(rand.Reader, keySize)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to generate RSA key")
	}

	notBefore := clock.Now()
	notAfter := notBefore.Add(certValid)

	//nolint:mnd
	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to generate serial number")
	}

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Kopia"},
		},
		NotBefore: notBefore,
		NotAfter:  notAfter,

		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	for _, n := range names {
		if ip := net.ParseIP(n); ip != nil {
			log(ctx).Debugf("adding alternative IP to certificate: %v", ip)
			template.IPAddresses = append(template.IPAddresses, ip)
		} else {
			log(ctx).Debugf("adding alternative DNS name to certificate: %v", n)
			template.DNSNames = append(template.DNSNames, n)
		}
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, priv.Public(), priv)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to create certificate")
	}

	cert, err := x509.ParseCertificate(derBytes)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to parse certificate")
	}

	return cert, priv, nil
}

// WritePrivateKeyToFile writes the private key to a given file.
func WritePrivateKeyToFile(fname string, priv *rsa.PrivateKey) (err error) {
	f, err := os.OpenFile(fname, os.O_RDWR|os.O_CREATE|os.O_TRUNC, privateKeyFileMode) //nolint:gosec
	if err != nil {
		return errors.Wrap(err, "error opening private key file")
	}

	defer func() {
		err = stderrors.Join(err, f.Close())
	}()

	privBytes, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return errors.Wrap(err, "Unable to marshal private key")
	}

	if err := pem.Encode(f, &pem.Block{Type: "PRIVATE KEY", Bytes: privBytes}); err != nil {
		return errors.Wrap(err, "Failed to write data to")
	}

	return nil
}

// WriteCertificateToFile writes the certificate to a given file.
func WriteCertificateToFile(fname string, cert *x509.Certificate) (err error) {
	f, err := os.OpenFile(fname, os.O_RDWR|os.O_CREATE|os.O_TRUNC, certificateFileMode) //nolint:gosec
	if err != nil {
		return errors.Wrap(err, "error opening certificate file")
	}

	defer func() {
		err = stderrors.Join(err, f.Close())
	}()

	if err := pem.Encode(f, &pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw}); err != nil {
		return errors.Wrap(err, "Failed to write data")
	}

	return nil
}

// TLSConfigTrustingSingleCertificate return tls.Config which trusts exactly one TLS certificate with
// provided SHA256 fingerprint.
func TLSConfigTrustingSingleCertificate(sha256Fingerprint string) *tls.Config {
	sha256FingerprintBytes, err := hex.DecodeString(sha256Fingerprint)
	if err != nil {
		return &tls.Config{
			MinVersion: tls.VersionTLS12,
			VerifyPeerCertificate: func(_ [][]byte, _ [][]*x509.Certificate) error {
				return errors.Errorf("invalid SHA256 fingerprint %q", sha256Fingerprint)
			},
			VerifyConnection: func(tls.ConnectionState) error {
				return errors.Errorf("invalid SHA256 fingerprint %q", sha256Fingerprint)
			},
		}
	}

	return &tls.Config{
		InsecureSkipVerify:    true, //nolint:gosec
		VerifyPeerCertificate: verifyPeerCertificateFunction(sha256FingerprintBytes),
		VerifyConnection:      verifyConnectionFunction(sha256FingerprintBytes),
	}
}

// TransportTrustingSingleCertificate return http.RoundTripper which trusts exactly one TLS certificate with
// provided SHA256 fingerprint.
func TransportTrustingSingleCertificate(sha256Fingerprint string) http.RoundTripper {
	t2 := http.DefaultTransport.(*http.Transport).Clone() //nolint:forcetypeassert
	t2.TLSClientConfig = TLSConfigTrustingSingleCertificate(sha256Fingerprint)

	return t2
}

func verifyPeerCertificateFunction(sha256FingerprintBytes []byte) func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
		_ = verifiedChains

		return verifyPeerCertificate(sha256FingerprintBytes, rawCerts)
	}
}

func verifyPeerCertificate(sha256FingerprintBytes []byte, rawCerts [][]byte) error {
	var serverCerts [][]byte

	for _, c := range rawCerts {
		serverCertFingerPrint := sha256.Sum256(c)

		if bytes.Equal(serverCertFingerPrint[:], sha256FingerprintBytes) {
			return nil
		}

		serverCerts = append(serverCerts, serverCertFingerPrint[:])
	}

	return errors.Errorf("can't find certificate matching SHA256 fingerprint %x (server had %x)", sha256FingerprintBytes, serverCerts)
}

func verifyConnectionFunction(sha256FingerprintBytes []byte) func(s tls.ConnectionState) error {
	return func(s tls.ConnectionState) error {
		return verifyConnection(sha256FingerprintBytes, &s)
	}
}

func verifyConnection(sha256FingerprintBytes []byte, s *tls.ConnectionState) error {
	var serverCerts [][]byte

	for _, c := range s.PeerCertificates {
		serverCertFingerPrint := sha256.Sum256(c.Raw)

		if bytes.Equal(serverCertFingerPrint[:], sha256FingerprintBytes) {
			return nil
		}

		serverCerts = append(serverCerts, serverCertFingerPrint[:])
	}

	return errors.Errorf("can't find certificate matching SHA256 fingerprint %x (server had %x)", sha256FingerprintBytes, serverCerts)
}
