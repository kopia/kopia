// Package tlsutil contains TLS utilities.
package tlsutil

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/repo/logging"
)

var log = logging.GetContextLoggerFunc("tls")

// GenerateServerCertificate generates random TLS certificate and key.
func GenerateServerCertificate(ctx context.Context, keySize int, certValid time.Duration, names []string) (*x509.Certificate, *rsa.PrivateKey, error) {
	log(ctx).Debugf("generating new TLS certificate")

	priv, err := rsa.GenerateKey(rand.Reader, keySize)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to generate RSA key")
	}

	notBefore := clock.Now()
	notAfter := notBefore.Add(certValid)

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
func WritePrivateKeyToFile(fname string, priv *rsa.PrivateKey) error {
	f, err := os.OpenFile(fname, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600) //nolint:gosec
	if err != nil {
		return err
	}
	defer f.Close() //nolint:errcheck,gosec

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
func WriteCertificateToFile(fname string, cert *x509.Certificate) error {
	f, err := os.OpenFile(fname, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600) //nolint:gosec
	if err != nil {
		return err
	}
	defer f.Close() //nolint:errcheck,gosec

	if err := pem.Encode(f, &pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw}); err != nil {
		return errors.Wrap(err, "Failed to write data")
	}

	return nil
}

// TransportTrustingSingleCertificate return http.RoundTripper which trusts exactly one TLS certificate with
// provided SHA256 fingerprint.
func TransportTrustingSingleCertificate(sha256Fingerprint string) http.RoundTripper {
	t2 := http.DefaultTransport.(*http.Transport).Clone()
	t2.TLSClientConfig = &tls.Config{
		InsecureSkipVerify:    true, //nolint:gosec
		VerifyPeerCertificate: verifyPeerCertificate(sha256Fingerprint),
	}

	return t2
}

func verifyPeerCertificate(sha256Fingerprint string) func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
	sha256Fingerprint = strings.ToLower(sha256Fingerprint)

	return func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
		for _, c := range rawCerts {
			h := sha256.Sum256(c)
			if hex.EncodeToString(h[:]) == sha256Fingerprint {
				return nil
			}
		}

		return errors.Errorf("can't find certificate matching SHA256 fingerprint %q", sha256Fingerprint)
	}
}
