package testutil

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kopia/kopia/internal/clock"
)

const (
	testKeyBits    = 2048
	serialBitsSize = 128
	certValidHours = 24
)

// CreateRootCA creates a self-signed CA certificate and key for tests.
//
// NOTE: not named/shaped after upstream PR kopia/kopia#4886's
// internal/testutil/certs.go (checked via `gh pr diff 4886`): that PR's
// CreateRootCA takes (commonName, organizationalUnit string) and signs
// client certificates. This helper is unrelated (server cert CA trust) and
// deliberately uses different names to avoid colliding on merge.
func CreateRootCA(t *testing.T) (*x509.Certificate, *rsa.PrivateKey) {
	t.Helper()

	priv, err := rsa.GenerateKey(rand.Reader, testKeyBits)
	if err != nil {
		t.Fatalf("generating CA key: %v", err)
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), serialBitsSize))
	if err != nil {
		t.Fatalf("generating CA serial: %v", err)
	}

	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             clock.Now().Add(-time.Minute),
		NotAfter:              clock.Now().Add(certValidHours * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("creating CA certificate: %v", err)
	}

	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parsing CA certificate: %v", err)
	}

	return cert, priv
}

// CreateAndSignServerCertificate creates a leaf certificate for names,
// signed by ca/caKey. Each name is added as an IP SAN or DNS SAN.
func CreateAndSignServerCertificate(
	t *testing.T, ca *x509.Certificate, caKey *rsa.PrivateKey, names ...string,
) (*x509.Certificate, *rsa.PrivateKey) {
	t.Helper()
	return createAndSignServerCertificate(t, ca, caKey, clock.Now().Add(-time.Minute), clock.Now().Add(certValidHours*time.Hour), names...)
}

// CreateAndSignExpiredServerCertificate creates an already-expired leaf
// certificate for names, signed by ca/caKey.
func CreateAndSignExpiredServerCertificate(
	t *testing.T, ca *x509.Certificate, caKey *rsa.PrivateKey, names ...string,
) (*x509.Certificate, *rsa.PrivateKey) {
	t.Helper()
	return createAndSignServerCertificate(t, ca, caKey, clock.Now().Add(-2*time.Hour), clock.Now().Add(-time.Hour), names...)
}

func createAndSignServerCertificate(
	t *testing.T, ca *x509.Certificate, caKey *rsa.PrivateKey, notBefore, notAfter time.Time, names ...string,
) (*x509.Certificate, *rsa.PrivateKey) {
	t.Helper()

	priv, err := rsa.GenerateKey(rand.Reader, testKeyBits)
	if err != nil {
		t.Fatalf("generating leaf key: %v", err)
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), serialBitsSize))
	if err != nil {
		t.Fatalf("generating leaf serial: %v", err)
	}

	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: names[0]},
		NotBefore:    notBefore,
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	for _, n := range names {
		if ip := net.ParseIP(n); ip != nil {
			tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
		} else {
			tmpl.DNSNames = append(tmpl.DNSNames, n)
		}
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &priv.PublicKey, caKey)
	if err != nil {
		t.Fatalf("creating leaf certificate: %v", err)
	}

	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parsing leaf certificate: %v", err)
	}

	return cert, priv
}

// WriteCertPEM writes cert as PEM to dir/name, returns the path.
func WriteCertPEM(t *testing.T, dir, name string, cert *x509.Certificate) string {
	t.Helper()
	return writePEM(t, dir, name, "CERTIFICATE", cert.Raw)
}

// WriteKeyPEM writes an RSA private key as PEM to dir/name, returns the path.
func WriteKeyPEM(t *testing.T, dir, name string, key *rsa.PrivateKey) string {
	t.Helper()
	return writePEM(t, dir, name, "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(key))
}

// CertPEM returns cert PEM-encoded.
func CertPEM(t *testing.T, cert *x509.Certificate) []byte {
	t.Helper()
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw})
}

func writePEM(t *testing.T, dir, name, blockType string, contents []byte) string {
	t.Helper()

	path := filepath.Join(dir, name)

	f, err := os.Create(path) //nolint:gosec
	if err != nil {
		t.Fatalf("creating %v: %v", path, err)
	}
	defer f.Close() //nolint:errcheck

	if err := pem.Encode(f, &pem.Block{Type: blockType, Bytes: contents}); err != nil {
		t.Fatalf("writing %v: %v", path, err)
	}

	return path
}
