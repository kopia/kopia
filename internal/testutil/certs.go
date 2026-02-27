package testutil

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/clock"
)

const (
	// This is the PEM block type of elliptic curves private key.
	ecPrivateKeyPEMBlockType = "EC PRIVATE KEY"

	// This is the PEM block type for certificates.
	certificatePEMBlockType = "CERTIFICATE"

	// How many hours are in a day?
	hoursInDay = 24
)

var (
	// ErrInvalidPrivateKeyPEMBlockType is raised when parsing a private key generated
	// by this package and a wrong PEM block type is detected.
	ErrInvalidPrivateKeyPEMBlockType = errors.New("invalid private key PEM block type")

	// ErrInvalidPublicKeyPEMBlockType is raised when parsing a public key generated
	// by this package and a wrong PEM block type is detected.
	ErrInvalidPublicKeyPEMBlockType = errors.New("invalid public key PEM block type")

	// serialNumberLimit is the upper limit of the certificate serial number.
	serialNumberLimit int64 = 1000
)

// KeyPair represent a pair of keys to be used for asymmetric encryption and a
// certificate declaring the intended usage of those keys.
type KeyPair struct {
	// The private key PEM block
	Private []byte

	// The certificate PEM block
	Certificate []byte
}

// WriteTo writes the public and private key to two separate files.
func (pair *KeyPair) WriteTo(crt, key string) error {
	crtFile, err := os.Create(crt) //nolint:gosec
	if err != nil {
		return fmt.Errorf("while creating %v: %w", crt, err)
	}

	defer func() {
		_ = crtFile.Close()
	}()

	keyFile, err := os.Create(key) //nolint:gosec
	if err != nil {
		return fmt.Errorf("while creating %v: %w", key, err)
	}

	defer func() {
		_ = keyFile.Close()
	}()

	if _, err := crtFile.Write(pair.Certificate); err != nil {
		return fmt.Errorf("while writing to %v: %w", crt, err)
	}

	if _, err := keyFile.Write(pair.Private); err != nil {
		return fmt.Errorf("while writing to %v: %w", key, err)
	}

	return nil
}

// CreateAndSignClientCertificate given a CA keypair, generate and sign a leaf keypair.
func (pair *KeyPair) CreateAndSignClientCertificate(username string) (*KeyPair, error) {
	certificateDuration := hoursInDay * time.Hour
	notBefore := clock.Now().Add(time.Minute * -5)
	notAfter := notBefore.Add(certificateDuration)

	return pair.createAndSignPairWithValidity(username, notBefore, notAfter)
}

// CreateAndSignExpiredClientCertificate given a CA keypair, generate and sign a leaf keypair.
func (pair *KeyPair) CreateAndSignExpiredClientCertificate(username string) (*KeyPair, error) {
	notBefore := clock.Now().Add(-60 * time.Minute)
	notAfter := notBefore.Add(-40 * time.Minute)

	return pair.createAndSignPairWithValidity(username, notBefore, notAfter)
}

// ParseECPrivateKey parse the ECDSA private key stored in the pair.
func (pair *KeyPair) ParseECPrivateKey() (*ecdsa.PrivateKey, error) {
	block, _ := pem.Decode(pair.Private)
	if block == nil || block.Type != ecPrivateKeyPEMBlockType {
		return nil, ErrInvalidPrivateKeyPEMBlockType
	}

	result, err := x509.ParseECPrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("while parsing private key: %w", err)
	}

	return result, nil
}

// ParseCertificate parse certificate stored in the pair.
func (pair *KeyPair) ParseCertificate() (*x509.Certificate, error) {
	block, _ := pem.Decode(pair.Certificate)
	if block == nil || block.Type != certificatePEMBlockType {
		return nil, ErrInvalidPublicKeyPEMBlockType
	}

	result, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("while parsing public key: %w", err)
	}

	return result, nil
}

func (pair *KeyPair) createAndSignPairWithValidity(
	host string,
	notBefore,
	notAfter time.Time,
) (*KeyPair, error) {
	caCertificate, err := pair.ParseCertificate()
	if err != nil {
		return nil, err
	}

	caPrivateKey, err := pair.ParseECPrivateKey()
	if err != nil {
		return nil, err
	}

	// Generate a new private key
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("while generating ECDSA private key: %w", err)
	}

	// Sign the public part of this key with the CA
	serialNumber, err := rand.Int(rand.Reader, big.NewInt(serialNumberLimit))
	if err != nil {
		return nil, fmt.Errorf("can't generate serial number: %w", err)
	}

	leafTemplate := x509.Certificate{
		SerialNumber:          serialNumber,
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		BasicConstraintsValid: true,
		IsCA:                  false,
		Subject: pkix.Name{
			CommonName: host,
		},
	}

	leafTemplate.KeyUsage = x509.KeyUsageDigitalSignature | x509.KeyUsageKeyAgreement
	leafTemplate.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}

	certificateBytes, err := x509.CreateCertificate(
		rand.Reader, &leafTemplate, caCertificate, &leafKey.PublicKey, caPrivateKey)
	if err != nil {
		return nil, fmt.Errorf("while formatting X509 certificate: %w", err)
	}

	privateKey, err := x509.MarshalECPrivateKey(leafKey)
	if err != nil {
		return nil, fmt.Errorf("while formatting EC private key: %w", err)
	}

	return &KeyPair{
		Private:     encodePrivateKey(privateKey),
		Certificate: encodeCertificate(certificateBytes),
	}, nil
}

// CreateRootCA generates a CA returning its keys.
func CreateRootCA(commonName, organizationalUnit string) (*KeyPair, error) {
	certificateDuration := hoursInDay * time.Hour
	notBefore := clock.Now().Add(time.Minute * -5)
	notAfter := notBefore.Add(certificateDuration)

	return createCAWithValidity(notBefore, notAfter, commonName, organizationalUnit)
}

// createCAWithValidity create a CA with a certain validity, with a parent certificate and signed by a certain
// private key. If the latest two parameters are nil, the CA will be a root one (self-signed).
func createCAWithValidity(
	notBefore,
	notAfter time.Time,
	commonName string,
	organizationalUnit string,
) (*KeyPair, error) {
	serialNumber, err := rand.Int(rand.Reader, big.NewInt(serialNumberLimit))
	if err != nil {
		return nil, fmt.Errorf("while generating pseudorandom serial number: %w", err)
	}

	rootKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("while generating ECDSA private key: %w", err)
	}

	rootTemplate := x509.Certificate{
		SerialNumber:          serialNumber,
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		Subject: pkix.Name{
			CommonName: commonName,
			OrganizationalUnit: []string{
				organizationalUnit,
			},
		},
	}

	// self-signed certificate
	parentCertificate := &rootTemplate
	parentPrivateKey := rootKey

	certificateBytes, err := x509.CreateCertificate(
		rand.Reader,
		&rootTemplate,
		parentCertificate,
		&rootKey.PublicKey,
		parentPrivateKey)
	if err != nil {
		return nil, fmt.Errorf("while formatting X509 certificate: %w", err)
	}

	privateKey, err := x509.MarshalECPrivateKey(rootKey)
	if err != nil {
		return nil, fmt.Errorf("while formatting ECDSA private key: %w", err)
	}

	return &KeyPair{
		Private:     encodePrivateKey(privateKey),
		Certificate: encodeCertificate(certificateBytes),
	}, nil
}

func encodeCertificate(derBytes []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: certificatePEMBlockType, Bytes: derBytes})
}

func encodePrivateKey(derBytes []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: ecPrivateKeyPEMBlockType, Bytes: derBytes})
}
