package tlsutil_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/tlsutil"
)

func TestGenerateServerCertificate(t *testing.T) {
	ctx := context.Background()
	certValid := 24 * time.Hour
	names := []string{"127.0.0.1", "localhost"}

	cert, priv, err := tlsutil.GenerateServerCertificate(ctx, 2048, certValid, names)
	require.NoError(t, err)
	require.NotNil(t, cert, "expected non-nil certificate")
	require.NotNil(t, priv, "expected non-nil private key")
	require.Len(t, cert.IPAddresses, 1)
	require.Equal(t, "127.0.0.1", cert.IPAddresses[0].String())
	require.Len(t, cert.DNSNames, 1)
	require.Equal(t, "localhost", cert.DNSNames[0])
	require.False(t, cert.NotBefore.After(clock.Now()), "certificate NotBefore is in the future")
	require.False(t, cert.NotAfter.Before(clock.Now().Add(certValid-time.Minute)), "certificate NotAfter is too early")
}

func TestTransportTrustingSingleCertificate(t *testing.T) {
	ctx := context.Background()
	certValid := 24 * time.Hour
	names := []string{"127.0.0.1", "localhost"}

	cert, _, err := tlsutil.GenerateServerCertificate(ctx, 2048, certValid, names)
	if err != nil {
		t.Fatalf("failed to generate server certificate: %v", err)
	}

	h := sha256.Sum256(cert.Raw)
	fingerprint := hex.EncodeToString(h[:])

	transport := tlsutil.TransportTrustingSingleCertificate(fingerprint)
	require.NotNil(t, transport)

	// Testing the VerifyPeerCertificate function
	verifyPeerCertificate := transport.(*http.Transport).TLSClientConfig.VerifyPeerCertificate //nolint:forcetypeassert

	t.Run("Test with the correct certificate", func(t *testing.T) {
		rawCerts := [][]byte{cert.Raw}
		err := verifyPeerCertificate(rawCerts, nil)
		require.NoError(t, err)
	})

	t.Run("Test with an incorrect certificate", func(t *testing.T) {
		invalidCert, _, err := tlsutil.GenerateServerCertificate(ctx, 2048, certValid, names)
		require.NoError(t, err)

		rawCerts := [][]byte{invalidCert.Raw}
		err = verifyPeerCertificate(rawCerts, nil)
		require.Error(t, err)
		require.ErrorContains(t, err, "can't find certificate matching SHA256 fingerprint")
	})
}
