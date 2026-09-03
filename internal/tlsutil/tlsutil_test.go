package tlsutil_test

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
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

func TestTransportTrustingSingleCertificate_BadFingerprint(t *testing.T) {
	ctx := t.Context()
	certValid := 24 * time.Hour
	names := []string{"127.0.0.1", "localhost"}

	cert, _, err := tlsutil.GenerateServerCertificate(ctx, 2048, certValid, names)
	require.NoError(t, err, "generating server cert")

	cases := []struct {
		name        string
		fingerprint string
	}{
		{
			name:        "OddLength",
			fingerprint: strings.Repeat("a", 33),
		},
		{
			name:        "TooShort",
			fingerprint: strings.Repeat("a", 2),
		},
		{
			name:        "TooLong",
			fingerprint: strings.Repeat("a", 42),
		},
		{
			name:        "NotHex",
			fingerprint: "coffee",
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			transport := tlsutil.TransportTrustingSingleCertificate(testCase.fingerprint)
			require.NotNil(t, transport)

			verifyPeerCertificate := transport.(*http.Transport).TLSClientConfig.VerifyPeerCertificate //nolint:forcetypeassert

			rawCerts := [][]byte{cert.Raw}
			err := verifyPeerCertificate(rawCerts, nil)
			require.Error(t, err)
			require.ErrorContains(t, err, "invalid SHA256 fingerprint")
		})
	}
}

func TestTransportTrustingSingleClientCertificate_TestClientFlow(t *testing.T) {
	ctx := t.Context()
	certValid := 24 * time.Hour
	names := []string{"127.0.0.1", "localhost"}

	cert, priv, err := tlsutil.GenerateServerCertificate(ctx, 2048, certValid, names)
	require.NoError(t, err, "generating server cert")

	h := sha256.Sum256(cert.Raw)
	fingerprint := hex.EncodeToString(h[:])

	tlsCert := tls.Certificate{
		Certificate: [][]byte{cert.Raw},
		PrivateKey:  priv,
		Leaf:        cert,
	}

	cases := []struct {
		name         string
		getTransport func() *http.Transport
		wantResumed  bool
	}{
		{
			name: "NoResume",
			getTransport: func() *http.Transport {
				return tlsutil.TransportTrustingSingleCertificate(fingerprint).(*http.Transport) //nolint:forcetypeassert
			},
		},
		{
			name: "Resume",
			getTransport: func() *http.Transport {
				transport := tlsutil.TransportTrustingSingleCertificate(fingerprint).(*http.Transport) //nolint:forcetypeassert
				transport.TLSClientConfig.ClientSessionCache = tls.NewLRUClientSessionCache(0)

				return transport
			},
			wantResumed: true,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			srv := httptest.NewUnstartedServer(
				http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(http.StatusOK)
				}),
			)
			srv.TLS = &tls.Config{
				Certificates: []tls.Certificate{tlsCert},
			}
			srv.StartTLS()
			t.Cleanup(srv.Close)

			transport := testCase.getTransport()
			client := &http.Client{
				Transport: transport,
			}

			// Make multiple calls to allow checking resume behavior.
			for i := range 2 {
				resp, err := client.Get(srv.URL) //nolint:noctx
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, resp.StatusCode)

				wantResumed := testCase.wantResumed && i > 0
				require.Equal(t, wantResumed, resp.TLS.DidResume)

				resp.Body.Close()

				// Use to force a resume if the client has a configured TLS cache.
				transport.CloseIdleConnections()
			}
		})
	}
}
