package auth_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"

	"github.com/kopia/kopia/internal/auth"
)

type fakeAuthInfo struct{}

func (fakeAuthInfo) AuthType() string {
	return "fake auth info"
}

func fakeCertificateForUser(user string) *x509.Certificate {
	return &x509.Certificate{
		Subject: pkix.Name{
			CommonName: user,
		},
	}
}

func TestCertificateAuthenticator(t *testing.T) {
	tests := []struct {
		name     string
		peerInfo *peer.Peer
		isValid  bool
		username string
	}{
		{
			name: "No peer info",
		},
		{
			name: "No TLS info",
			peerInfo: &peer.Peer{
				AuthInfo: fakeAuthInfo{},
			},
		},
		{
			name: "No certificates",
			peerInfo: &peer.Peer{
				AuthInfo: credentials.TLSInfo{
					State: tls.ConnectionState{
						PeerCertificates: nil,
					},
				},
			},
		},
		{
			name: "Single certificate, mismatching user name",
			peerInfo: &peer.Peer{
				AuthInfo: credentials.TLSInfo{
					State: tls.ConnectionState{
						PeerCertificates: []*x509.Certificate{
							fakeCertificateForUser("test@host"),
						},
					},
				},
			},
			username: "toast@host",
		},
		{
			name: "Single certificate, matching user name",
			peerInfo: &peer.Peer{
				AuthInfo: credentials.TLSInfo{
					State: tls.ConnectionState{
						PeerCertificates: []*x509.Certificate{
							fakeCertificateForUser("test@host"),
						},
					},
				},
			},
			username: "test@host",
			isValid:  true,
		},
		{
			name: "Multiple non-matching certificates",
			peerInfo: &peer.Peer{
				AuthInfo: credentials.TLSInfo{
					State: tls.ConnectionState{
						PeerCertificates: []*x509.Certificate{
							fakeCertificateForUser("test@host"),
							fakeCertificateForUser("toast@host"),
						},
					},
				},
			},
			username: "teeth@host",
		},
		{
			name: "Multiple certificates, the first in the list matches",
			peerInfo: &peer.Peer{
				AuthInfo: credentials.TLSInfo{
					State: tls.ConnectionState{
						PeerCertificates: []*x509.Certificate{
							fakeCertificateForUser("test@host"),
							fakeCertificateForUser("taste@host"),
							fakeCertificateForUser("toast@host"),
						},
					},
				},
			},
			username: "test@host",
			isValid:  true,
		},
		{
			name: "Multiple certificates, the last in the list matches",
			peerInfo: &peer.Peer{
				AuthInfo: credentials.TLSInfo{
					State: tls.ConnectionState{
						PeerCertificates: []*x509.Certificate{
							fakeCertificateForUser("test@host"),
							fakeCertificateForUser("taste@host"),
							fakeCertificateForUser("toast@host"),
						},
					},
				},
			},
			username: "toast@host",
			isValid:  true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := peer.NewContext(context.Background(), test.peerInfo)

			isValid := auth.AuthenticateClientCertificateUsers().IsValid(ctx, nil, test.username, "")
			require.Equal(t, test.isValid, isValid)
		})
	}
}
