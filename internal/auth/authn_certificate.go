package auth

import (
	"context"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"

	"github.com/kopia/kopia/repo"
)

type clientCertificateAuthenticator struct{}

func (c *clientCertificateAuthenticator) IsValid(
	ctx context.Context,
	_ repo.Repository,
	username string,
	_ string,
) bool {
	peerInfo, ok := peer.FromContext(ctx)
	if !ok {
		// Missing peer information
		log(ctx).Debug("Missing peer information for GRPC request")
		return false
	}

	if peerInfo == nil {
		// Missing peer information
		log(ctx).Debug("Nil peer information for GRPC request")
		return false
	}

	tlsInfo, ok := peerInfo.AuthInfo.(credentials.TLSInfo)
	if !ok {
		// This was not a TLS connection
		log(ctx).Debug("Peer authentication was not a TLS info")
		return false
	}

	for _, cert := range tlsInfo.State.PeerCertificates {
		// TODO(leonardoce): it would be nice to allow the user to map CNs to usernames.
		// This is the same thing PostgreSQL would do with pg_ident.
		if username == cert.Subject.CommonName {
			log(ctx).Debugf(
				"Found matching client certificate with serial %q for user %q",
				cert.SerialNumber.String(),
				username,
			)

			return true
		}

		log(ctx).Debugf("Client certificate common name %q does not correspond to the requested username %q",
			cert.Subject.CommonName,
			username)
	}

	return false
}

func (c *clientCertificateAuthenticator) Refresh(_ context.Context) error {
	return nil
}

// AuthenticateClientCertificateUsers returns authenticator that uses the client
// certificates to authenticate users.
func AuthenticateClientCertificateUsers() Authenticator {
	a := &clientCertificateAuthenticator{}

	return a
}
