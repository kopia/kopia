package server

import (
	"net"
	"net/http"
	"strings"

	"github.com/go-webauthn/webauthn/protocol"
	"github.com/go-webauthn/webauthn/webauthn"
	"github.com/pkg/errors"
)

type webAuthnUser struct {
	username string
	creds    *mfaUserCredentials
}

func (u *webAuthnUser) WebAuthnID() []byte {
	return u.creds.WebAuthnUserID
}

func (u *webAuthnUser) WebAuthnName() string {
	return u.username
}

func (u *webAuthnUser) WebAuthnDisplayName() string {
	return u.username
}

func (u *webAuthnUser) WebAuthnCredentials() []webauthn.Credential {
	return u.creds.Passkeys
}

func (s *Server) webAuthnForRequest(r *http.Request) (*webauthn.WebAuthn, error) {
	origin := requestOrigin(r)
	rpID := relyingPartyID(r)

	wa, err := webauthn.New(&webauthn.Config{
		RPID:          rpID,
		RPDisplayName: "Kopia",
		RPOrigins:     []string{origin},
		AuthenticatorSelection: protocol.AuthenticatorSelection{
			ResidentKey:        protocol.ResidentKeyRequirementPreferred,
			UserVerification:   protocol.VerificationPreferred,
			RequireResidentKey: protocol.ResidentKeyNotRequired(),
		},
	})
	if err != nil {
		return nil, errors.Wrap(err, "unable to configure WebAuthn")
	}

	return wa, nil
}

func requestOrigin(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}

	return scheme + "://" + r.Host
}

func relyingPartyID(r *http.Request) string {
	host := r.Host
	if h, _, err := net.SplitHostPort(host); err == nil {
		return h
	}

	return strings.Trim(host, "[]")
}
