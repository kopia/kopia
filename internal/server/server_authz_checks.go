package server

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"io"
	"net/http"

	"github.com/kopia/kopia/internal/auth"
)

const kopiaSessionCookie = "Kopia-Session-Cookie"

func (s *Server) generateCSRFToken(sessionID string) string {
	h := hmac.New(sha256.New, s.authCookieSigningKey)
	io.WriteString(h, sessionID) //nolint:errcheck

	return hex.EncodeToString(h.Sum(nil))
}

func (s *Server) validateCSRFToken(r *http.Request) bool {
	if s.options.DisableCSRFTokenChecks {
		return true
	}

	ctx := r.Context()
	path := r.URL.Path

	sessionCookie, err := r.Cookie(kopiaSessionCookie)
	if err != nil {
		log(ctx).Warnf("missing or invalid session cookie for %v", path)

		return false
	}

	validToken := s.generateCSRFToken(sessionCookie.Value)

	token := r.Header.Get("X-Kopia-Csrf-Token")
	if token == "" {
		log(ctx).Warnf("missing CSRF token for %v", path)
		return false
	}

	if subtle.ConstantTimeCompare([]byte(validToken), []byte(token)) == 1 {
		return true
	}

	log(ctx).Warnf("got invalid CSRF token for %v: %v, want %v, session %v", path, token, validToken, sessionCookie.Value)

	return false
}

func requireUIUser(s *Server, r *http.Request) bool {
	if s.authenticator == nil {
		return true
	}

	if s.options.UIUser == "" {
		return false
	}

	user, _, _ := r.BasicAuth()

	return user == s.options.UIUser
}

func requireServerControlUser(s *Server, r *http.Request) bool {
	if s.authenticator == nil {
		return true
	}

	if s.options.ServerControlUser == "" {
		return false
	}

	user, _, _ := r.BasicAuth()

	return user == s.options.ServerControlUser
}

func anyAuthenticatedUser(s *Server, r *http.Request) bool {
	return true
}

func handlerWillCheckAuthorization(s *Server, r *http.Request) bool {
	return true
}

func requireContentAccess(level auth.AccessLevel) isAuthorizedFunc {
	return func(s *Server, r *http.Request) bool {
		return s.httpAuthorizationInfo(r.Context(), r).ContentAccessLevel() >= level
	}
}

func hasManifestAccess(ctx context.Context, s *Server, r *http.Request, labels map[string]string, level auth.AccessLevel) bool {
	return s.httpAuthorizationInfo(ctx, r).ManifestAccessLevel(labels) >= level
}

var (
	_ isAuthorizedFunc = requireUIUser
	_ isAuthorizedFunc = anyAuthenticatedUser
	_ isAuthorizedFunc = handlerWillCheckAuthorization
)
