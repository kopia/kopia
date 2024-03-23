package server

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"io"
	"net/http"

	"github.com/kopia/kopia/internal/apiclient"
)

// kopiaSessionCookie is the name of the session cookie that Kopia server will generate for all
// UI sessions.
const kopiaSessionCookie = "Kopia-Session-Cookie"

func (s *Server) generateCSRFToken(sessionID string) string {
	h := hmac.New(sha256.New, s.authCookieSigningKey)

	if _, err := io.WriteString(h, sessionID); err != nil {
		panic("io.WriteString() failed: " + err.Error())
	}

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
		log(ctx).Warnf("missing or invalid session cookie for %q: %v", path, err)

		return false
	}

	validToken := s.generateCSRFToken(sessionCookie.Value)

	token := r.Header.Get(apiclient.CSRFTokenHeader)
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

func requireUIUser(ctx context.Context, rc requestContext) bool {
	if rc.srv.getAuthenticator() == nil {
		return true
	}

	if rc.srv.getOptions().UIUser == "" {
		return false
	}

	user, _, _ := rc.req.BasicAuth()

	return user == rc.srv.getOptions().UIUser
}

func requireServerControlUser(ctx context.Context, rc requestContext) bool {
	if rc.srv.getAuthenticator() == nil {
		return true
	}

	if rc.srv.getOptions().ServerControlUser == "" {
		return false
	}

	user, _, _ := rc.req.BasicAuth()

	return user == rc.srv.getOptions().ServerControlUser
}

func anyAuthenticatedUser(ctx context.Context, _ requestContext) bool {
	return true
}

func handlerWillCheckAuthorization(ctx context.Context, _ requestContext) bool {
	return true
}

var (
	_ isAuthorizedFunc = requireUIUser
	_ isAuthorizedFunc = anyAuthenticatedUser
	_ isAuthorizedFunc = handlerWillCheckAuthorization
)
