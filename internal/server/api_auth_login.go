package server

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/serverapi"
)

const (
	authRequestMaxBytes = 1 << 20
	authFailureMessage  = "invalid username or password"
)

func handleAuthStatus(_ context.Context, rc requestContext) (any, *apiError) {
	s := mustServer(rc)
	sessionID := ensureSessionCookie(rc.w, rc.req)

	resp := &serverapi.AuthStatusResponse{
		CSRFToken: s.generateCSRFToken(sessionID),
	}

	if sess := s.loginSessions.get(sessionID); sess != nil {
		switch sess.State {
		case loginSessionAuthenticated:
			resp.Authenticated = true
			resp.Username = sess.Username
			resp.TOTPEnabled = s.mfaStore.isTOTPEnabled(sess.Username)
			resp.PasskeyAvailable = s.mfaStore.hasPasskeys(sess.Username)
		case loginSessionPendingMFA:
			resp.TOTPRequired = true
			resp.Username = sess.Username
		}
	}

	return resp, nil
}

func handleAuthLogin(ctx context.Context, rc requestContext) (any, *apiError) {
	s := mustServer(rc)

	if err := s.requireLoginRateLimit(rc.req); err != nil {
		return nil, err
	}

	var req serverapi.LoginRequest
	if err := json.Unmarshal(rc.body, &req); err != nil {
		return nil, unableToDecodeRequest(err)
	}

	req.Username = strings.TrimSpace(req.Username)
	if req.Username == "" || req.Password == "" {
		return nil, requestError(serverapi.ErrorMalformedRequest, "username and password required")
	}

	authn := s.getAuthenticator()
	if authn == nil {
		return &serverapi.LoginResponse{Status: "ok"}, nil
	}

	clientKey := clientRateLimitKey(rc.req)
	if !authn.IsValid(ctx, rc.rep, req.Username, req.Password) || (s.options.UIUser != "" && req.Username != s.options.UIUser) {
		s.loginLimiter.failure(clientKey)
		userLog(ctx).Warnf("failed form login by client %s for user %s", rc.req.RemoteAddr, req.Username)

		return nil, &apiError{http.StatusUnauthorized, serverapi.ErrorAuthFailed, authFailureMessage}
	}

	sessionID := ensureSessionCookie(rc.w, rc.req)

	if s.mfaStore.isTOTPEnabled(req.Username) {
		pending := s.loginSessions.markPendingMFA(sessionID, req.Username)
		hardenSessionCookie(rc.w, rc.req, pending.ID, pendingMFASessionTTL)

		return &serverapi.LoginResponse{
			Status:    "totp_required",
			Username:  req.Username,
			CSRFToken: s.generateCSRFToken(pending.ID),
		}, nil
	}

	return s.completeUILogin(ctx, rc, clientKey, sessionID, req.Username), nil
}

func handleAuthLoginTOTP(ctx context.Context, rc requestContext) (any, *apiError) {
	s := mustServer(rc)

	if err := s.requireLoginRateLimit(rc.req); err != nil {
		return nil, err
	}

	var req serverapi.TOTPVerifyRequest
	if err := json.Unmarshal(rc.body, &req); err != nil {
		return nil, unableToDecodeRequest(err)
	}

	sessionID, err := sessionIDFromRequest(rc.req)
	if err != nil {
		return nil, &apiError{http.StatusUnauthorized, serverapi.ErrorAuthFailed, authFailureMessage}
	}

	sess := s.loginSessions.get(sessionID)
	if sess == nil || sess.State != loginSessionPendingMFA {
		return nil, &apiError{http.StatusUnauthorized, serverapi.ErrorMFAPending, "TOTP verification not pending"}
	}

	secret, ok := s.mfaStore.totpSecret(sess.Username)
	if !ok {
		return nil, &apiError{http.StatusUnauthorized, serverapi.ErrorTOTPInvalid, "invalid TOTP code"}
	}

	clientKey := clientRateLimitKey(rc.req)
	if !validateTOTPCode(secret, req.Code) {
		s.loginLimiter.failure(clientKey)
		userLog(ctx).Warnf("failed TOTP login by client %s for user %s", rc.req.RemoteAddr, sess.Username)

		return nil, &apiError{http.StatusUnauthorized, serverapi.ErrorTOTPInvalid, "invalid TOTP code"}
	}

	return s.completeUILogin(ctx, rc, clientKey, sessionID, sess.Username), nil
}

func handleAuthLogout(_ context.Context, rc requestContext) (any, *apiError) {
	s := mustServer(rc)

	if c, err := rc.req.Cookie(kopiaSessionCookie); err == nil {
		s.loginSessions.delete(c.Value)
		clearCookie(rc.w, rc.req, kopiaSessionCookie)
	}

	clearCookie(rc.w, rc.req, kopiaAuthCookie)

	return serverapi.Empty{}, nil
}

func (s *Server) handlePublicAuth(f apiRequestFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rc := s.captureRequestContext(w, r)

		body, berr := io.ReadAll(http.MaxBytesReader(w, r.Body, authRequestMaxBytes))
		if berr != nil {
			writeAuthJSONError(w, http.StatusRequestEntityTooLarge, serverapi.ErrorMalformedRequest, "request body too large or unreadable")
			return
		}

		rc.body = body

		// GET bootstraps the anonymous session + CSRF token for the login page and API clients.
		// State-changing auth calls still require a matching session cookie and CSRF token.
		if r.Method == http.MethodGet {
			ensureSessionCookie(w, r)
		} else {
			if _, err := r.Cookie(kopiaSessionCookie); err != nil {
				writeAuthJSONError(w, http.StatusUnauthorized, serverapi.ErrorAuthFailed, "missing session cookie, reload the login page")
				return
			}

			if !s.validateCSRFToken(r) {
				writeAuthJSONError(w, http.StatusUnauthorized, serverapi.ErrorAuthFailed, "invalid or missing CSRF token, reload the login page")
				return
			}
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Cache-Control", "no-store")

		v, apiErr := f(r.Context(), rc)
		e := json.NewEncoder(w)

		if apiErr != nil {
			w.WriteHeader(apiErr.httpErrorCode)
			_ = e.Encode(&serverapi.ErrorResponse{
				Code:  apiErr.apiErrorCode,
				Error: apiErr.message,
			})

			return
		}

		if b, ok := v.([]byte); ok {
			_, _ = w.Write(b)
			return
		}

		_ = e.Encode(v)
	}
}

func writeAuthJSONError(w http.ResponseWriter, status int, code serverapi.APIErrorCode, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(&serverapi.ErrorResponse{
		Code:  code,
		Error: message,
	})
}

func mustServer(rc requestContext) *Server {
	s, ok := rc.srv.(*Server)
	if !ok {
		panic("requestContext.srv is not *Server")
	}

	return s
}

func (s *Server) requireLoginRateLimit(r *http.Request) *apiError {
	if s.loginLimiter.allow(clientRateLimitKey(r)) {
		return nil
	}

	return &apiError{http.StatusTooManyRequests, serverapi.ErrorRateLimited, "too many login attempts"}
}

func (s *Server) completeUILogin(ctx context.Context, rc requestContext, clientKey, oldSessionID, username string) *serverapi.LoginResponse {
	s.loginLimiter.success(clientKey)
	sess := s.loginSessions.markAuthenticated(oldSessionID, username)
	hardenSessionCookie(rc.w, rc.req, sess.ID, loginSessionTTL)

	if s.options.LogRequests {
		userLog(ctx).Infof("successful form login by client %s for user %s", rc.req.RemoteAddr, username)
	}

	return &serverapi.LoginResponse{
		Status:    "ok",
		Username:  username,
		CSRFToken: s.generateCSRFToken(sess.ID),
	}
}

func ensureSessionCookie(w http.ResponseWriter, r *http.Request) string {
	if c, err := r.Cookie(kopiaSessionCookie); err == nil && c.Value != "" {
		return c.Value
	}

	id := newSessionID()
	hardenSessionCookie(w, r, id, loginSessionTTL)
	r.AddCookie(&http.Cookie{Name: kopiaSessionCookie, Value: id})

	return id
}

func hardenSessionCookie(w http.ResponseWriter, r *http.Request, sessionID string, ttl time.Duration) {
	http.SetCookie(w, &http.Cookie{
		Name:     kopiaSessionCookie,
		Value:    sessionID,
		Path:     "/",
		MaxAge:   int(ttl.Seconds()),
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   r.TLS != nil,
	})
}

func clearCookie(w http.ResponseWriter, r *http.Request, name string) {
	http.SetCookie(w, &http.Cookie{
		Name:     name,
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   r.TLS != nil,
	})
}

func sessionIDFromRequest(r *http.Request) (string, error) {
	c, err := r.Cookie(kopiaSessionCookie)
	if err != nil || c.Value == "" {
		return "", errors.New("missing session cookie")
	}

	return c.Value, nil
}

func authenticatedUsername(rc requestContext) (string, bool) {
	s, ok := rc.srv.(*Server)
	if ok {
		if username, ok := s.uiSessionUsername(rc.req); ok {
			return username, true
		}
	}

	if user, _, ok := rc.req.BasicAuth(); ok && user != "" {
		return user, true
	}

	return "", false
}

// uiSessionUsername returns the username from a fully authenticated form-login session.
// Basic Auth alone never counts — that keeps the browser UI non-bypassable.
func (s *Server) uiSessionUsername(r *http.Request) (string, bool) {
	c, err := r.Cookie(kopiaSessionCookie)
	if err != nil || c.Value == "" {
		return "", false
	}

	sess := s.loginSessions.get(c.Value)
	if sess == nil || sess.State != loginSessionAuthenticated {
		return "", false
	}

	return sess.Username, true
}

func requireUISessionUser(rc requestContext) (string, *apiError) {
	s := mustServer(rc)

	username, ok := s.uiSessionUsername(rc.req)
	if !ok {
		return "", accessDeniedError()
	}

	if s.options.UIUser != "" && username != s.options.UIUser {
		return "", accessDeniedError()
	}

	return username, nil
}

func (s *Server) requirePasswordStepUp(ctx context.Context, rc requestContext, username, password string) *apiError {
	authn := s.getAuthenticator()
	if authn == nil {
		return nil
	}

	if strings.TrimSpace(password) == "" || !authn.IsValid(ctx, rc.rep, username, password) {
		return &apiError{http.StatusUnauthorized, serverapi.ErrorAuthFailed, "invalid password"}
	}

	return nil
}

func clientRateLimitKey(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}

	return host
}

func setSecurityHeaders(w http.ResponseWriter) {
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-Frame-Options", "DENY")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Security-Policy",
		"default-src 'none'; style-src 'self'; script-src 'self'; img-src 'self' data:; connect-src 'self'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'")
}
