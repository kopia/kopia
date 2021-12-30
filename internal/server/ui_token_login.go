package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/serverapi"
)

const (
	// number of random bytes per UI token.
	uiTokenLengthBytes = 16

	// query string parameter to pass the UI token, when present, the server will redirect to
	// URL without this query parameter and set the equivalent session cookie.
	uiTokenLoginQueryParameter = "uiAuthToken"

	// name of the cookie that grants user access to the UI without having to provide
	// HTTP authentication.
	kopiaUISessionCookie = "Kopia-UI-Session"
)

func (s *Server) sweepTokens(ctx context.Context) {
	n := clock.Now()

	s.pendingUITokens.Range(func(key, value interface{}) bool {
		exp, _ := value.(time.Time)

		if n.After(exp) {
			if _, ok := s.pendingUITokens.LoadAndDelete(key); ok {
				log(ctx).Debugw("removing unclaimed UI token", "expired", exp, "now", n)
			}
		}

		return true
	})
}

func (s *Server) generateUITokenLogin(ctx context.Context) (serverapi.UIAuthToken, error) {
	if s.options.SingleUseUIAuthTokenTTL <= 0 {
		return serverapi.UIAuthToken{}, errors.Errorf("UI token authentication is disabled")
	}

	var b [uiTokenLengthBytes]byte

	_, err := io.ReadFull(rand.Reader, b[:])
	if err != nil {
		return serverapi.UIAuthToken{}, errors.Wrap(err, "unable to generate random token")
	}

	tok := hex.EncodeToString(b[:])
	exp := clock.Now().Add(s.options.SingleUseUIAuthTokenTTL)

	s.sweepTokens(ctx)
	s.pendingUITokens.Store(tok, exp)

	return serverapi.UIAuthToken{
		Token:   tok,
		Expires: exp,
	}, nil
}

// checkAndInvalidateUIToken returns true if the provided token is valid and immediately
// invalidates it.
func (s *Server) checkAndInvalidateUIToken(token string) bool {
	v, ok := s.pendingUITokens.LoadAndDelete(token)
	if !ok {
		return false
	}

	expireTime, _ := v.(time.Time)

	return !clock.Now().After(expireTime)
}

func (s *Server) handleGenerateUIToken(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	// UI tokens only work for localhost.
	if !IsLocalhost(r.Host) {
		return nil, requestError(serverapi.ErrorMalformedRequest, "this API only supports localhost")
	}

	v, err := s.generateUITokenLogin(ctx)
	if err != nil {
		return nil, internalServerError(err)
	}

	return v, nil
}

// authenticateUsingUIToken checks if the request has `uiAuthToken` query string parameter,
// and if it's present and valid, exchanges it with a short-term session cookie and redirects
// to the same URL but without 'uiAuthToken' parameter.
func (s *Server) authenticateUsingUIToken(ctx context.Context, w http.ResponseWriter, r *http.Request) bool {
	// UI tokens only work for localhost.
	if !IsLocalhost(r.Host) {
		return false
	}

	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		log(ctx).Errorf("unable to parse URL query: %v", err)
		return false
	}

	uitl := q.Get(uiTokenLoginQueryParameter)
	if uitl == "" {
		// no query parameter provided.
		return false
	}

	if !s.checkAndInvalidateUIToken(uitl) {
		log(ctx).Debugw("invalid UI login token", "token", uitl)
		return false
	}

	s.setUIAuthorizedCookie(ctx, w)

	q.Del(uiTokenLoginQueryParameter)
	r.URL.RawQuery = q.Encode()

	http.Redirect(w, r, r.URL.String(), http.StatusFound)

	return true
}

func (s *Server) setUIAuthorizedCookie(ctx context.Context, w http.ResponseWriter) {
	now := clock.Now()

	ac, err := s.generateAuthCookie(s.options.UIUser, now, s.options.UISessionCookieTTL)
	if err != nil {
		log(ctx).Debugw("unable to generate UI auth cookie", "error", err)
		return
	}

	c := &http.Cookie{
		Name:     kopiaUISessionCookie,
		Value:    ac,
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		Path:     "/",
		Expires:  clock.Now().Add(s.options.UISessionCookieTTL),
	}

	http.SetCookie(w, c)
}
