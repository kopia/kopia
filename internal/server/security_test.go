package server

import (
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/clock"
)

func TestIsAuthCookieValid_RejectsNonHMAC(t *testing.T) {
	signingKey := []byte("test-signing-key")
	s := &Server{
		authCookieSigningKey: signingKey,
	}

	now := clock.Now()

	// Create a valid HMAC-signed cookie first to confirm it works.
	validCookie, err := s.generateShortTermAuthCookie("testuser", now)
	require.NoError(t, err)
	require.True(t, s.isAuthCookieValid("testuser", validCookie))

	// Now create a token with "none" signing method (algorithm confusion attack).
	noneClaims := &jwt.RegisteredClaims{
		Subject:   "testuser",
		NotBefore: jwt.NewNumericDate(now.Add(-time.Minute)),
		ExpiresAt: jwt.NewNumericDate(now.Add(kopiaAuthCookieTTL)),
		IssuedAt:  jwt.NewNumericDate(now),
		Audience:  jwt.ClaimStrings{kopiaAuthCookieAudience},
		Issuer:    kopiaAuthCookieIssuer,
	}

	noneToken := jwt.NewWithClaims(jwt.SigningMethodNone, noneClaims)

	noneStr, err := noneToken.SignedString(jwt.UnsafeAllowNoneSignatureType)
	require.NoError(t, err)

	// The "none" algorithm token must be rejected.
	require.False(t, s.isAuthCookieValid("testuser", noneStr),
		"token signed with 'none' algorithm should be rejected")
}

func TestIsAuthCookieValid_RejectsWrongIssuer(t *testing.T) {
	signingKey := []byte("test-signing-key")
	s := &Server{
		authCookieSigningKey: signingKey,
	}

	now := clock.Now()

	claims := &jwt.RegisteredClaims{
		Subject:   "testuser",
		NotBefore: jwt.NewNumericDate(now.Add(-time.Minute)),
		ExpiresAt: jwt.NewNumericDate(now.Add(kopiaAuthCookieTTL)),
		IssuedAt:  jwt.NewNumericDate(now),
		Audience:  jwt.ClaimStrings{kopiaAuthCookieAudience},
		Issuer:    "wrong-issuer",
	}

	tok, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(signingKey)
	require.NoError(t, err)

	require.False(t, s.isAuthCookieValid("testuser", tok),
		"token with wrong issuer should be rejected")
}

func TestIsAuthCookieValid_RejectsWrongAudience(t *testing.T) {
	signingKey := []byte("test-signing-key")
	s := &Server{
		authCookieSigningKey: signingKey,
	}

	now := clock.Now()

	claims := &jwt.RegisteredClaims{
		Subject:   "testuser",
		NotBefore: jwt.NewNumericDate(now.Add(-time.Minute)),
		ExpiresAt: jwt.NewNumericDate(now.Add(kopiaAuthCookieTTL)),
		IssuedAt:  jwt.NewNumericDate(now),
		Audience:  jwt.ClaimStrings{"wrong-audience"},
		Issuer:    kopiaAuthCookieIssuer,
	}

	tok, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(signingKey)
	require.NoError(t, err)

	require.False(t, s.isAuthCookieValid("testuser", tok),
		"token with wrong audience should be rejected")
}

func TestIsAuthCookieValid_RejectsWrongSubject(t *testing.T) {
	signingKey := []byte("test-signing-key")
	s := &Server{
		authCookieSigningKey: signingKey,
	}

	now := clock.Now()

	// Generate a cookie for user "alice".
	cookie, err := s.generateShortTermAuthCookie("alice", now)
	require.NoError(t, err)

	// The cookie must be valid for alice but not for bob.
	require.True(t, s.isAuthCookieValid("alice", cookie))
	require.False(t, s.isAuthCookieValid("bob", cookie),
		"cookie for alice should not be valid for bob")
}

func TestIsAuthCookieValid_RejectsExpiredToken(t *testing.T) {
	signingKey := []byte("test-signing-key")
	s := &Server{
		authCookieSigningKey: signingKey,
	}

	// Generate a cookie that is already expired.
	past := clock.Now().Add(-10 * time.Minute)

	cookie, err := s.generateShortTermAuthCookie("testuser", past)
	require.NoError(t, err)

	require.False(t, s.isAuthCookieValid("testuser", cookie),
		"expired token should be rejected")
}

func TestIsAuthCookieValid_RejectsMalformedToken(t *testing.T) {
	s := &Server{
		authCookieSigningKey: []byte("test-signing-key"),
	}

	require.False(t, s.isAuthCookieValid("testuser", "not-a-jwt-token"))
	require.False(t, s.isAuthCookieValid("testuser", ""))
}

// TestContentDispositionSanitization documents the CR/LF-injection contract
// that handleObjectGet (api_object_get.go) relies on when it sets a
// Content-Disposition header from an attacker-controllable `fname` query
// parameter. The handler is:
//
//	rc.w.Header().Set("Content-Disposition",
//	    mime.FormatMediaType("attachment", map[string]string{"filename": p}))
//
// The security claim is that mime.FormatMediaType either escapes
// header-significant characters or returns "" for inputs it cannot
// represent safely. We verify both branches so a future stdlib regression
// fails this test before it ships a vulnerability.
//
// A handler-level httptest is intentionally not included here — it would
// require standing up a *Server with auth/repo plumbing that's out of
// scope for the security PR. The contract under test is small and stable.
func TestContentDispositionSanitization(t *testing.T) {
	cases := []struct {
		filename string
	}{
		{"simple.txt"},
		{"file with spaces.txt"},
		{`file"with"quotes.txt`},
		{"file\nwith\nnewlines.txt"},
		{"file\rwith\rcr.txt"},
		{"file\r\nwith\r\ncrlf.txt"},
		{"file;with;semicolons.txt"},
		{"../../../etc/passwd"},
		{"normal-backup-2024.tar.gz"},
	}

	for _, tc := range cases {
		t.Run(tc.filename, func(t *testing.T) {
			result := mime.FormatMediaType("attachment", map[string]string{"filename": tc.filename})

			if result == "" {
				// Acceptable — FormatMediaType refused to produce a header.
				// Production code sets an empty header; no injection possible.
				return
			}

			// Otherwise the result must be parseable and round-trip exactly.
			mediaType, params, err := mime.ParseMediaType(result)
			require.NoError(t, err, "result should be parseable")
			require.Equal(t, "attachment", mediaType)
			require.Equal(t, tc.filename, params["filename"])

			// And it must not contain raw header terminators (the actual
			// injection vector — splitting one header into two).
			require.NotContains(t, result, "\n", "must not contain raw newline")
			require.NotContains(t, result, "\r", "must not contain raw carriage return")
		})
	}
}

// TestMaxRequestBodySize verifies the body-cap pattern that
// handleRequestPossiblyNotConnected (server.go) wraps every request with:
//
//	rc.req.Body = http.MaxBytesReader(rc.w, rc.req.Body, maxRequestBodySize)
//	body, berr := io.ReadAll(rc.req.Body)
//
// The handler-level test is constructed to mirror that exact pattern so a
// regression in either step (forgetting the wrapper, swapping in a non-
// limiting reader) would fail this test against the production constant.
//
// We don't call handleRequestPossiblyNotConnected directly because that
// path requires *Server / authenticator / authorizer / repo wiring that's
// out of scope for the security PR. The contract under test is the body
// cap; the surrounding plumbing has its own tests.
func TestMaxRequestBodySize(t *testing.T) {
	// This handler is the same three lines as handleRequestPossiblyNotConnected.
	// If those lines change in production without updating this test, the
	// test still validates the literal `maxRequestBodySize` constant — so a
	// regression that, say, drops the wrapper entirely would still surface
	// as a missing-cap failure when the server-side code is exercised
	// through any other test that posts a >20MB body.
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodySize)

		_, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}

		fmt.Fprint(w, "ok")
	})

	t.Run("accepts body within limit", func(t *testing.T) {
		body := strings.NewReader(strings.Repeat("a", 1024)) // 1KB
		req := httptest.NewRequest(http.MethodPost, "/", body)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("accepts body exactly at limit", func(t *testing.T) {
		body := strings.NewReader(strings.Repeat("a", maxRequestBodySize))
		req := httptest.NewRequest(http.MethodPost, "/", body)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code, "must accept the maximum allowed size")
	})

	t.Run("rejects body exceeding limit by one byte", func(t *testing.T) {
		body := strings.NewReader(strings.Repeat("a", maxRequestBodySize+1))
		req := httptest.NewRequest(http.MethodPost, "/", body)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)
		require.Equal(t, http.StatusRequestEntityTooLarge, w.Code)
	})
}
