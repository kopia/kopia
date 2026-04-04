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

func TestContentDispositionSanitization(t *testing.T) {
	cases := []struct {
		filename string
	}{
		{"simple.txt"},
		{"file with spaces.txt"},
		{`file"with"quotes.txt`},
		{"file\nwith\nnewlines.txt"},
		{"file;with;semicolons.txt"},
		{"../../../etc/passwd"},
		{"normal-backup-2024.tar.gz"},
	}

	for _, tc := range cases {
		t.Run(tc.filename, func(t *testing.T) {
			result := mime.FormatMediaType("attachment", map[string]string{"filename": tc.filename})
			require.NotEmpty(t, result, "FormatMediaType should produce a valid result")

			// The result must be parseable.
			mediaType, params, err := mime.ParseMediaType(result)
			require.NoError(t, err, "result should be parseable")
			require.Equal(t, "attachment", mediaType)
			require.Equal(t, tc.filename, params["filename"])

			// The result must not contain raw unescaped newlines or unescaped quotes
			// that could be used for header injection.
			require.NotContains(t, result, "\n", "must not contain raw newline")
			require.NotContains(t, result, "\r", "must not contain raw carriage return")
		})
	}
}

func TestMaxRequestBodySize(t *testing.T) {
	// Create a handler that uses MaxBytesReader (simulating what handleRequestPossiblyNotConnected does).
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

	t.Run("rejects body exceeding limit", func(t *testing.T) {
		// Create a body larger than maxRequestBodySize (20MB + 1 byte).
		body := strings.NewReader(strings.Repeat("a", maxRequestBodySize+1))
		req := httptest.NewRequest(http.MethodPost, "/", body)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)
		require.Equal(t, http.StatusRequestEntityTooLarge, w.Code)
	})
}
