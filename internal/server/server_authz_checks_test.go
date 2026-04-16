package server

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/apiclient"
)

func TestGenerateCSRFToken(t *testing.T) {
	s1 := &Server{
		authCookieSigningKey: []byte("some-key"),
	}

	s2 := &Server{
		authCookieSigningKey: []byte("some-other-key"),
	}

	cases := []struct {
		srv       *Server
		session   string
		wantToken string
	}{
		// CSRF token is a stable function of session ID and per-server so we can hardcode it
		{s1, "session1", "557c279a9203afbd5e1edd8a3b091fbcaf699841cd95058954e11886f0a3e6d0"},
		{s2, "session1", "7fd10608493e844581247d44e61de56b80df83ecdae49891f150823f73524ef7"},
		{s1, "session2", "e3aeba64243485ac4664e27445f48711b987c3bf5c7e58d1b89eb1e2722fedcd"},
		{s2, "session2", "714a124df0f6b6e79500fb06a900e7870a94f50b6d1e532c92a0abc0c63146f8"},
	}

	for _, tc := range cases {
		require.Equal(t, tc.wantToken, tc.srv.generateCSRFToken(tc.session))
	}
}

func TestValidateCSRFToken(t *testing.T) {
	s1 := &Server{
		authCookieSigningKey: []byte("some-key"),
	}

	s2 := &Server{
		authCookieSigningKey: []byte("some-other-key"),
	}

	s3 := &Server{
		options: Options{
			DisableCSRFTokenChecks: true,
		},
	}

	cases := []struct {
		srv     *Server
		session string
		token   string
		valid   bool
	}{
		// valid
		{s1, "session1", "557c279a9203afbd5e1edd8a3b091fbcaf699841cd95058954e11886f0a3e6d0", true},
		{s2, "session1", "7fd10608493e844581247d44e61de56b80df83ecdae49891f150823f73524ef7", true},
		{s1, "session2", "e3aeba64243485ac4664e27445f48711b987c3bf5c7e58d1b89eb1e2722fedcd", true},
		{s2, "session2", "714a124df0f6b6e79500fb06a900e7870a94f50b6d1e532c92a0abc0c63146f8", true},

		// invalid cases
		{s1, "", "557c279a9203afbd5e1edd8a3b091fbcaf699841cd95058954e11886f0a3e6d0", false}, // missing session ID
		{s1, "session2", "", false},        // missing token
		{s2, "session2", "invalid", false}, // invalid token

		// token is invalid but ignored, since 's3' does not validate tokens.
		{s3, "", "557c279a9203afbd5e1edd8a3b091fbcaf699841cd95058954e11886f0a3e6d0", true},
		{s3, "session2", "", true},
		{s3, "session2", "invalid-token", true},
	}

	ctx := context.Background()

	for i, tc := range cases {
		t.Run(fmt.Sprintf("case-%v", i), func(t *testing.T) {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, "/somepath", http.NoBody)
			require.NoError(t, err)

			if tc.session != "" {
				req.AddCookie(&http.Cookie{
					Name:  "Kopia-Session-Cookie",
					Value: tc.session,
				})
			}

			if tc.token != "" {
				req.Header.Add(apiclient.CSRFTokenHeader, tc.token)
			}

			require.Equal(t, tc.valid, tc.srv.validateCSRFToken(req))
		})
	}
}
