package server

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewCookie(t *testing.T) {
	expires := time.Date(2026, time.October, 3, 0, 0, 0, 0, time.UTC)

	cookie := newCookie(false, "name", "value", expires)

	require.Equal(t, "name", cookie.Name)
	require.Equal(t, "value", cookie.Value)
	require.Equal(t, "/", cookie.Path)
	require.True(t, cookie.HttpOnly)
	require.Equal(t, http.SameSiteStrictMode, cookie.SameSite)
	require.False(t, cookie.Secure)
	require.Equal(t, expires, cookie.Expires)

	cookie = newCookie(true, "name", "value", time.Time{})

	require.True(t, cookie.Secure)
	require.True(t, cookie.Expires.IsZero())
}
