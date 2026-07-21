package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/pquerna/otp/totp"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/internal/auth"
	"github.com/kopia/kopia/internal/passwordpersist"
	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/internal/testutil"
)

func newLoginTestServer(t *testing.T) (*Server, *httptest.Server) {
	t.Helper()

	dir := testutil.TempDirectory(t)
	s, err := New(context.Background(), &Options{
		PasswordPersist:        passwordpersist.File(),
		Authorizer:             auth.LegacyAuthorizer(),
		Authenticator:          auth.AuthenticateSingleUser("kopia", "secret"),
		UIUser:                 "kopia",
		AuthCookieSigningKey:   "test-signing-key-for-login-ui",
		MFACredentialsFile:     filepath.Join(dir, "mfa.json"),
		UIPreferencesFile:      filepath.Join(dir, "ui.json"),
		DisableCSRFTokenChecks: false,
	})
	require.NoError(t, err)

	m := mux.NewRouter()
	s.SetupHTMLUIAPIHandlers(m)
	s.ServeStaticFiles(m, http.Dir(dir))

	hs := httptest.NewServer(m)
	t.Cleanup(hs.Close)

	return s, hs
}

func cookieValue(cookies []*http.Cookie, name string) string {
	for _, c := range cookies {
		if c.Name == name {
			return c.Value
		}
	}

	return ""
}

func doJSON(t *testing.T, hs *httptest.Server, method, path, sessionID, csrf string, body any) (*http.Response, []byte) {
	t.Helper()

	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		require.NoError(t, err)
		rdr = bytes.NewReader(b)
	}

	req, err := http.NewRequest(method, hs.URL+path, rdr) //nolint:noctx
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	if csrf != "" {
		req.Header.Set(apiclient.CSRFTokenHeader, csrf)
	}

	if sessionID != "" {
		req.AddCookie(&http.Cookie{Name: kopiaSessionCookie, Value: sessionID})
	}

	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	data, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.NoError(t, res.Body.Close())

	return res, data
}

func TestFormLoginRedirectsHTMLWithoutBasicChallenge(t *testing.T) {
	_, hs := newLoginTestServer(t)

	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	req, err := http.NewRequest(http.MethodGet, hs.URL+"/", http.NoBody) //nolint:noctx
	require.NoError(t, err)
	req.Header.Set("Accept", "text/html")

	res, err := client.Do(req)
	require.NoError(t, err)
	defer res.Body.Close() //nolint:errcheck

	require.Equal(t, http.StatusFound, res.StatusCode)
	require.Equal(t, "/login", res.Header.Get("Location"))
	require.Empty(t, res.Header.Get("WWW-Authenticate"))
}

func TestFormLoginAndSessionAccess(t *testing.T) {
	s, hs := newLoginTestServer(t)

	resLoginPage, err := http.Get(hs.URL + "/login") //nolint:noctx,gosec
	require.NoError(t, err)
	defer resLoginPage.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resLoginPage.StatusCode)
	require.Equal(t, "DENY", resLoginPage.Header.Get("X-Frame-Options"))
	require.Contains(t, resLoginPage.Header.Get("Content-Security-Policy"), "frame-ancestors 'none'")

	sessionID := cookieValue(resLoginPage.Cookies(), kopiaSessionCookie)
	require.NotEmpty(t, sessionID)
	csrf := s.generateCSRFToken(sessionID)

	resBad, bodyBad := doJSON(t, hs, http.MethodPost, "/api/v1/auth/login", sessionID, csrf, serverapi.LoginRequest{
		Username: "kopia",
		Password: "wrong",
	})
	require.Equal(t, http.StatusUnauthorized, resBad.StatusCode)
	require.Contains(t, string(bodyBad), "AUTH_FAILED")

	resOK, bodyOK := doJSON(t, hs, http.MethodPost, "/api/v1/auth/login", sessionID, csrf, serverapi.LoginRequest{
		Username: "kopia",
		Password: "secret",
	})
	require.Equal(t, http.StatusOK, resOK.StatusCode)

	var loginResp serverapi.LoginResponse
	require.NoError(t, json.Unmarshal(bodyOK, &loginResp))
	require.Equal(t, "ok", loginResp.Status)

	newSessionID := cookieValue(resOK.Cookies(), kopiaSessionCookie)
	require.NotEmpty(t, newSessionID)
	require.NotEqual(t, sessionID, newSessionID)
	require.NotEmpty(t, loginResp.CSRFToken)

	resStatus, bodyStatus := doJSON(t, hs, http.MethodGet, "/api/v1/auth/status", newSessionID, loginResp.CSRFToken, nil)
	require.Equal(t, http.StatusOK, resStatus.StatusCode)

	var status serverapi.AuthStatusResponse
	require.NoError(t, json.Unmarshal(bodyStatus, &status))
	require.True(t, status.Authenticated)
	require.Equal(t, "kopia", status.Username)

	resOld, _ := doJSON(t, hs, http.MethodGet, "/api/v1/auth/status", sessionID, csrf, nil)
	require.Equal(t, http.StatusOK, resOld.StatusCode)

	var oldStatus serverapi.AuthStatusResponse
	_, bodyOld := doJSON(t, hs, http.MethodGet, "/api/v1/auth/status", sessionID, csrf, nil)
	require.NoError(t, json.Unmarshal(bodyOld, &oldStatus))
	require.False(t, oldStatus.Authenticated)
	_ = resOld

	resLogout, _ := doJSON(t, hs, http.MethodPost, "/api/v1/auth/logout", newSessionID, loginResp.CSRFToken, serverapi.Empty{})
	require.Equal(t, http.StatusOK, resLogout.StatusCode)

	_, bodyStatus2 := doJSON(t, hs, http.MethodGet, "/api/v1/auth/status", newSessionID, loginResp.CSRFToken, nil)

	var status2 serverapi.AuthStatusResponse
	require.NoError(t, json.Unmarshal(bodyStatus2, &status2))
	require.False(t, status2.Authenticated)
}

func TestFormLoginWithTOTP(t *testing.T) {
	s, hs := newLoginTestServer(t)

	sessionID := newSessionID()
	csrf := s.generateCSRFToken(sessionID)

	key, err := totp.Generate(totp.GenerateOpts{
		Issuer:      "Kopia",
		AccountName: "kopia",
	})
	require.NoError(t, err)

	enc, err := s.mfaStore.encryptSecret(key.Secret())
	require.NoError(t, err)
	require.NoError(t, s.mfaStore.update("kopia", func(u *mfaUserCredentials) error {
		u.TOTPEnabled = true
		u.TOTPSecretEnc = enc

		return nil
	}))

	res, body := doJSON(t, hs, http.MethodPost, "/api/v1/auth/login", sessionID, csrf, serverapi.LoginRequest{
		Username: "kopia",
		Password: "secret",
	})
	require.Equal(t, http.StatusOK, res.StatusCode)

	var loginResp serverapi.LoginResponse
	require.NoError(t, json.Unmarshal(body, &loginResp))
	require.Equal(t, "totp_required", loginResp.Status)

	pendingSession := cookieValue(res.Cookies(), kopiaSessionCookie)
	require.NotEmpty(t, pendingSession)
	require.NotEqual(t, sessionID, pendingSession)
	require.NotEmpty(t, loginResp.CSRFToken)

	req, err := http.NewRequest(http.MethodGet, hs.URL+"/api/v1/repo/status", http.NoBody) //nolint:noctx
	require.NoError(t, err)
	req.Header.Set(apiclient.CSRFTokenHeader, loginResp.CSRFToken)
	req.AddCookie(&http.Cookie{Name: kopiaSessionCookie, Value: pendingSession})

	resDenied, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resDenied.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusUnauthorized, resDenied.StatusCode)

	code, err := totp.GenerateCode(key.Secret(), time.Now())
	require.NoError(t, err)

	res2, body2 := doJSON(t, hs, http.MethodPost, "/api/v1/auth/login/totp", pendingSession, loginResp.CSRFToken, serverapi.TOTPVerifyRequest{Code: code})
	require.Equal(t, http.StatusOK, res2.StatusCode)

	var okResp serverapi.LoginResponse
	require.NoError(t, json.Unmarshal(body2, &okResp))
	require.Equal(t, "ok", okResp.Status)

	newSessionID := cookieValue(res2.Cookies(), kopiaSessionCookie)
	require.NotEmpty(t, newSessionID)
	require.NotEqual(t, pendingSession, newSessionID)
}

func TestJSONUnauthorizedHasNoBasicChallenge(t *testing.T) {
	_, hs := newLoginTestServer(t)

	req, err := http.NewRequest(http.MethodGet, hs.URL+"/api/v1/repo/status", http.NoBody) //nolint:noctx
	require.NoError(t, err)
	req.Header.Set("Accept", "application/json")

	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer res.Body.Close() //nolint:errcheck

	require.Equal(t, http.StatusUnauthorized, res.StatusCode)
	require.Empty(t, res.Header.Get("WWW-Authenticate"))
}

func TestBasicAuthCannotOpenBrowserUI(t *testing.T) {
	_, hs := newLoginTestServer(t)

	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	req, err := http.NewRequest(http.MethodGet, hs.URL+"/", http.NoBody) //nolint:noctx
	require.NoError(t, err)
	req.Header.Set("Accept", "text/html")
	req.SetBasicAuth("kopia", "secret")

	res, err := client.Do(req)
	require.NoError(t, err)
	defer res.Body.Close() //nolint:errcheck

	require.Equal(t, http.StatusFound, res.StatusCode)
	require.Equal(t, "/login", res.Header.Get("Location"))

	reqSec, err := http.NewRequest(http.MethodGet, hs.URL+"/account/security", http.NoBody) //nolint:noctx
	require.NoError(t, err)
	reqSec.SetBasicAuth("kopia", "secret")

	resSec, err := client.Do(reqSec)
	require.NoError(t, err)
	defer resSec.Body.Close() //nolint:errcheck

	require.Equal(t, http.StatusFound, resSec.StatusCode)
	require.Equal(t, "/login", resSec.Header.Get("Location"))
}

func TestFormSessionRequiredForBrowserUI(t *testing.T) {
	s, hs := newLoginTestServer(t)

	resLoginPage, err := http.Get(hs.URL + "/login") //nolint:noctx,gosec
	require.NoError(t, err)
	defer resLoginPage.Body.Close() //nolint:errcheck

	sessionID := cookieValue(resLoginPage.Cookies(), kopiaSessionCookie)
	csrf := s.generateCSRFToken(sessionID)

	resOK, bodyOK := doJSON(t, hs, http.MethodPost, "/api/v1/auth/login", sessionID, csrf, serverapi.LoginRequest{
		Username: "kopia",
		Password: "secret",
	})
	require.Equal(t, http.StatusOK, resOK.StatusCode)

	var loginResp serverapi.LoginResponse
	require.NoError(t, json.Unmarshal(bodyOK, &loginResp))
	require.Equal(t, "ok", loginResp.Status)

	authSession := cookieValue(resOK.Cookies(), kopiaSessionCookie)
	require.NotEmpty(t, authSession)

	req, err := http.NewRequest(http.MethodGet, hs.URL+"/", http.NoBody) //nolint:noctx
	require.NoError(t, err)
	req.Header.Set("Accept", "text/html")
	req.AddCookie(&http.Cookie{Name: kopiaSessionCookie, Value: authSession})

	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer res.Body.Close() //nolint:errcheck

	require.Equal(t, http.StatusOK, res.StatusCode)
}

func TestAuthStatusBootstrapsCSRFWithoutToken(t *testing.T) {
	_, hs := newLoginTestServer(t)

	res, err := http.Get(hs.URL + "/api/v1/auth/status") //nolint:noctx,gosec
	require.NoError(t, err)
	defer res.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, res.StatusCode)

	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)

	var status serverapi.AuthStatusResponse
	require.NoError(t, json.Unmarshal(body, &status))
	require.NotEmpty(t, status.CSRFToken)
	require.NotEmpty(t, cookieValue(res.Cookies(), kopiaSessionCookie))
}

func TestTOTPBlocksBasicAuthForUIAPI(t *testing.T) {
	s, hs := newLoginTestServer(t)

	key, err := totp.Generate(totp.GenerateOpts{
		Issuer:      "Kopia",
		AccountName: "kopia",
	})
	require.NoError(t, err)

	enc, err := s.mfaStore.encryptSecret(key.Secret())
	require.NoError(t, err)
	require.NoError(t, s.mfaStore.update("kopia", func(u *mfaUserCredentials) error {
		u.TOTPEnabled = true
		u.TOTPSecretEnc = enc

		return nil
	}))

	resStatus, err := http.Get(hs.URL + "/api/v1/auth/status") //nolint:noctx,gosec
	require.NoError(t, err)

	bodyStatus, err := io.ReadAll(resStatus.Body)
	require.NoError(t, err)
	require.NoError(t, resStatus.Body.Close())

	var status serverapi.AuthStatusResponse
	require.NoError(t, json.Unmarshal(bodyStatus, &status))

	sessionID := cookieValue(resStatus.Cookies(), kopiaSessionCookie)
	require.NotEmpty(t, sessionID)
	require.NotEmpty(t, status.CSRFToken)

	req, err := http.NewRequest(http.MethodGet, hs.URL+"/api/v1/repo/status", http.NoBody) //nolint:noctx
	require.NoError(t, err)
	req.SetBasicAuth("kopia", "secret")
	req.Header.Set(apiclient.CSRFTokenHeader, status.CSRFToken)
	req.AddCookie(&http.Cookie{Name: kopiaSessionCookie, Value: sessionID})

	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer res.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusForbidden, res.StatusCode)
}

func TestBasicAuthStillWorksWithoutTOTP(t *testing.T) {
	_, hs := newLoginTestServer(t)

	resStatus, err := http.Get(hs.URL + "/api/v1/auth/status") //nolint:noctx,gosec
	require.NoError(t, err)

	bodyStatus, err := io.ReadAll(resStatus.Body)
	require.NoError(t, err)
	require.NoError(t, resStatus.Body.Close())

	var status serverapi.AuthStatusResponse
	require.NoError(t, json.Unmarshal(bodyStatus, &status))

	sessionID := cookieValue(resStatus.Cookies(), kopiaSessionCookie)
	require.NotEmpty(t, sessionID)

	req, err := http.NewRequest(http.MethodGet, hs.URL+"/api/v1/repo/status", http.NoBody) //nolint:noctx
	require.NoError(t, err)
	req.SetBasicAuth("kopia", "secret")
	req.Header.Set(apiclient.CSRFTokenHeader, status.CSRFToken)
	req.AddCookie(&http.Cookie{Name: kopiaSessionCookie, Value: sessionID})

	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer res.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, res.StatusCode)
}

func TestWithoutPasswordServesUI(t *testing.T) {
	dir := testutil.TempDirectory(t)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "index.html"), []byte("<html>ok</html>"), 0o644))

	s, err := New(context.Background(), &Options{
		PasswordPersist:      passwordpersist.File(),
		Authorizer:           auth.LegacyAuthorizer(),
		Authenticator:        nil,
		UIUser:               "kopia",
		AuthCookieSigningKey: "test-signing-key-for-login-ui",
		MFACredentialsFile:   filepath.Join(dir, "mfa.json"),
		UIPreferencesFile:    filepath.Join(dir, "ui.json"),
	})
	require.NoError(t, err)

	m := mux.NewRouter()
	s.SetupHTMLUIAPIHandlers(m)
	s.ServeStaticFiles(m, http.Dir(dir))

	hs := httptest.NewServer(m)
	t.Cleanup(hs.Close)

	req, err := http.NewRequest(http.MethodGet, hs.URL+"/", http.NoBody) //nolint:noctx
	require.NoError(t, err)
	req.Header.Set("Accept", "text/html")

	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer res.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, res.StatusCode)
}

func TestLoginRateLimit(t *testing.T) {
	s, hs := newLoginTestServer(t)

	sessionID := newSessionID()
	csrf := s.generateCSRFToken(sessionID)

	for i := 0; i < loginRateLimitMaxFails; i++ {
		res, _ := doJSON(t, hs, http.MethodPost, "/api/v1/auth/login", sessionID, csrf, serverapi.LoginRequest{
			Username: "kopia",
			Password: "wrong",
		})
		require.Equal(t, http.StatusUnauthorized, res.StatusCode)
	}

	res, body := doJSON(t, hs, http.MethodPost, "/api/v1/auth/login", sessionID, csrf, serverapi.LoginRequest{
		Username: "kopia",
		Password: "wrong",
	})
	require.Equal(t, http.StatusTooManyRequests, res.StatusCode)
	require.Contains(t, string(body), "RATE_LIMITED")
}
