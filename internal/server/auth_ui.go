package server

import (
	"embed"
	"encoding/base64"
	"html/template"
	"io/fs"
	"net/http"
	"strconv"
	"strings"
)

//go:embed auth_ui.css auth_ui.js auth_nav.js login.js security.js login_logo.svg login_page.html security_page.html
var authUIFS embed.FS

var (
	loginPageTemplate    = template.Must(template.ParseFS(authUIFS, "login_page.html"))
	securityPageTemplate = template.Must(template.ParseFS(authUIFS, "security_page.html"))
)

type passkeyView struct {
	ID    string
	Label string
}

type authPageData struct {
	CSRFToken       string
	TitlePrefix     string
	DefaultUsername string
	Username        string
	TOTPEnabled     bool
	PasskeyCount    int
	Passkeys        []passkeyView
}

func (s *Server) serveAuthUIFile(name, contentType string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		b, err := fs.ReadFile(authUIFS, name)
		if err != nil {
			http.NotFound(w, r)
			return
		}

		w.Header().Set("Content-Type", contentType)
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		_, _ = w.Write(b)
	}
}

func (s *Server) uiTitlePrefix() string {
	if s.options.UITitlePrefix != "" {
		return s.options.UITitlePrefix + "Kopia"
	}

	return "Kopia"
}

func (s *Server) writeAuthHTML(w http.ResponseWriter, tmpl *template.Template, data authPageData) {
	setSecurityHeaders(w)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = tmpl.Execute(w, data)
}

func (s *Server) handleLoginPage(w http.ResponseWriter, r *http.Request) {
	if s.getAuthenticator() == nil {
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}

	if c, err := r.Cookie(kopiaSessionCookie); err == nil {
		if sess := s.loginSessions.get(c.Value); sess != nil && sess.State == loginSessionAuthenticated {
			http.Redirect(w, r, "/", http.StatusFound)
			return
		}
	}

	sessionID := ensureSessionCookie(w, r)
	s.writeAuthHTML(w, loginPageTemplate, authPageData{
		CSRFToken:       s.generateCSRFToken(sessionID),
		TitlePrefix:     s.uiTitlePrefix(),
		DefaultUsername: s.options.UIUser,
	})
}

func (s *Server) handleSecurityPage(w http.ResponseWriter, r *http.Request) {
	if s.getAuthenticator() == nil {
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}

	username, ok := s.uiSessionUsername(r)
	if !ok {
		http.Redirect(w, r, "/login", http.StatusFound)
		return
	}

	if s.options.UIUser != "" && username != s.options.UIUser {
		http.Error(w, "UI Access denied.", http.StatusForbidden)
		return
	}

	passkeys := []passkeyView{}
	if u := s.mfaStore.get(username); u != nil {
		for i, pk := range u.Passkeys {
			passkeys = append(passkeys, passkeyView{
				ID:    base64.RawURLEncoding.EncodeToString(pk.ID),
				Label: "Passkey " + strconv.Itoa(i+1),
			})
		}
	}

	sessionID := ensureSessionCookie(w, r)
	s.writeAuthHTML(w, securityPageTemplate, authPageData{
		CSRFToken:    s.generateCSRFToken(sessionID),
		TitlePrefix:  s.uiTitlePrefix(),
		Username:     username,
		TOTPEnabled:  s.mfaStore.isTOTPEnabled(username),
		PasskeyCount: len(passkeys),
		Passkeys:     passkeys,
	})
}

func wantsHTML(r *http.Request) bool {
	return strings.Contains(r.Header.Get("Accept"), "text/html")
}
