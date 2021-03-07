package server

import (
	"net/http"

	"github.com/kopia/kopia/internal/auth"
)

func requireUIUser(s *Server, r *http.Request) bool {
	user, _, _ := r.BasicAuth()

	return user == s.options.UIUser
}

func anyAuthenticatedUser(s *Server, r *http.Request) bool {
	return true
}

func handlerWillCheckAuthorization(s *Server, r *http.Request) bool {
	return true
}

func requireContentAccess(level auth.AccessLevel) isAuthorizedFunc {
	return func(s *Server, r *http.Request) bool {
		return s.httpAuthorizationInfo(r).ContentAccessLevel() >= level
	}
}

func hasManifestAccess(s *Server, r *http.Request, labels map[string]string, level auth.AccessLevel) bool {
	return s.httpAuthorizationInfo(r).ManifestAccessLevel(labels) >= level
}

var (
	_ isAuthorizedFunc = requireUIUser
	_ isAuthorizedFunc = anyAuthenticatedUser
	_ isAuthorizedFunc = handlerWillCheckAuthorization
)
