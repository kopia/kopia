package server

import (
	"context"
	"net/http"

	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/repo"
)

func (s *Server) handleCurrentUser(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	return serverapi.CurrentUserResponse{
		Username: repo.GetDefaultUserName(ctx),
		Hostname: repo.GetDefaultHostName(ctx),
	}, nil
}
