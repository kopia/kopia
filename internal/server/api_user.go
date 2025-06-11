package server

import (
	"context"

	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/repo"
)

func handleCurrentUser(ctx context.Context, _ requestContext) (any, *apiError) {
	return serverapi.CurrentUserResponse{
		Username: repo.GetDefaultUserName(ctx),
		Hostname: repo.GetDefaultHostName(ctx),
	}, nil
}
