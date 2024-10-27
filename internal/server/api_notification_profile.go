package server

import (
	"context"
	"encoding/json"

	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/notification/notifyprofile"
	"github.com/kopia/kopia/repo"
)

func handleNotificationProfileCreate(ctx context.Context, rc requestContext) (any, *apiError) {
	var cfg notifyprofile.Config

	if err := json.Unmarshal(rc.body, &cfg); err != nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "malformed request body: "+string(rc.body))
	}

	if err := repo.WriteSession(ctx, rc.rep, repo.WriteSessionOptions{
		Purpose: "NotificationProfileCreate",
	}, func(ctx context.Context, w repo.RepositoryWriter) error {
		return notifyprofile.SaveProfile(ctx, w, cfg)
	}); err != nil {
		return nil, internalServerError(err)
	}

	return &serverapi.Empty{}, nil
}

func handleNotificationProfileGet(ctx context.Context, rc requestContext) (any, *apiError) {
	cfg, ok, err := notifyprofile.GetProfile(ctx, rc.rep, rc.muxVar("profileName"))
	if err != nil {
		return nil, internalServerError(err)
	}

	if !ok {
		return nil, notFoundError("profile not found")
	}

	return cfg, nil
}

func handleNotificationProfileDelete(ctx context.Context, rc requestContext) (any, *apiError) {
	if err := repo.WriteSession(ctx, rc.rep, repo.WriteSessionOptions{
		Purpose: "NotificationProfileDelete",
	}, func(ctx context.Context, w repo.RepositoryWriter) error {
		return notifyprofile.DeleteProfile(ctx, w, rc.muxVar("profileName"))
	}); err != nil {
		return nil, internalServerError(err)
	}

	return &serverapi.Empty{}, nil
}

func handleNotificationProfileList(ctx context.Context, rc requestContext) (any, *apiError) {
	profiles, err := notifyprofile.ListProfiles(ctx, rc.rep)
	if err != nil {
		return nil, internalServerError(err)
	}

	return profiles, nil
}
