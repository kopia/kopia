package server

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/internal/user"
	"github.com/kopia/kopia/repo"
	"github.com/pkg/errors"
)

func handleListUsers(ctx context.Context, rc requestContext) (any, *apiError) {
	profiles, err := user.ListUserProfiles(ctx, rc.rep)

	if err != nil {
		return nil, internalServerError(err)
	}

	if profiles == nil {
		profiles = []*user.Profile{}
	}

	resp := &serverapi.ProfilesResponse{
		Profiles: []*serverapi.Profile{},
	}

	for _, profile := range profiles {
		pf := fillProfile(profile)
		resp.Profiles = append(resp.Profiles, &pf)
	}

	return resp, nil
}

func handleGetUser(ctx context.Context, rc requestContext) (any, *apiError) {
	username := rc.muxVar("username")
	usr, err := user.GetUserProfile(ctx, rc.rep, username)

	if err != nil {
		if errors.Is(err, user.ErrUserNotFound) {
			return nil, notFoundError(err.Error())
		}
		return nil, internalServerError(err)
	}

	pf := fillProfile(usr)
	return &pf, nil
}

func handleDeleteUser(ctx context.Context, rc requestContext) (any, *apiError) {
	username := rc.muxVar("username")

	if _, err := user.GetUserProfile(ctx, rc.rep, username); errors.Is(err, user.ErrUserNotFound) {
		return nil, notFoundError(err.Error())
	}

	if err := repo.WriteSession(ctx, rc.rep, repo.WriteSessionOptions{
		Purpose: "handleDeleteUser",
	}, func(ctx context.Context, w repo.RepositoryWriter) error {
		return user.DeleteUserProfile(ctx, w, username)
	}); err != nil {
		return nil, internalServerError(errors.Wrap(err, "unable to delete user"))
	}

	return &serverapi.Empty{}, nil
}

func handleAddUser(ctx context.Context, rc requestContext) (any, *apiError) {
	req := &serverapi.AddProfileRequest{}
	if err := json.Unmarshal(rc.body, req); err != nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "malformed request body")
	}

	if err := user.ValidateUsername(req.Username); err != nil {
		return nil, requestError(serverapi.ErrorInvalidUsername, err.Error())
	}

	if err := repo.WriteSession(ctx, rc.rep, repo.WriteSessionOptions{
		Purpose: "handleAddUser",
	}, func(ctx context.Context, w repo.RepositoryWriter) error {
		np, err := user.GetNewProfile(ctx, w, req.Username)
		if err != nil {
			if errors.Is(err, user.ErrUserAlreadyExists) {
				return err
			}
			return err
		}

		if err := np.SetPassword(req.Password); err != nil {
			return err
		}

		if err := user.SetUserProfile(ctx, w, np); err != nil {
			return errors.Wrap(err, "error setting user profile")
		}
		return nil
	}); err != nil {
		return nil, internalServerError(err)
	}

	rc.srv.Refresh()

	usr, err := user.GetUserProfile(ctx, rc.rep, req.Username)
	if err != nil {
		return nil, internalServerError(err)
	}

	pf := fillProfile(usr)
	return &pf, nil
}

func handleUpdatePasswordUser(ctx context.Context, rc requestContext) (any, *apiError) {
	username := rc.muxVar("username")
	req := &serverapi.UpdateProfilePasswordRequest{}

	if err := json.Unmarshal(rc.body, req); err != nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "malformed request body")
	}

	usr, err := user.GetUserProfile(ctx, rc.rep, username)
	if err != nil {
		if errors.Is(err, user.ErrUserNotFound) {
			return nil, notFoundError(err.Error())
		}
		return nil, internalServerError(err)
	}

	if err := usr.SetPassword(req.Password); err != nil {
		return nil, internalServerError(err)
	}

	pf := fillProfile(usr)
	return &pf, nil
}

func fillProfile(usr *user.Profile) serverapi.Profile {
	usernameParts := strings.Split(usr.Username, "@")
	return serverapi.Profile{
		Username: usr.Username,
		User:     usernameParts[0],
		Hostname: usernameParts[1],
	}
}
