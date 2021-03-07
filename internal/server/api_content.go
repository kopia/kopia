package server

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/gorilla/mux"

	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/manifest"
)

func (s *Server) handleContentGet(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	dr, ok := s.rep.(repo.DirectRepository)
	if !ok {
		return nil, notFoundError("content not found")
	}

	cid := content.ID(mux.Vars(r)["contentID"])

	data, err := dr.ContentReader().GetContent(ctx, cid)
	if errors.Is(err, content.ErrContentNotFound) {
		return nil, notFoundError("content not found")
	}

	return data, nil
}

func (s *Server) handleContentInfo(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	dr, ok := s.rep.(repo.DirectRepository)
	if !ok {
		return nil, notFoundError("content not found")
	}

	cid := content.ID(mux.Vars(r)["contentID"])

	ci, err := dr.ContentReader().ContentInfo(ctx, cid)

	switch {
	case err == nil:
		return ci, nil

	case errors.Is(err, content.ErrContentNotFound):
		return nil, notFoundError("content not found")

	default:
		return nil, internalServerError(err)
	}
}

func (s *Server) handleContentPut(ctx context.Context, r *http.Request, data []byte) (interface{}, *apiError) {
	dr, ok := s.rep.(repo.DirectRepositoryWriter)
	if !ok {
		return nil, repositoryNotWritableError()
	}

	cid := content.ID(mux.Vars(r)["contentID"])
	prefix := cid.Prefix()

	if strings.HasPrefix(string(prefix), manifest.ContentPrefix) {
		// it's not allowed to create contents prefixed with 'm' since those could be mistaken for manifest contents.
		return nil, accessDeniedError()
	}

	actualCID, err := dr.ContentManager().WriteContent(ctx, data, prefix)
	if err != nil {
		return nil, internalServerError(err)
	}

	if actualCID != cid {
		return nil, requestError(serverapi.ErrorMalformedRequest, "mismatched content ID")
	}

	return &serverapi.Empty{}, nil
}
