package server

import (
	"context"
	"errors"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
)

func (s *Server) handleContentGet(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	dr, ok := s.rep.(*repo.DirectRepository)
	if !ok {
		return nil, notFoundError("content not found")
	}

	cid := content.ID(mux.Vars(r)["contentID"])

	data, err := dr.Content.GetContent(ctx, cid)
	if errors.Is(err, content.ErrContentNotFound) {
		return nil, notFoundError("content not found")
	}

	return data, nil
}

func (s *Server) handleContentInfo(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	dr, ok := s.rep.(*repo.DirectRepository)
	if !ok {
		return nil, notFoundError("content not found")
	}

	cid := content.ID(mux.Vars(r)["contentID"])

	ci, err := dr.Content.ContentInfo(ctx, cid)
	switch err {
	case nil:
		return ci, nil

	case content.ErrContentNotFound:
		return nil, notFoundError("content not found")

	default:
		return nil, internalServerError(err)
	}
}

func (s *Server) handleContentPut(ctx context.Context, r *http.Request, data []byte) (interface{}, *apiError) {
	dr, ok := s.rep.(*repo.DirectRepository)
	if !ok {
		return nil, notFoundError("content not found")
	}

	cid := content.ID(mux.Vars(r)["contentID"])
	prefix := cid.Prefix()

	actualCID, err := dr.Content.WriteContent(ctx, data, prefix)
	if err != nil {
		return nil, internalServerError(err)
	}

	if actualCID != cid {
		return nil, requestError(serverapi.ErrorMalformedRequest, "mismatched content ID")
	}

	return &serverapi.Empty{}, nil
}
