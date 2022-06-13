package server

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/remoterepoapi"
	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/compression"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/manifest"
)

func handleContentGet(ctx context.Context, rc requestContext) (interface{}, *apiError) {
	dr, ok := rc.rep.(repo.DirectRepository)
	if !ok {
		return nil, notFoundError("content not found")
	}

	cid, err := content.ParseID(rc.muxVar("contentID"))
	if err != nil {
		return nil, notFoundError("content not found")
	}

	data, err := dr.ContentReader().GetContent(ctx, cid)
	if errors.Is(err, content.ErrContentNotFound) {
		return nil, notFoundError("content not found")
	}

	return data, nil
}

func handleContentInfo(ctx context.Context, rc requestContext) (interface{}, *apiError) {
	cid, err := content.ParseID(rc.muxVar("contentID"))
	if err != nil {
		return nil, notFoundError("content not found")
	}

	ci, err := rc.rep.ContentInfo(ctx, cid)

	switch {
	case err == nil:
		return ci, nil

	case errors.Is(err, content.ErrContentNotFound):
		return nil, notFoundError("content not found")

	default:
		return nil, internalServerError(err)
	}
}

func handleContentPut(ctx context.Context, rc requestContext) (interface{}, *apiError) {
	dr, ok := rc.rep.(repo.DirectRepositoryWriter)
	if !ok {
		return nil, repositoryNotWritableError()
	}

	cid, cerr := content.ParseID(rc.muxVar("contentID"))
	if cerr != nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "malformed content ID")
	}

	prefix := cid.Prefix()

	if strings.HasPrefix(string(prefix), manifest.ContentPrefix) {
		// it's not allowed to create contents prefixed with 'm' since those could be mistaken for manifest contents.
		return nil, accessDeniedError()
	}

	var comp compression.HeaderID

	if c := rc.queryParam("compression"); c != "" {
		// nolint:gomnd
		v, err := strconv.ParseInt(c, 16, 32)
		if err != nil {
			return nil, requestError(serverapi.ErrorMalformedRequest, "malformed compression ID")
		}

		comp = compression.HeaderID(v)
		if _, ok := compression.ByHeaderID[comp]; !ok {
			return nil, requestError(serverapi.ErrorMalformedRequest, "invalid compression ID")
		}
	}

	actualCID, err := dr.ContentManager().WriteContent(ctx, gather.FromSlice(rc.body), prefix, comp)
	if err != nil {
		return nil, internalServerError(err)
	}

	if actualCID != cid {
		return nil, requestError(serverapi.ErrorMalformedRequest, "mismatched content ID")
	}

	return &serverapi.Empty{}, nil
}

func handleContentPrefetch(ctx context.Context, rc requestContext) (interface{}, *apiError) {
	var req remoterepoapi.PrefetchContentsRequest

	if err := json.Unmarshal(rc.body, &req); err != nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "malformed request")
	}

	return &remoterepoapi.PrefetchContentsResponse{
		ContentIDs: rc.rep.PrefetchContents(ctx, req.ContentIDs, req.Hint),
	}, nil
}
