package server

import (
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"

	"github.com/kopia/kopia/internal/ospath"
	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/snapshot"
)

func (s *Server) handlePathResolve(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	var req serverapi.ResolvePathRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "malformed request body")
	}

	return &serverapi.ResolvePathResponse{
		SourceInfo: snapshot.SourceInfo{
			Path:     filepath.Clean(ospath.ResolveUserFriendlyPath(req.Path, true)),
			Host:     s.rep.ClientOptions().Hostname,
			UserName: s.rep.ClientOptions().Username,
		},
	}, nil
}
