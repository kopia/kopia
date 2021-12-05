package server

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"sort"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/ospath"
	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

func (s *Server) handleSourcesList(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	_, multiUser := s.rep.(repo.DirectRepository)

	resp := &serverapi.SourcesResponse{
		Sources:       []*serverapi.SourceStatus{},
		LocalHost:     s.rep.ClientOptions().Hostname,
		LocalUsername: s.rep.ClientOptions().Username,
		MultiUser:     multiUser,
	}

	for _, v := range s.sourceManagers {
		if !sourceMatchesURLFilter(v.src, r.URL.Query()) {
			continue
		}

		resp.Sources = append(resp.Sources, v.Status())
	}

	sort.Slice(resp.Sources, func(i, j int) bool {
		return resp.Sources[i].Source.String() < resp.Sources[j].Source.String()
	})

	return resp, nil
}

func (s *Server) handleSourcesCreate(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	var req serverapi.CreateSnapshotSourceRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "malformed request body")
	}

	if req.Path == "" {
		return nil, requestError(serverapi.ErrorMalformedRequest, "missing path")
	}

	if req.Policy == nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "missing policy")
	}

	req.Path = ospath.ResolveUserFriendlyPath(req.Path, true)

	_, err := os.Stat(req.Path)
	if os.IsNotExist(err) {
		return nil, requestError(serverapi.ErrorPathNotFound, "path does not exist")
	}

	if err != nil {
		return nil, internalServerError(err)
	}

	sourceInfo := snapshot.SourceInfo{
		UserName: s.rep.ClientOptions().Username,
		Host:     s.rep.ClientOptions().Hostname,
		Path:     req.Path,
	}

	resp := &serverapi.CreateSnapshotSourceResponse{}

	if err = repo.WriteSession(ctx, s.rep, repo.WriteSessionOptions{
		Purpose: "handleSourcesCreate",
	}, func(ctx context.Context, w repo.RepositoryWriter) error {
		// nolint:wrapcheck
		return policy.SetPolicy(ctx, w, sourceInfo, req.Policy)
	}); err != nil {
		return nil, internalServerError(errors.Wrap(err, "unable to set initial policy"))
	}

	// upgrade to exclusive lock to ensure we have source manager
	s.mu.RUnlock()
	s.mu.Lock()
	if s.sourceManagers[sourceInfo] == nil {
		log(ctx).Debugf("creating source manager for %v", sourceInfo)
		sm := newSourceManager(sourceInfo, s)
		s.sourceManagers[sourceInfo] = sm

		sm.refreshStatus(ctx)

		go sm.run(ctx)
	}
	s.mu.Unlock()
	s.mu.RLock()

	manager := s.sourceManagers[sourceInfo]
	if manager == nil {
		return nil, internalServerError(errors.Errorf("could not find source manager that was just created"))
	}

	if req.CreateSnapshot {
		resp.SnapshotStarted = true

		log(ctx).Debugf("scheduling snapshot of %v immediately...", sourceInfo)
		manager.scheduleSnapshotNow()
	}

	return resp, nil
}
