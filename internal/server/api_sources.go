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

	// ensure we have the policy for this source, otherwise it will not show up in the
	// list of sources at all.
	_, err = policy.GetDefinedPolicy(ctx, s.rep, sourceInfo)

	switch {
	case err == nil:
		// already have policy, do nothing
		log(ctx).Debugw("policy already exists", "source", sourceInfo)

		resp.Created = false

	case errors.Is(err, policy.ErrPolicyNotFound):
		resp.Created = true
		// don't have policy - create an empty one
		log(ctx).Debugw("policy not found, creating empty one", "source", sourceInfo)

		if err = repo.WriteSession(ctx, s.rep, repo.WriteSessionOptions{
			Purpose: "handleSourcesCreate",
		}, func(ctx context.Context, w repo.RepositoryWriter) error {
			// nolint:wrapcheck
			return policy.SetPolicy(ctx, w, sourceInfo, &req.InitialPolicy)
		}); err != nil {
			return nil, internalServerError(errors.Wrap(err, "unable to set initial policy"))
		}

	default:
		return nil, internalServerError(err)
	}

	// upgrade to exclusive lock to ensure we have source manager
	s.mu.RUnlock()
	s.mu.Lock()
	if s.sourceManagers[sourceInfo] == nil {
		log(ctx).Debugw("creating source manager", "source", sourceInfo)
		sm := newSourceManager(sourceInfo, s)
		s.sourceManagers[sourceInfo] = sm

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

		log(ctx).Debugw("scheduling snapshot immediately", "source", sourceInfo)
		manager.scheduleSnapshotNow()
	}

	return resp, nil
}
