package server

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"sort"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/ctxutil"
	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

func (s *Server) handleSourcesList(ctx context.Context, r *http.Request) (interface{}, *apiError) {
	resp := &serverapi.SourcesResponse{
		Sources:       []*serverapi.SourceStatus{},
		LocalHost:     s.rep.Hostname,
		LocalUsername: s.rep.Username,
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

func (s *Server) handleSourcesCreate(ctx context.Context, r *http.Request) (interface{}, *apiError) {
	var req serverapi.CreateSnapshotSourceRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "malformed request body")
	}

	if req.Path == "" {
		return nil, requestError(serverapi.ErrorMalformedRequest, "missing path")
	}

	_, err := os.Stat(req.Path)
	if os.IsNotExist(err) {
		return nil, requestError(serverapi.ErrorPathNotFound, "path does not exist")
	}

	if err != nil {
		return nil, internalServerError(err)
	}

	sourceInfo := snapshot.SourceInfo{
		UserName: s.rep.Username,
		Host:     s.rep.Hostname,
		Path:     req.Path,
	}

	resp := &serverapi.CreateSnapshotSourceResponse{}

	// ensure we have the policy for this source, otherwise it will not show up in the
	// list of sources at all.
	_, err = policy.GetDefinedPolicy(ctx, s.rep, sourceInfo)
	switch err {
	case nil:
		// already have policy, do nothing
		log(ctx).Debugf("policy for %v already exists", sourceInfo)

		resp.Created = false

	case policy.ErrPolicyNotFound:
		resp.Created = true
		// don't have policy - create an empty one
		log(ctx).Debugf("policy for %v not found, creating empty one", sourceInfo)

		if err = policy.SetPolicy(ctx, s.rep, sourceInfo, &req.InitialPolicy); err != nil {
			return nil, internalServerError(errors.Wrap(err, "unable to set initial policy"))
		}

		if err = s.rep.Flush(ctx); err != nil {
			return nil, internalServerError(errors.Wrap(err, "unable to flush"))
		}

	default:
		return nil, internalServerError(err)
	}

	// upgrade to exclusive lock to ensure we have source manager
	s.mu.RUnlock()
	s.mu.Lock()
	if s.sourceManagers[sourceInfo] == nil {
		log(ctx).Debugf("creating source manager for %v", sourceInfo)
		sm := newSourceManager(sourceInfo, s)
		s.sourceManagers[sourceInfo] = sm

		go sm.run(ctxutil.Detach(ctx))
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
