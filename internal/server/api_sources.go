package server

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"sort"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

func (s *Server) handleSourcesList(ctx context.Context, r *http.Request) (interface{}, *apiError) {
	resp := &serverapi.SourcesResponse{
		Sources: []*serverapi.SourceStatus{},
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
		UserName: s.options.Username,
		Host:     s.options.Hostname,
		Path:     req.Path,
	}

	// ensure we have the policy for this source, otherwise it will not show up in the
	// list of sources at all.
	_, err = policy.GetDefinedPolicy(ctx, s.rep, sourceInfo)
	switch err {
	case nil:
		log.Debugf("policy for %v already exists", sourceInfo)
		// have policy, do nothing

	case policy.ErrPolicyNotFound:
		// don't have policy - create an empty one
		log.Debugf("policy for %v not found, creating empty one", sourceInfo)

		if err = policy.SetPolicy(ctx, s.rep, sourceInfo, &policy.Policy{}); err != nil {
			return nil, internalServerError(errors.Wrap(err, "unable to set policy"))
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
		log.Debugf("creating source manager for %v", sourceInfo)
		sm := newSourceManager(sourceInfo, s)
		s.sourceManagers[sourceInfo] = sm

		go sm.run(context.Background())
	}
	s.mu.Unlock()
	s.mu.RLock()

	manager := s.sourceManagers[sourceInfo]
	if manager == nil {
		return nil, internalServerError(errors.Errorf("could not find source manager that was just created"))
	}

	if req.CreateSnapshot {
		log.Debugf("scheduling snapshot of %v immediately...", sourceInfo)
		manager.scheduleSnapshotNow()
	}

	return &serverapi.Empty{}, nil
}
