package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

func (s *Server) handlePolicyList(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	policies, err := policy.ListPolicies(ctx, s.rep)
	if err != nil {
		return nil, internalServerError(err)
	}

	resp := &serverapi.PoliciesResponse{
		Policies: []*serverapi.PolicyListEntry{},
	}

	for _, pol := range policies {
		target := pol.Target()
		if !sourceMatchesURLFilter(target, r.URL.Query()) {
			continue
		}

		resp.Policies = append(resp.Policies, &serverapi.PolicyListEntry{
			ID:     pol.ID(),
			Target: target,
			Policy: pol,
		})
	}

	return resp, nil
}

func getPolicyTargetFromURL(u *url.URL) snapshot.SourceInfo {
	host := u.Query().Get("host")
	path := u.Query().Get("path")
	username := u.Query().Get("userName")

	return snapshot.SourceInfo{
		Host:     host,
		Path:     path,
		UserName: username,
	}
}

func (s *Server) handlePolicyGet(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	pol, err := policy.GetDefinedPolicy(ctx, s.rep, getPolicyTargetFromURL(r.URL))
	if errors.Is(err, policy.ErrPolicyNotFound) {
		return nil, requestError(serverapi.ErrorNotFound, "policy not found")
	}

	return pol, nil
}

func (s *Server) handlePolicyDelete(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	w, ok := s.rep.(repo.RepositoryWriter)
	if !ok {
		return nil, repositoryNotWritableError()
	}

	if err := policy.RemovePolicy(ctx, w, getPolicyTargetFromURL(r.URL)); err != nil {
		return nil, internalServerError(err)
	}

	if err := w.Flush(ctx); err != nil {
		return nil, internalServerError(err)
	}

	return &serverapi.Empty{}, nil
}

func (s *Server) handlePolicyPut(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	newPolicy := &policy.Policy{}
	if err := json.Unmarshal(body, newPolicy); err != nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "malformed request body")
	}

	w, ok := s.rep.(repo.RepositoryWriter)
	if !ok {
		return nil, repositoryNotWritableError()
	}

	if err := policy.SetPolicy(ctx, w, getPolicyTargetFromURL(r.URL), newPolicy); err != nil {
		return nil, internalServerError(err)
	}

	if err := w.Flush(ctx); err != nil {
		return nil, internalServerError(err)
	}

	return &serverapi.Empty{}, nil
}
