package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/clock"
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

func getSnapshotSourceFromURL(u *url.URL) snapshot.SourceInfo {
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
	pol, err := policy.GetDefinedPolicy(ctx, s.rep, getSnapshotSourceFromURL(r.URL))
	if errors.Is(err, policy.ErrPolicyNotFound) {
		return nil, requestError(serverapi.ErrorNotFound, "policy not found")
	}

	return pol, nil
}

func (s *Server) handlePolicyResolve(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	var req serverapi.ResolvePolicyRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "unable to decode request: "+err.Error())
	}

	target := getSnapshotSourceFromURL(r.URL)

	// build a list of parents
	policies, err := policy.GetPolicyHierarchy(ctx, s.rep, target, nil)
	if err != nil {
		return nil, internalServerError(err)
	}

	resp := &serverapi.ResolvePolicyResponse{
		Defined: policies[0],
	}

	if req.Updates != nil {
		policies[0] = req.Updates
		policies[0].Labels = policy.LabelsForSource(target)
	}

	resp.Effective, resp.Definition = policy.MergePolicies(policies, target)
	resp.UpcomingSnapshotTimes = []time.Time{}

	now := clock.Now().Local()

	for i := 0; i < req.NumUpcomingSnapshotTimes; i++ {
		st, ok := resp.Effective.SchedulingPolicy.NextSnapshotTime(now, now)
		if !ok {
			break
		}

		resp.UpcomingSnapshotTimes = append(resp.UpcomingSnapshotTimes, st)
		now = st.Add(1 * time.Second)
	}

	return resp, nil
}

func (s *Server) handlePolicyDelete(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	if _, ok := s.rep.(repo.RepositoryWriter); !ok {
		return nil, repositoryNotWritableError()
	}

	sourceInfo := getSnapshotSourceFromURL(r.URL)

	if err := repo.WriteSession(ctx, s.rep, repo.WriteSessionOptions{
		Purpose: "PolicyDelete",
	}, func(ctx context.Context, w repo.RepositoryWriter) error {
		return errors.Wrap(policy.RemovePolicy(ctx, w, sourceInfo), "unable to delete policy")
	}); err != nil {
		return nil, internalServerError(err)
	}

	s.triggerRefreshSource(sourceInfo)

	return &serverapi.Empty{}, nil
}

func (s *Server) handlePolicyPut(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	newPolicy := &policy.Policy{}
	if err := json.Unmarshal(body, newPolicy); err != nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "malformed request body")
	}

	if _, ok := s.rep.(repo.RepositoryWriter); !ok {
		return nil, repositoryNotWritableError()
	}

	sourceInfo := getSnapshotSourceFromURL(r.URL)

	if err := repo.WriteSession(ctx, s.rep, repo.WriteSessionOptions{
		Purpose: "PolicyPut",
	}, func(ctx context.Context, w repo.RepositoryWriter) error {
		return errors.Wrap(policy.SetPolicy(ctx, w, sourceInfo, newPolicy), "unable to set policy")
	}); err != nil {
		return nil, internalServerError(err)
	}

	s.triggerRefreshSource(sourceInfo)

	return &serverapi.Empty{}, nil
}
