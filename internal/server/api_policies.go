package server

import (
	"context"
	"encoding/json"
	"net/url"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

func handlePolicyList(ctx context.Context, rc requestContext) (interface{}, *apiError) {
	policies, err := policy.ListPolicies(ctx, rc.rep)
	if err != nil {
		return nil, internalServerError(err)
	}

	resp := &serverapi.PoliciesResponse{
		Policies: []*serverapi.PolicyListEntry{},
	}

	for _, pol := range policies {
		target := pol.Target()
		if !sourceMatchesURLFilter(target, rc.req.URL.Query()) {
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

func handlePolicyGet(ctx context.Context, rc requestContext) (interface{}, *apiError) {
	pol, err := policy.GetDefinedPolicy(ctx, rc.rep, getSnapshotSourceFromURL(rc.req.URL))
	if errors.Is(err, policy.ErrPolicyNotFound) {
		return nil, requestError(serverapi.ErrorNotFound, "policy not found")
	}

	return pol, nil
}

func handlePolicyResolve(ctx context.Context, rc requestContext) (interface{}, *apiError) {
	var req serverapi.ResolvePolicyRequest

	if err := json.Unmarshal(rc.body, &req); err != nil {
		return nil, unableToDecodeRequest(err)
	}

	target := getSnapshotSourceFromURL(rc.req.URL)

	// build a list of parents
	policies, err := policy.GetPolicyHierarchy(ctx, rc.rep, target, nil)
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

	if err := policy.ValidateSchedulingPolicy(policies[0].SchedulingPolicy); err != nil {
		resp.SchedulingError = err.Error()
	}

	now := clock.Now().Local()

	for range req.NumUpcomingSnapshotTimes {
		st, ok := resp.Effective.SchedulingPolicy.NextSnapshotTime(now, now)
		if !ok {
			break
		}

		resp.UpcomingSnapshotTimes = append(resp.UpcomingSnapshotTimes, st)
		now = st.Add(1 * time.Second)
	}

	return resp, nil
}

func handlePolicyDelete(ctx context.Context, rc requestContext) (interface{}, *apiError) {
	if _, ok := rc.rep.(repo.RepositoryWriter); !ok {
		return nil, repositoryNotWritableError()
	}

	sourceInfo := getSnapshotSourceFromURL(rc.req.URL)

	if err := repo.WriteSession(ctx, rc.rep, repo.WriteSessionOptions{
		Purpose: "PolicyDelete",
	}, func(ctx context.Context, w repo.RepositoryWriter) error {
		return errors.Wrap(policy.RemovePolicy(ctx, w, sourceInfo), "unable to delete policy")
	}); err != nil {
		return nil, internalServerError(err)
	}

	rc.srv.Refresh()

	return &serverapi.Empty{}, nil
}

func handlePolicyPut(ctx context.Context, rc requestContext) (interface{}, *apiError) {
	newPolicy := &policy.Policy{}
	if err := json.Unmarshal(rc.body, newPolicy); err != nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "malformed request body")
	}

	if _, ok := rc.rep.(repo.RepositoryWriter); !ok {
		return nil, repositoryNotWritableError()
	}

	sourceInfo := getSnapshotSourceFromURL(rc.req.URL)

	if err := repo.WriteSession(ctx, rc.rep, repo.WriteSessionOptions{
		Purpose: "PolicyPut",
	}, func(ctx context.Context, w repo.RepositoryWriter) error {
		return errors.Wrap(policy.SetPolicy(ctx, w, sourceInfo, newPolicy), "unable to set policy")
	}); err != nil {
		return nil, internalServerError(err)
	}

	rc.srv.Refresh()

	return &serverapi.Empty{}, nil
}
