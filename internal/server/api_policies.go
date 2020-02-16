package server

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

func (s *Server) handlePolicyList(ctx context.Context, r *http.Request) (interface{}, *apiError) {
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

func (s *Server) handlePolicyCRUD(ctx context.Context, r *http.Request) (interface{}, *apiError) {
	host := r.URL.Query().Get("host")
	path := r.URL.Query().Get("path")
	username := r.URL.Query().Get("userName")
	target := snapshot.SourceInfo{
		Host:     host,
		Path:     path,
		UserName: username,
	}

	switch r.Method {
	case "GET":
		pol, err := policy.GetDefinedPolicy(ctx, s.rep, target)
		if err == policy.ErrPolicyNotFound {
			return nil, requestError(serverapi.ErrorNotFound, "policy not found")
		}

		return pol, nil

	case "DELETE":
		if err := policy.RemovePolicy(ctx, s.rep, target); err != nil {
			return nil, internalServerError(err)
		}

		return &serverapi.Empty{}, nil

	case "PUT":
		newPolicy := &policy.Policy{}
		if err := json.NewDecoder(r.Body).Decode(newPolicy); err != nil {
			return nil, requestError(serverapi.ErrorMalformedRequest, "malformed request body")
		}

		if err := policy.SetPolicy(ctx, s.rep, target, newPolicy); err != nil {
			return nil, internalServerError(err)
		}

		return &serverapi.Empty{}, nil

	default:
		return nil, requestError(serverapi.ErrorMalformedRequest, "incompatible HTTP method")
	}
}
