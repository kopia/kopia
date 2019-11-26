package server

import (
	"context"
	"net/http"

	"github.com/kopia/kopia/internal/serverapi"
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
