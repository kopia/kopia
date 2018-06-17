package server

import (
	"net/http"

	"github.com/kopia/kopia/internal/serverapi"
)

func (s *Server) handlePolicyList(r *http.Request) (interface{}, *apiError) {
	policies, err := s.policyManager.ListPolicies()
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
