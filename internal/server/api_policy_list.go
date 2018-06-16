package server

import (
	"net/http"

	"github.com/kopia/kopia/snapshot"
)

type policyListEntry struct {
	ID     string              `json:"id"`
	Target snapshot.SourceInfo `json:"target"`
	Policy *snapshot.Policy    `json:"policy"`
}

type policyListResponse struct {
	Policies []*policyListEntry `json:"policies"`
}

func (s *Server) handlePolicyList(r *http.Request) (interface{}, *apiError) {
	policies, err := s.policyManager.ListPolicies()
	if err != nil {
		return nil, internalServerError(err)
	}

	resp := &policyListResponse{
		Policies: []*policyListEntry{},
	}

	for _, pol := range policies {
		target := pol.Target()
		if !sourceMatchesURLFilter(target, r.URL.Query()) {
			continue
		}
		resp.Policies = append(resp.Policies, &policyListEntry{
			ID:     pol.ID(),
			Target: target,
			Policy: pol,
		})
	}

	return resp, nil
}
