package server

import (
	"net/http"

	"github.com/kopia/kopia/snapshot"
)

type policyListEntry struct {
	ID     string              `json:"id"`
	Source snapshot.SourceInfo `json:"source"`
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
		src := pol.Source()
		if !sourceMatchesURLFilter(src, r.URL.Query()) {
			continue
		}
		resp.Policies = append(resp.Policies, &policyListEntry{
			ID:     pol.ID(),
			Source: src,
			Policy: pol,
		})
	}

	return resp, nil
}
