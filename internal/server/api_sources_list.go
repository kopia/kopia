package server

import (
	"net/http"
	"sort"
)

type sourcesListResponse struct {
	Sources []sourceStatus `json:"sources"`
}

func (s *Server) handleSourcesList(r *http.Request) (interface{}, *apiError) {
	resp := &sourcesListResponse{
		Sources: []sourceStatus{},
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
