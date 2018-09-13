package server

import (
	"context"
	"net/http"
	"sort"

	"github.com/kopia/kopia/internal/serverapi"
)

func (s *Server) handleSourcesList(ctx context.Context, r *http.Request) (interface{}, *apiError) {
	resp := &serverapi.SourcesResponse{
		Sources: []serverapi.SourceStatus{},
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
