package server

import (
	"net/http"
	"sort"

	"github.com/kopia/kopia/snapshot"
)

type sourcesListResponse struct {
	Sources []snapshot.SourceInfo `json:"sources"`
}

func (s *Server) handleSourcesList(r *http.Request) (interface{}, *apiError) {
	mgr := snapshot.NewManager(s.rep)

	resp := &sourcesListResponse{
		Sources: mgr.ListSources(),
	}

	sort.Slice(resp.Sources, func(i, j int) bool {
		return resp.Sources[i].String() < resp.Sources[j].String()
	})

	return resp, nil
}
