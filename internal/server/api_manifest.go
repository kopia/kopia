package server

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/kopia/kopia/internal/remoterepoapi"
	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/repo/manifest"
)

func (s *Server) handleManifestGet(ctx context.Context, r *http.Request) (interface{}, *apiError) {
	mid := manifest.ID(mux.Vars(r)["manifestID"])

	var data json.RawMessage

	md, err := s.rep.GetManifest(ctx, mid, &data)
	if err == manifest.ErrNotFound {
		return nil, notFoundError("manifest not found")
	}

	if err != nil {
		return nil, internalServerError(err)
	}

	return &remoterepoapi.ManifestWithMetadata{
		Payload:  data,
		Metadata: md,
	}, nil
}

func (s *Server) handleManifestDelete(ctx context.Context, r *http.Request) (interface{}, *apiError) {
	mid := manifest.ID(mux.Vars(r)["manifestID"])

	err := s.rep.DeleteManifest(ctx, mid)
	if err == manifest.ErrNotFound {
		return nil, notFoundError("manifest not found")
	}

	if err != nil {
		return nil, internalServerError(err)
	}

	return &serverapi.Empty{}, nil
}
func (s *Server) handleManifestList(ctx context.Context, r *http.Request) (interface{}, *apiError) {
	labels := map[string]string{}

	for k, v := range r.URL.Query() {
		labels[k] = v[0]
	}

	m, err := s.rep.FindManifests(ctx, labels)
	if err != nil {
		return nil, internalServerError(err)
	}

	if m == nil {
		m = []*manifest.EntryMetadata{}
	}

	return m, nil
}

func (s *Server) handleManifestCreate(ctx context.Context, r *http.Request) (interface{}, *apiError) {
	var req remoterepoapi.ManifestWithMetadata

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "malformed request")
	}

	id, err := s.rep.PutManifest(ctx, req.Metadata.Labels, req.Payload)
	if err != nil {
		return nil, internalServerError(err)
	}

	return &manifest.EntryMetadata{ID: id}, nil
}
