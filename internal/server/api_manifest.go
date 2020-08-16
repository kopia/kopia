package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/kopia/kopia/internal/remoterepoapi"
	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/repo/manifest"
)

func (s *Server) handleManifestGet(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	// password already validated by a wrapper, no need to check here.
	userAtHost, _, _ := r.BasicAuth()

	mid := manifest.ID(mux.Vars(r)["manifestID"])

	var data json.RawMessage

	md, err := s.rep.GetManifest(ctx, mid, &data)
	if errors.Is(err, manifest.ErrNotFound) {
		return nil, notFoundError("manifest not found")
	}

	if err != nil {
		return nil, internalServerError(err)
	}

	if !manifestMatchesUser(md, userAtHost) {
		return nil, notFoundError("manifest not found")
	}

	return &remoterepoapi.ManifestWithMetadata{
		Payload:  data,
		Metadata: md,
	}, nil
}

func (s *Server) handleManifestDelete(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	mid := manifest.ID(mux.Vars(r)["manifestID"])

	err := s.rep.DeleteManifest(ctx, mid)
	if errors.Is(err, manifest.ErrNotFound) {
		return nil, notFoundError("manifest not found")
	}

	if err != nil {
		return nil, internalServerError(err)
	}

	return &serverapi.Empty{}, nil
}

func (s *Server) handleManifestList(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	// password already validated by a wrapper, no need to check here.
	userAtHost, _, _ := r.BasicAuth()

	labels := map[string]string{}

	for k, v := range r.URL.Query() {
		labels[k] = v[0]
	}

	m, err := s.rep.FindManifests(ctx, labels)
	if err != nil {
		return nil, internalServerError(err)
	}

	return filterManifests(m, userAtHost), nil
}

func manifestMatchesUser(m *manifest.EntryMetadata, userAtHost string) bool {
	if userAtHost == "" {
		return true
	}

	actualUser := m.Labels["username"] + "@" + m.Labels["hostname"]

	return actualUser == userAtHost
}

func filterManifests(manifests []*manifest.EntryMetadata, userAtHost string) []*manifest.EntryMetadata {
	result := []*manifest.EntryMetadata{}

	for _, m := range manifests {
		if manifestMatchesUser(m, userAtHost) {
			result = append(result, m)
		}
	}

	return result
}

func (s *Server) handleManifestCreate(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	var req remoterepoapi.ManifestWithMetadata

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "malformed request")
	}

	id, err := s.rep.PutManifest(ctx, req.Metadata.Labels, req.Payload)
	if err != nil {
		return nil, internalServerError(err)
	}

	return &manifest.EntryMetadata{ID: id}, nil
}
