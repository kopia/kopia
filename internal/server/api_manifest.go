package server

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/auth"
	"github.com/kopia/kopia/internal/remoterepoapi"
	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
)

func (s *Server) handleManifestGet(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	mid := manifest.ID(mux.Vars(r)["manifestID"])

	var data json.RawMessage

	md, err := s.rep.GetManifest(ctx, mid, &data)
	if errors.Is(err, manifest.ErrNotFound) {
		return nil, notFoundError("manifest not found")
	}

	if err != nil {
		return nil, internalServerError(err)
	}

	if !hasManifestAccess(s, r, md.Labels, auth.AccessLevelRead) {
		return nil, accessDeniedError()
	}

	return &remoterepoapi.ManifestWithMetadata{
		Payload:  data,
		Metadata: md,
	}, nil
}

func (s *Server) handleManifestDelete(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	rw, ok := s.rep.(repo.RepositoryWriter)
	if !ok {
		return nil, repositoryNotWritableError()
	}

	mid := manifest.ID(mux.Vars(r)["manifestID"])

	var data json.RawMessage

	em, err := s.rep.GetManifest(ctx, mid, &data)
	if errors.Is(err, manifest.ErrNotFound) {
		return nil, notFoundError("manifest not found")
	}

	if err != nil {
		return nil, internalServerError(err)
	}

	if !hasManifestAccess(s, r, em.Labels, auth.AccessLevelFull) {
		return nil, accessDeniedError()
	}

	err = rw.DeleteManifest(ctx, mid)
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
	labels := map[string]string{}

	for k, v := range r.URL.Query() {
		labels[k] = v[0]
	}

	m, err := s.rep.FindManifests(ctx, labels)
	if err != nil {
		return nil, internalServerError(err)
	}

	return filterManifests(m, s.httpAuthorizationInfo(r)), nil
}

func filterManifests(manifests []*manifest.EntryMetadata, authz auth.AuthorizationInfo) []*manifest.EntryMetadata {
	result := []*manifest.EntryMetadata{}

	for _, m := range manifests {
		if authz.ManifestAccessLevel(m.Labels) >= auth.AccessLevelRead {
			result = append(result, m)
		}
	}

	return result
}

func (s *Server) handleManifestCreate(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	rw, ok := s.rep.(repo.RepositoryWriter)
	if !ok {
		return nil, repositoryNotWritableError()
	}

	var req remoterepoapi.ManifestWithMetadata

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "malformed request")
	}

	if !hasManifestAccess(s, r, req.Metadata.Labels, auth.AccessLevelAppend) {
		return nil, accessDeniedError()
	}

	id, err := rw.PutManifest(ctx, req.Metadata.Labels, req.Payload)
	if err != nil {
		return nil, internalServerError(err)
	}

	return &manifest.EntryMetadata{ID: id}, nil
}
