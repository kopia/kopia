package server

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/auth"
	"github.com/kopia/kopia/internal/remoterepoapi"
	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

func handleManifestGet(ctx context.Context, rc requestContext) (interface{}, *apiError) {
	mid := manifest.ID(rc.muxVar("manifestID"))

	var data json.RawMessage

	md, err := rc.rep.GetManifest(ctx, mid, &data)
	if errors.Is(err, manifest.ErrNotFound) {
		return nil, notFoundError("manifest not found")
	}

	if err != nil {
		return nil, internalServerError(err)
	}

	if !hasManifestAccess(ctx, rc, md.Labels, auth.AccessLevelRead) {
		return nil, accessDeniedError()
	}

	return &remoterepoapi.ManifestWithMetadata{
		Payload:  data,
		Metadata: md,
	}, nil
}

func handleManifestDelete(ctx context.Context, rc requestContext) (interface{}, *apiError) {
	rw, ok := rc.rep.(repo.RepositoryWriter)
	if !ok {
		return nil, repositoryNotWritableError()
	}

	mid := manifest.ID(rc.muxVar("manifestID"))

	var data json.RawMessage

	em, err := rc.rep.GetManifest(ctx, mid, &data)
	if errors.Is(err, manifest.ErrNotFound) {
		return nil, notFoundError("manifest not found")
	}

	if err != nil {
		return nil, internalServerError(err)
	}

	if !hasManifestAccess(ctx, rc, em.Labels, auth.AccessLevelFull) {
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

func handleManifestList(ctx context.Context, rc requestContext) (interface{}, *apiError) {
	// password already validated by a wrapper, no need to check here.
	labels := map[string]string{}

	for k, v := range rc.req.URL.Query() {
		labels[k] = v[0]
	}

	m, err := rc.rep.FindManifests(ctx, labels)
	if err != nil {
		return nil, internalServerError(err)
	}

	return filterManifests(m, httpAuthorizationInfo(ctx, rc)), nil
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

func handleManifestCreate(ctx context.Context, rc requestContext) (interface{}, *apiError) {
	rw, ok := rc.rep.(repo.RepositoryWriter)
	if !ok {
		return nil, repositoryNotWritableError()
	}

	var req remoterepoapi.ManifestWithMetadata

	if err := json.Unmarshal(rc.body, &req); err != nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "malformed request")
	}

	if !hasManifestAccess(ctx, rc, req.Metadata.Labels, auth.AccessLevelAppend) {
		return nil, accessDeniedError()
	}

	id, err := rw.PutManifest(ctx, req.Metadata.Labels, req.Payload)
	if err != nil {
		return nil, internalServerError(err)
	}

	return &manifest.EntryMetadata{ID: id}, nil
}

func handleApplyRetentionPolicy(ctx context.Context, rc requestContext) (interface{}, *apiError) {
	rw, ok := rc.rep.(repo.RepositoryWriter)
	if !ok {
		return nil, repositoryNotWritableError()
	}

	var req remoterepoapi.ApplyRetentionPolicyRequest

	if err := json.Unmarshal(rc.body, &req); err != nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "malformed request")
	}

	usernameAtHostname, _, _ := rc.req.BasicAuth()

	parts := strings.Split(usernameAtHostname, "@")
	if len(parts) != 2 { //nolint:gomnd
		return nil, requestError(serverapi.ErrorMalformedRequest, "malformed username")
	}

	// only allow users to apply retention policy if they have permission to add snapshots
	// for a particular path.
	if !hasManifestAccess(ctx, rc, map[string]string{
		manifest.TypeLabelKey:  snapshot.ManifestType,
		snapshot.UsernameLabel: parts[0],
		snapshot.HostnameLabel: parts[1],
		snapshot.PathLabel:     req.SourcePath,
	}, auth.AccessLevelAppend) {
		return nil, accessDeniedError()
	}

	ids, err := policy.ApplyRetentionPolicy(ctx, rw, snapshot.SourceInfo{
		UserName: parts[0],
		Host:     parts[1],
		Path:     req.SourcePath,
	}, req.ReallyDelete)
	if err != nil {
		return nil, internalServerError(err)
	}

	return &remoterepoapi.ApplyRetentionPolicyResponse{
		ManifestIDs: ids,
	}, nil
}
