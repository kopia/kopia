package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

func (s *Server) handleSnapshotList(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	manifestIDs, err := snapshot.ListSnapshotManifests(ctx, s.rep, nil, nil)
	if err != nil {
		return nil, internalServerError(err)
	}

	manifests, err := snapshot.LoadSnapshots(ctx, s.rep, manifestIDs)
	if err != nil {
		return nil, internalServerError(err)
	}

	resp := &serverapi.SnapshotsResponse{
		Snapshots: []*serverapi.Snapshot{},
	}

	groups := snapshot.GroupBySource(manifests)
	for _, grp := range groups {
		first := grp[0]
		if !sourceMatchesURLFilter(first.Source, r.URL.Query()) {
			continue
		}

		pol, _, err := policy.GetEffectivePolicy(ctx, s.rep, first.Source)
		if err == nil {
			pol.RetentionPolicy.ComputeRetentionReasons(grp)
		}

		for _, m := range grp {
			resp.Snapshots = append(resp.Snapshots, convertSnapshotManifest(m))
		}
	}

	return resp, nil
}

func sourceMatchesURLFilter(src snapshot.SourceInfo, query url.Values) bool {
	if v := query.Get("host"); v != "" && src.Host != v {
		return false
	}

	if v := query.Get("userName"); v != "" && src.UserName != v {
		return false
	}

	if v := query.Get("path"); v != "" && src.Path != v {
		return false
	}

	return true
}

func convertSnapshotManifest(m *snapshot.Manifest) *serverapi.Snapshot {
	e := &serverapi.Snapshot{
		ID:               m.ID,
		Source:           m.Source,
		Description:      m.Description,
		StartTime:        m.StartTime,
		EndTime:          m.EndTime,
		IncompleteReason: m.IncompleteReason,
		RootEntry:        m.RootObjectID().String(),
		RetentionReasons: m.RetentionReasons,
	}

	if re := m.RootEntry; re != nil {
		e.Summary = re.DirSummary
	}

	return e
}

func (s *Server) handleSnapshotDelete(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	mid := mux.Vars(r)["snapshotID"]
	rep := s.rep

	// logic here should eventually be similar to that in commandSnapshotDelete run
	// for now just assume the oid is a manifest ID and handle simple not found errors
	_, err := snapshot.LoadSnapshot(ctx, rep, manifest.ID(mid))
	if err != nil {
		log(ctx).Errorf("error loading snapshot %v", mid)
		return nil, requestError(serverapi.ErrorNotFound, "snapshot ID not found")
	}

	log(ctx).Debugw("would delete snapshot with id", "id", mid)

	return nil, internalServerError(errors.Errorf("deletion not implemented"))
}

func (s *Server) handleSnapshotDeleteBatch(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	var req serverapi.BatchSnapshotDeleteRequest

	rep := s.rep

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "malformed request body")
	}

	if len(req.SnapshotManifestIds) == 0 {
		return nil, requestError(serverapi.ErrorMalformedRequest, "missing list of snapshots")
	}

	// make sure all manifest ids are valid
	for _, id := range req.SnapshotManifestIds {
		_, err := snapshot.LoadSnapshot(ctx, rep, manifest.ID(id))
		if err != nil {
			log(ctx).Errorf("error loading snapshot %v", id)
			return nil, requestError(serverapi.ErrorMalformedRequest, "invalid manifest id in snapshotManifestIds")
		}
	}

	return nil, internalServerError(errors.Errorf("deletion not implemented"))
}
