package server

import (
	"context"
	"net/http"
	"net/url"

	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

func (s *Server) handleSnapshotList(ctx context.Context, r *http.Request) (interface{}, *apiError) {
	manifestIDs, err := snapshot.ListSnapshotManifests(ctx, s.rep, nil)
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
