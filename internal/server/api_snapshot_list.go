package server

import (
	"net/http"
	"time"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/snapshot"
)

type snapshotListEntry struct {
	ID               string               `json:"id"`
	Source           snapshot.SourceInfo  `json:"source"`
	Description      string               `json:"description"`
	StartTime        time.Time            `json:"startTime"`
	EndTime          time.Time            `json:"endTime"`
	IncompleteReason string               `json:"incomplete,omitempty"`
	Summary          *fs.DirectorySummary `json:"summary"`
	RootEntry        string               `json:"rootID"`
	RetentionReasons []string             `json:"retention"`
}

type snapshotListResponse struct {
	Snapshots []*snapshotListEntry `json:"snapshots"`
}

func (s *Server) handleSourceSnapshotList(r *http.Request) (interface{}, *apiError) {
	mgr := snapshot.NewManager(s.rep)
	pmgr := snapshot.NewPolicyManager(s.rep)

	manifestIDs := mgr.ListSnapshotManifests(nil)
	manifests, err := mgr.LoadSnapshots(manifestIDs)
	if err != nil {
		return nil, internalServerError(err)
	}

	resp := &snapshotListResponse{}

	groups := snapshot.GroupBySource(manifests)

	for _, grp := range groups {
		pol, err := pmgr.GetEffectivePolicy(grp[0].Source)
		if err == nil {
			pol.RetentionPolicy.ComputeRetentionReasons(grp)
		}
		for _, m := range grp {
			resp.Snapshots = append(resp.Snapshots, convertSnapshotManifest(m))
		}
	}

	return resp, nil
}

func convertSnapshotManifest(m *snapshot.Manifest) *snapshotListEntry {
	return &snapshotListEntry{
		ID:               m.ID,
		Source:           m.Source,
		Description:      m.Description,
		StartTime:        m.StartTime,
		EndTime:          m.EndTime,
		IncompleteReason: m.IncompleteReason,
		RootEntry:        m.RootObjectID().String(),
		Summary:          m.RootEntry.DirSummary,
		RetentionReasons: m.RetentionReasons,
	}
}
