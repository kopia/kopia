package serverapi

import (
	"context"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
)

// CreateSnapshotSource creates snapshot source with a given path.
func CreateSnapshotSource(ctx context.Context, c *apiclient.KopiaAPIClient, req *CreateSnapshotSourceRequest) (*CreateSnapshotSourceResponse, error) {
	resp := &CreateSnapshotSourceResponse{}
	if err := c.Post(ctx, "sources", req, resp); err != nil {
		return nil, errors.Wrap(err, "CreateSnapshotSource")
	}

	return resp, nil
}

// UploadSnapshots triggers snapshot upload on matching snapshots.
func UploadSnapshots(ctx context.Context, c *apiclient.KopiaAPIClient, match *snapshot.SourceInfo) (*MultipleSourceActionResponse, error) {
	resp := &MultipleSourceActionResponse{}
	if err := c.Post(ctx, "sources/upload"+matchSourceParameters(match), &Empty{}, resp); err != nil {
		return nil, errors.Wrap(err, "UploadSnapshots")
	}

	return resp, nil
}

// CancelUpload cancels snapshot upload on matching snapshots.
func CancelUpload(ctx context.Context, c *apiclient.KopiaAPIClient, match *snapshot.SourceInfo) (*MultipleSourceActionResponse, error) {
	resp := &MultipleSourceActionResponse{}
	if err := c.Post(ctx, "sources/cancel"+matchSourceParameters(match), &Empty{}, resp); err != nil {
		return nil, errors.Wrap(err, "CancelUpload")
	}

	return resp, nil
}

// CreateRepository invokes the 'repo/create' API.
func CreateRepository(ctx context.Context, c *apiclient.KopiaAPIClient, req *CreateRepositoryRequest) error {
	return c.Post(ctx, "repo/create", req, &StatusResponse{})
}

// ConnectToRepository invokes the 'repo/connect' API.
func ConnectToRepository(ctx context.Context, c *apiclient.KopiaAPIClient, req *ConnectRepositoryRequest) error {
	return c.Post(ctx, "repo/connect", req, &StatusResponse{})
}

// DisconnectFromRepository invokes the 'repo/disconnect' API.
func DisconnectFromRepository(ctx context.Context, c *apiclient.KopiaAPIClient) error {
	return c.Post(ctx, "repo/disconnect", &Empty{}, &Empty{})
}

// Shutdown invokes the 'repo/shutdown' API.
func Shutdown(ctx context.Context, c *apiclient.KopiaAPIClient) {
	_ = c.Post(ctx, "shutdown", &Empty{}, &Empty{})
}

// Status invokes the 'repo/status' API.
func Status(ctx context.Context, c *apiclient.KopiaAPIClient) (*StatusResponse, error) {
	resp := &StatusResponse{}
	if err := c.Get(ctx, "repo/status", nil, resp); err != nil {
		return nil, errors.Wrap(err, "Status")
	}

	return resp, nil
}

// ListSources lists the snapshot sources managed by the server.
func ListSources(ctx context.Context, c *apiclient.KopiaAPIClient, match *snapshot.SourceInfo) (*SourcesResponse, error) {
	resp := &SourcesResponse{}
	if err := c.Get(ctx, "sources"+matchSourceParameters(match), nil, resp); err != nil {
		return nil, errors.Wrap(err, "ListSources")
	}

	return resp, nil
}

// ListSnapshots lists the snapshots managed by the server for a given source filter.
func ListSnapshots(ctx context.Context, c *apiclient.KopiaAPIClient, match *snapshot.SourceInfo) (*SnapshotsResponse, error) {
	resp := &SnapshotsResponse{}
	if err := c.Get(ctx, "snapshots"+matchSourceParameters(match), nil, resp); err != nil {
		return nil, errors.Wrap(err, "ListSnapshots")
	}

	return resp, nil
}

// ListPolicies lists the policies managed by the server for a given target filter.
func ListPolicies(ctx context.Context, c *apiclient.KopiaAPIClient, match *snapshot.SourceInfo) (*PoliciesResponse, error) {
	resp := &PoliciesResponse{}
	if err := c.Get(ctx, "policies"+matchSourceParameters(match), nil, resp); err != nil {
		return nil, errors.Wrap(err, "ListPolicies")
	}

	return resp, nil
}

// GetObject returns the object payload.
func GetObject(ctx context.Context, c *apiclient.KopiaAPIClient, objectID string) ([]byte, error) {
	var b []byte

	if err := c.Get(ctx, "objects/"+objectID, object.ErrObjectNotFound, &b); err != nil {
		return nil, errors.Wrap(err, "GetObject")
	}

	return b, nil
}

func matchSourceParameters(match *snapshot.SourceInfo) string {
	if match == nil {
		return ""
	}

	var clauses []string
	if v := match.Host; v != "" {
		clauses = append(clauses, "host="+v)
	}

	if v := match.UserName; v != "" {
		clauses = append(clauses, "username="+v)
	}

	if v := match.Path; v != "" {
		clauses = append(clauses, "path="+v)
	}

	if len(clauses) == 0 {
		return ""
	}

	return "?" + strings.Join(clauses, "&")
}
