package serverapi

import (
	"context"
	"strings"

	"github.com/kopia/kopia/snapshot"
)

// CreateSnapshotSource creates snapshot source with a given path.
func (c *Client) CreateSnapshotSource(ctx context.Context, req *CreateSnapshotSourceRequest) error {
	return c.Post("sources", req, &CreateSnapshotSourceRequest{})
}

// UploadSnapshots triggers snapshot upload on matching snapshots.
func (c *Client) UploadSnapshots(ctx context.Context, match *snapshot.SourceInfo) (*MultipleSourceActionResponse, error) {
	resp := &MultipleSourceActionResponse{}
	if err := c.Post("sources/upload"+matchSourceParameters(match), &Empty{}, resp); err != nil {
		return nil, err
	}

	return resp, nil
}

// CancelUpload cancels snapshot upload on matching snapshots.
func (c *Client) CancelUpload(ctx context.Context, match *snapshot.SourceInfo) (*MultipleSourceActionResponse, error) {
	resp := &MultipleSourceActionResponse{}
	if err := c.Post("sources/cancel"+matchSourceParameters(match), &Empty{}, resp); err != nil {
		return nil, err
	}

	return resp, nil
}

// CreateRepository invokes the 'repo/create' API.
func (c *Client) CreateRepository(ctx context.Context, req *CreateRepositoryRequest) error {
	return c.Post("repo/create", req, &StatusResponse{})
}

// ConnectToRepository invokes the 'repo/connect' API.
func (c *Client) ConnectToRepository(ctx context.Context, req *ConnectRepositoryRequest) error {
	return c.Post("repo/connect", req, &StatusResponse{})
}

// DisconnectFromRepository invokes the 'repo/disconnect' API.
func (c *Client) DisconnectFromRepository(ctx context.Context) error {
	return c.Post("repo/disconnect", &Empty{}, &Empty{})
}

// Shutdown invokes the 'repo/shutdown' API.
func (c *Client) Shutdown(ctx context.Context) {
	_ = c.Post("shutdown", &Empty{}, &Empty{})
}

// Status invokes the 'repo/status' API.
func (c *Client) Status(ctx context.Context) (*StatusResponse, error) {
	resp := &StatusResponse{}
	if err := c.Get("repo/status", resp); err != nil {
		return nil, err
	}

	return resp, nil
}

// ListSources lists the snapshot sources managed by the server.
func (c *Client) ListSources(ctx context.Context, match *snapshot.SourceInfo) (*SourcesResponse, error) {
	resp := &SourcesResponse{}
	if err := c.Get("sources"+matchSourceParameters(match), resp); err != nil {
		return nil, err
	}

	return resp, nil
}

// ListSnapshots invokes the 'sources' API.
func (c *Client) ListSnapshots(ctx context.Context, match *snapshot.SourceInfo) (*SnapshotsResponse, error) {
	resp := &SnapshotsResponse{}
	if err := c.Get("snapshots"+matchSourceParameters(match), resp); err != nil {
		return nil, err
	}

	return resp, nil
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
