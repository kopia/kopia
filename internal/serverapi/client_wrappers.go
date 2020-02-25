package serverapi

import (
	"context"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/snapshot"
)

// CreateSnapshotSource creates snapshot source with a given path.
func (c *Client) CreateSnapshotSource(ctx context.Context, req *CreateSnapshotSourceRequest) (*CreateSnapshotSourceResponse, error) {
	resp := &CreateSnapshotSourceResponse{}
	if err := c.Post(ctx, "sources", req, resp); err != nil {
		return nil, err
	}

	return resp, nil
}

// UploadSnapshots triggers snapshot upload on matching snapshots.
func (c *Client) UploadSnapshots(ctx context.Context, match *snapshot.SourceInfo) (*MultipleSourceActionResponse, error) {
	resp := &MultipleSourceActionResponse{}
	if err := c.Post(ctx, "sources/upload"+matchSourceParameters(match), &Empty{}, resp); err != nil {
		return nil, err
	}

	return resp, nil
}

// CancelUpload cancels snapshot upload on matching snapshots.
func (c *Client) CancelUpload(ctx context.Context, match *snapshot.SourceInfo) (*MultipleSourceActionResponse, error) {
	resp := &MultipleSourceActionResponse{}
	if err := c.Post(ctx, "sources/cancel"+matchSourceParameters(match), &Empty{}, resp); err != nil {
		return nil, err
	}

	return resp, nil
}

// CreateRepository invokes the 'repo/create' API.
func (c *Client) CreateRepository(ctx context.Context, req *CreateRepositoryRequest) error {
	return c.Post(ctx, "repo/create", req, &StatusResponse{})
}

// ConnectToRepository invokes the 'repo/connect' API.
func (c *Client) ConnectToRepository(ctx context.Context, req *ConnectRepositoryRequest) error {
	return c.Post(ctx, "repo/connect", req, &StatusResponse{})
}

// DisconnectFromRepository invokes the 'repo/disconnect' API.
func (c *Client) DisconnectFromRepository(ctx context.Context) error {
	return c.Post(ctx, "repo/disconnect", &Empty{}, &Empty{})
}

// Shutdown invokes the 'repo/shutdown' API.
func (c *Client) Shutdown(ctx context.Context) {
	_ = c.Post(ctx, "shutdown", &Empty{}, &Empty{})
}

// Status invokes the 'repo/status' API.
func (c *Client) Status(ctx context.Context) (*StatusResponse, error) {
	resp := &StatusResponse{}
	if err := c.Get(ctx, "repo/status", resp); err != nil {
		return nil, err
	}

	return resp, nil
}

// ListSources lists the snapshot sources managed by the server.
func (c *Client) ListSources(ctx context.Context, match *snapshot.SourceInfo) (*SourcesResponse, error) {
	resp := &SourcesResponse{}
	if err := c.Get(ctx, "sources"+matchSourceParameters(match), resp); err != nil {
		return nil, err
	}

	return resp, nil
}

// ListSnapshots lists the snapshots managed by the server for a given source filter.
func (c *Client) ListSnapshots(ctx context.Context, match *snapshot.SourceInfo) (*SnapshotsResponse, error) {
	resp := &SnapshotsResponse{}
	if err := c.Get(ctx, "snapshots"+matchSourceParameters(match), resp); err != nil {
		return nil, err
	}

	return resp, nil
}

// ListPolicies lists the policies managed by the server for a given target filter.
func (c *Client) ListPolicies(ctx context.Context, match *snapshot.SourceInfo) (*PoliciesResponse, error) {
	resp := &PoliciesResponse{}
	if err := c.Get(ctx, "policies"+matchSourceParameters(match), resp); err != nil {
		return nil, err
	}

	return resp, nil
}

// GetObject returns the object payload.
func (c *Client) GetObject(ctx context.Context, objectID string) ([]byte, error) {
	req, err := http.NewRequest("GET", c.options.BaseURL+"objects/"+objectID, nil)
	if err != nil {
		return nil, err
	}

	if c.options.LogRequests {
		log(ctx).Debugf("GET %v", req.URL)
	}

	if c.options.Username != "" {
		req.SetBasicAuth(c.options.Username, c.options.Password)
	}

	resp, err := c.options.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("invalid server response: %v", resp.Status)
	}

	return ioutil.ReadAll(resp.Body)
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
