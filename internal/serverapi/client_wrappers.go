package serverapi

import "context"

// CreateRepository invokes the 'repo/create' API.
func (c *Client) CreateRepository(ctx context.Context, req *CreateRequest) error {
	return c.Post("repo/create", req, &StatusResponse{})
}

// ConnectToRepository invokes the 'repo/connect' API.
func (c *Client) ConnectToRepository(ctx context.Context, req *ConnectRequest) error {
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

// Sources invokes the 'sources' API.
func (c *Client) Sources(ctx context.Context) (*SourcesResponse, error) {
	resp := &SourcesResponse{}
	if err := c.Get("sources", resp); err != nil {
		return nil, err
	}

	return resp, nil
}
