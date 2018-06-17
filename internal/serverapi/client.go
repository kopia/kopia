package serverapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

// Client provides helper methods for communicating with Kopia API serevr.
type Client struct {
	baseURL string
	client  *http.Client
}

// Get sends HTTP GET request and decodes the JSON response into the provided payload structure.
func (c *Client) Get(path string, respPayload interface{}) error {
	resp, err := c.client.Get(c.baseURL + path)
	if err != nil {
		return err
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != 200 {
		return fmt.Errorf("invalid server response: %v", resp.Status)
	}
	if err := json.NewDecoder(resp.Body).Decode(respPayload); err != nil {
		return fmt.Errorf("malformed server response: %v", err)
	}

	return nil
}

// Post sends HTTP post request with given JSON payload structure and decodes the JSON response into another payload structure.
func (c *Client) Post(path string, reqPayload, respPayload interface{}) error {
	var buf bytes.Buffer

	if err := json.NewEncoder(&buf).Encode(reqPayload); err != nil {
		return fmt.Errorf("unable to encode request: %v", err)
	}

	resp, err := c.client.Post(c.baseURL+path, "application/json", &buf)
	if err != nil {
		return err
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != 200 {
		return fmt.Errorf("invalid server response: %v", resp.Status)
	}
	if err := json.NewDecoder(resp.Body).Decode(respPayload); err != nil {
		return fmt.Errorf("malformed server response: %v", err)
	}

	return nil
}

// NewClient creates a client for connecting to Kopia HTTP API.
func NewClient(serverAddress string, cli *http.Client) *Client {
	if cli == nil {
		cli = http.DefaultClient
	}
	return &Client{"http://" + serverAddress + "/api/v1/", cli}
}
