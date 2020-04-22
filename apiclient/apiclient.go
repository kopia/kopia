// Package apiclient implements a client for connecting to Kopia HTTP API server.
package apiclient

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"net/http"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/logging"
)

var log = logging.GetContextLoggerFunc("client")

// DefaultUsername is the default username for Kopia server.
const DefaultUsername = "kopia"

// KopiaAPIClient provides helper methods for communicating with Kopia API server.
type KopiaAPIClient struct {
	options Options
}

// HTTPClient returns HTTP client that connects to the server.
func (c *KopiaAPIClient) HTTPClient() *http.Client {
	return c.options.HTTPClient
}

// Get sends HTTP GET request and decodes the JSON response into the provided payload structure.
func (c *KopiaAPIClient) Get(ctx context.Context, path string, respPayload interface{}) error {
	resp, err := c.GetRaw(ctx, path)
	if err != nil {
		return err
	}

	defer resp.Body.Close() //nolint:errcheck

	if err := json.NewDecoder(resp.Body).Decode(respPayload); err != nil {
		return errors.Wrap(err, "malformed server response")
	}

	return nil
}

// GetRaw returns the response of a GET call
func (c *KopiaAPIClient) GetRaw(ctx context.Context, path string) (*http.Response, error) {
	req, err := http.NewRequest("GET", c.options.BaseURL+path, nil)
	if err != nil {
		return nil, err
	}

	if c.options.LogRequests {
		log(ctx).Debugf("GET %v", req.URL)
	}

	if c.options.Username != "" {
		req.SetBasicAuth(c.options.Username, c.options.Password)
	}

	resp, err := c.HTTPClient().Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close() //nolint:errcheck
		return nil, errors.Errorf("invalid server response: %v", resp.Status)
	}

	return resp, nil
}

// Post sends HTTP post request with given JSON payload structure and decodes the JSON response into another payload structure.
func (c *KopiaAPIClient) Post(ctx context.Context, path string, reqPayload, respPayload interface{}) error {
	var buf bytes.Buffer

	if err := json.NewEncoder(&buf).Encode(reqPayload); err != nil {
		return errors.Wrap(err, "unable to encode request")
	}

	if c.options.LogRequests {
		log(ctx).Infof("POST %v (%v bytes)", c.options.BaseURL+path, buf.Len())
	}

	req, err := http.NewRequest("POST", c.options.BaseURL+path, &buf)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	if c.options.Username != "" {
		req.SetBasicAuth(c.options.Username, c.options.Password)
	}

	resp, err := c.options.HTTPClient.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		return errors.Errorf("invalid server response: %v", resp.Status)
	}

	if err := json.NewDecoder(resp.Body).Decode(respPayload); err != nil {
		return errors.Wrap(err, "malformed server response")
	}

	return nil
}

// Options encapsulates all optional API options.HTTPClient options.
type Options struct {
	BaseURL string

	HTTPClient *http.Client

	Username string
	Password string

	TrustedServerCertificateFingerprint string

	RootCAs *x509.CertPool

	LogRequests bool
}

// NewKopiaAPIClient creates a options.HTTPClient for connecting to Kopia HTTP API.
// nolint:gocritic
func NewKopiaAPIClient(options Options) (*KopiaAPIClient, error) {
	if options.HTTPClient == nil {
		transport := &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs: options.RootCAs,
			},
		}

		if f := options.TrustedServerCertificateFingerprint; f != "" {
			if options.RootCAs != nil {
				return nil, errors.Errorf("can't set both RootCAs and TrustedServerCertificateFingerprint")
			}

			transport.TLSClientConfig.InsecureSkipVerify = true
			transport.TLSClientConfig.VerifyPeerCertificate = verifyPeerCertificate(f)
		}

		options.HTTPClient = &http.Client{
			Transport: transport,
		}
	}

	if options.Username == "" {
		options.Username = DefaultUsername
	}

	options.BaseURL += "/api/v1/"

	return &KopiaAPIClient{options}, nil
}

func verifyPeerCertificate(sha256Fingerprint string) func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
		for _, c := range rawCerts {
			h := sha256.Sum256(c)
			if hex.EncodeToString(h[:]) == sha256Fingerprint {
				return nil
			}
		}

		return errors.Errorf("can't find certificate matching SHA256 fingerprint %q", sha256Fingerprint)
	}
}
