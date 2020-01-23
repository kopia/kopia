package serverapi

import (
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"net/http"

	"github.com/pkg/errors"
)

// DefaultUsername is the default username for Kopia server.
const DefaultUsername = "kopia"

// Client provides helper methods for communicating with Kopia API serevr.
type Client struct {
	options ClientOptions
}

// Get sends HTTP GET request and decodes the JSON response into the provided payload structure.
func (c *Client) Get(path string, respPayload interface{}) error {
	req, err := http.NewRequest("GET", c.options.BaseURL+path, nil)
	if err != nil {
		return err
	}

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

// Post sends HTTP post request with given JSON payload structure and decodes the JSON response into another payload structure.
func (c *Client) Post(path string, reqPayload, respPayload interface{}) error {
	var buf bytes.Buffer

	if err := json.NewEncoder(&buf).Encode(reqPayload); err != nil {
		return errors.Wrap(err, "unable to encode request")
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

// ClientOptions encapsulates all optional API options.HTTPClient options.
type ClientOptions struct {
	BaseURL string

	HTTPClient *http.Client

	Username string
	Password string

	TrustedServerCertificateFingerprint string

	RootCAs *x509.CertPool
}

// NewClient creates a options.HTTPClient for connecting to Kopia HTTP API.
// nolint:hugeParam
func NewClient(options ClientOptions) (*Client, error) {
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

	return &Client{options}, nil
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
