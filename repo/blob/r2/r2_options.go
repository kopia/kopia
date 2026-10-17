package r2

import (
	"net/url"
	"strings"
	"unicode"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob/s3"
	"github.com/kopia/kopia/repo/blob/throttling"
)

const (
	// StorageType is the blob storage type used for Cloudflare R2 repositories.
	StorageType = "r2"
	r2Region    = "auto"
)

// Options defines options for Cloudflare R2-backed storage.
type Options struct {
	// AccountID is the Cloudflare account ID used to derive the R2 endpoint.
	AccountID string `json:"accountID,omitempty"`

	// Jurisdiction scopes the derived R2 endpoint when set to "eu" or "fedramp".
	Jurisdiction string `json:"jurisdiction,omitempty"`

	// BucketName is the name of the R2 bucket where data is stored.
	BucketName string `json:"bucket"`

	// Prefix specifies additional string to prepend to all objects.
	Prefix string `json:"prefix,omitempty"`

	// Endpoint optionally overrides the endpoint derived from AccountID.
	Endpoint string `json:"endpoint,omitempty"`

	DoNotUseTLS    bool   `json:"doNotUseTLS,omitempty"`
	DoNotVerifyTLS bool   `json:"doNotVerifyTLS,omitempty"`
	RootCA         []byte `json:"rootCA,omitempty"`

	AccessKeyID     string `json:"accessKeyID"`
	SecretAccessKey string `json:"secretAccessKey" kopia:"sensitive"`
	SessionToken    string `json:"sessionToken" kopia:"sensitive"`

	throttling.Limits
}

func (o *Options) toS3Options() (*s3.Options, error) {
	endpoint, doNotUseTLS, err := o.s3Endpoint()
	if err != nil {
		return nil, err
	}

	if o.BucketName == "" {
		return nil, errors.New("bucket name must be specified")
	}

	return &s3.Options{
		BucketName:      o.BucketName,
		Prefix:          o.Prefix,
		Endpoint:        endpoint,
		DoNotUseTLS:     doNotUseTLS,
		DoNotVerifyTLS:  o.DoNotVerifyTLS,
		RootCA:          o.RootCA,
		AccessKeyID:     o.AccessKeyID,
		SecretAccessKey: o.SecretAccessKey,
		SessionToken:    o.SessionToken,
		Region:          r2Region,
		Limits:          o.Limits,
	}, nil
}

func (o *Options) s3Endpoint() (endpoint string, doNotUseTLS bool, err error) {
	if o.Endpoint != "" {
		return normalizeEndpoint(o.Endpoint, o.DoNotUseTLS)
	}

	if o.AccountID == "" {
		return "", false, errors.New("account ID must be specified when endpoint is not provided")
	}
	if !isValidAccountID(o.AccountID) {
		return "", false, errors.New("account ID must contain only letters and digits")
	}

	switch strings.ToLower(o.Jurisdiction) {
	case "", "default":
		return o.AccountID + ".r2.cloudflarestorage.com", o.DoNotUseTLS, nil
	case "eu":
		return o.AccountID + ".eu.r2.cloudflarestorage.com", o.DoNotUseTLS, nil
	case "fedramp":
		return o.AccountID + ".fedramp.r2.cloudflarestorage.com", o.DoNotUseTLS, nil
	default:
		return "", false, errors.Errorf("unsupported R2 jurisdiction: %q", o.Jurisdiction)
	}
}

func normalizeEndpoint(endpoint string, doNotUseTLS bool) (string, bool, error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return "", false, errors.New("endpoint must be specified")
	}

	if !strings.Contains(endpoint, "://") {
		if strings.ContainsAny(endpoint, "/?#@") {
			return "", false, errors.New("endpoint must not include path, query, fragment, or user info")
		}

		return endpoint, doNotUseTLS, nil
	}

	u, err := url.Parse(endpoint)
	if err != nil {
		return "", false, errors.Wrap(err, "invalid endpoint")
	}

	if (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
		return "", false, errors.New("endpoint must not include path, query, fragment, or user info")
	}

	if u.Host == "" {
		return "", false, errors.New("endpoint host must be specified")
	}

	switch u.Scheme {
	case "https":
		if doNotUseTLS {
			return "", false, errors.New("endpoint uses https but TLS is disabled")
		}

		return u.Host, false, nil

	case "http":
		return u.Host, true, nil

	default:
		return "", false, errors.Errorf("unsupported endpoint scheme: %q", u.Scheme)
	}
}

func isValidAccountID(accountID string) bool {
	for _, r := range accountID {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			return false
		}
	}

	return true
}
