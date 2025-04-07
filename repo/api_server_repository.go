package repo

import (
	"context"

	"github.com/pkg/errors"
)

// APIServerInfo is remote repository configuration stored in local configuration.
//
// NOTE: this structure is persistent on disk may be read/written using
// different versions of Kopia, so it must be backwards-compatible.
//
// Apply appropriate defaults when reading.
type APIServerInfo struct {
	BaseURL                             string `json:"url"`
	TrustedServerCertificateFingerprint string `json:"serverCertFingerprint"`
	LocalCacheKeyDerivationAlgorithm    string `json:"localCacheKeyDerivationAlgorithm,omitempty"`
}

// ConnectAPIServer sets up repository connection to a particular API server.
func ConnectAPIServer(ctx context.Context, configFile string, si *APIServerInfo, password string, opt *ConnectOptions) error {
	lc := LocalConfig{
		APIServer:     si,
		ClientOptions: opt.ClientOptions.ApplyDefaults(ctx, "API Server: "+si.BaseURL),
	}

	if err := setupCachingOptionsWithDefaults(ctx, configFile, &lc, &opt.CachingOptions, []byte(si.BaseURL)); err != nil {
		return errors.Wrap(err, "unable to set up caching")
	}

	if err := lc.writeToFile(configFile); err != nil {
		return errors.Wrap(err, "unable to write config file")
	}

	return verifyConnect(ctx, configFile, password)
}
