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
	TrustedServerCACertificate          []byte `json:"serverCertCA,omitempty"`
	LocalCacheKeyDerivationAlgorithm    string `json:"localCacheKeyDerivationAlgorithm,omitempty"`
}

// serverCertFingerprintForCA is stored as serverCertFingerprint when serverCertCA is set.
// Older Kopia versions ignore serverCertCA: this value makes them fail to connect
// instead of trusting the system roots.
const serverCertFingerprintForCA = "trusted-by-server-cert-ca"

func (si *APIServerInfo) validate() error {
	if si.TrustedServerCertificateFingerprint != "" && si.TrustedServerCertificateFingerprint != serverCertFingerprintForCA && len(si.TrustedServerCACertificate) > 0 {
		return errors.New("invalid server info, serverCertFingerprint and serverCertCA are mutually exclusive")
	}

	if si.TrustedServerCACertificate != nil && len(si.TrustedServerCACertificate) == 0 {
		return errors.New("invalid server info, serverCertCA is empty")
	}

	return nil
}

// ConnectAPIServer sets up repository connection to a particular API server.
func ConnectAPIServer(ctx context.Context, configFile string, si *APIServerInfo, password string, opt *ConnectOptions) error {
	if err := si.validate(); err != nil {
		return err
	}

	stored := *si
	if len(stored.TrustedServerCACertificate) > 0 {
		stored.TrustedServerCertificateFingerprint = serverCertFingerprintForCA
	}

	lc := LocalConfig{
		APIServer:     &stored,
		ClientOptions: opt.ApplyDefaults(ctx, "API Server: "+si.BaseURL),
	}

	if err := setupCachingOptionsWithDefaults(ctx, configFile, &lc, &opt.CachingOptions, []byte(si.BaseURL)); err != nil {
		return errors.Wrap(err, "unable to set up caching")
	}

	if err := lc.writeToFile(configFile); err != nil {
		return errors.Wrap(err, "unable to write config file")
	}

	return verifyConnect(ctx, configFile, password)
}
