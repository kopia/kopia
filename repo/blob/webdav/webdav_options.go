package webdav

import "github.com/kopia/kopia/repo/blob/sharded"

// Options defines options for Filesystem-backed storage.
type Options struct {
	URL                                 string `json:"url"`
	Username                            string `json:"username,omitempty"`
	Password                            string `json:"password,omitempty" kopia:"sensitive"`
	TrustedServerCertificateFingerprint string `json:"trustedServerCertificateFingerprint,omitempty"`
	AtomicWrites                        bool   `json:"atomicWrites"`

	sharded.Options
}
