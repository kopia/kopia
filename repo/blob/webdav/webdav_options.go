package webdav

// Options defines options for Filesystem-backed storage.
type Options struct {
	URL                                 string `json:"url"`
	DirectoryShards                     []int  `json:"dirShards"`
	Username                            string `json:"username,omitempty"`
	Password                            string `json:"password,omitempty" kopia:"sensitive"`
	TrustedServerCertificateFingerprint string `json:"trustedServerCertificateFingerprint,omitempty"`
}

func (fso *Options) shards() []int {
	if fso.DirectoryShards == nil {
		return fsDefaultShards
	}

	return fso.DirectoryShards
}
