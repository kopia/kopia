// Package swift implements Storage based on an OpenStack Swift container.
package swift

import "github.com/kopia/kopia/repo/blob/throttling"

// Options defines options for OpenStack Swift-based storage.
type Options struct {
	// ContainerName is the name of the Swift container where data is stored.
	ContainerName string `json:"container"`

	// Prefix specifies additional string to prepend to all objects.
	Prefix string `json:"prefix,omitempty"`

	AuthURL string `json:"authURL"`

	Username string `json:"username,omitempty"`
	UserID   string `json:"userID,omitempty"`
	Password string `json:"password,omitempty" kopia:"sensitive"`

	DomainName string `json:"domainName,omitempty"`
	DomainID   string `json:"domainID,omitempty"`
	TenantName string `json:"tenantName,omitempty"`
	TenantID   string `json:"tenantID,omitempty"`

	Token string `json:"token,omitempty" kopia:"sensitive"`

	ApplicationCredentialID     string `json:"applicationCredentialID,omitempty"`
	ApplicationCredentialName   string `json:"applicationCredentialName,omitempty"`
	ApplicationCredentialSecret string `json:"applicationCredentialSecret,omitempty" kopia:"sensitive"`

	Region       string `json:"region,omitempty"`
	Availability string `json:"availability,omitempty"`

	ReadOnly bool `json:"readOnly,omitempty"`

	DoNotVerifyTLS bool   `json:"doNotVerifyTLS,omitempty"`
	RootCA         []byte `json:"rootCA,omitempty"`

	throttling.Limits
}
