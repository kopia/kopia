// Package feature tracks features that are supported by Kopia client to ensure forwards and backwards
// compatibility.
package feature

import (
	"fmt"
)

// IfNotUnderstood describes the behavior of Kopia when a required feature is not understood.
type IfNotUnderstood struct {
	Warn             bool   `json:"warn"`                       // warn instead of failing
	Message          string `json:"message,omitempty"`          // message to show to users
	URL              string `json:"url,omitempty"`              // URL to show to users
	UpgradeToVersion string `json:"upgradeToVersion,omitempty"` // suggest upgrading to a particular version
}

// Feature identifies a feature that particular Kopia client must understand.
type Feature string

// Required describes a single required feature that a client must understand
// along with desired behavior when not understood.
type Required struct {
	Feature         Feature         `json:"feature"`
	IfNotUnderstood IfNotUnderstood `json:"ifNotUnderstood"`
}

// UnsupportedMessage returns a message to display to users if the feature is unsupported.
func (r Required) UnsupportedMessage() string {
	msg := fmt.Sprintf("This version of Kopia does not support feature '%v'.", r.Feature)

	if r.IfNotUnderstood.Message != "" {
		msg += "\n" + r.IfNotUnderstood.Message
	}

	if r.IfNotUnderstood.URL != "" {
		msg += "\nSee: " + r.IfNotUnderstood.URL
	}

	if r.IfNotUnderstood.UpgradeToVersion != "" {
		msg += "\nPlease upgrade to version " + r.IfNotUnderstood.UpgradeToVersion + " or newer."
	}

	return msg
}

// GetUnsupportedFeatures compares the provided list of required features to the list of supported features
// and returns the list of []RequireFeature that are not supported.
func GetUnsupportedFeatures(required []Required, supported []Feature) []Required {
	var result []Required

	for _, req := range required {
		if !isSupported(req, supported) {
			result = append(result, req)
		}
	}

	return result
}

func isSupported(req Required, supported []Feature) bool {
	for _, s := range supported {
		if req.Feature == s {
			return true
		}
	}

	return false
}
