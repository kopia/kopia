package pushover

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/notification/sender"
)

// Options defines Pushover notification sender options.
type Options struct {
	AppToken string `json:"appToken"`
	UserKey  string `json:"userKey"`
	Format   string `json:"format"` // format of the message, must be "html" or "md"

	Endpoint string `json:"endpoint,omitempty"` // override the default endpoint for testing
}

// ApplyDefaultsAndValidate applies default values and validates the configuration.
func (o *Options) ApplyDefaultsAndValidate(ctx context.Context) error {
	if o.AppToken == "" {
		return errors.Errorf("App Token must be provided")
	}

	if o.UserKey == "" {
		return errors.Errorf("User Key must be provided")
	}

	if err := sender.ValidateMessageFormatAndSetDefault(&o.Format, sender.FormatPlainText); err != nil {
		return errors.Wrap(err, "invalid format")
	}

	return nil
}

// MergeOptions updates the destination options with the source options.
func MergeOptions(ctx context.Context, src Options, dst *Options, isUpdate bool) error {
	copyOrMerge(&dst.AppToken, src.AppToken, isUpdate)
	copyOrMerge(&dst.UserKey, src.UserKey, isUpdate)

	return dst.ApplyDefaultsAndValidate(ctx)
}

func copyOrMerge[T comparable](dst *T, src T, isUpdate bool) {
	var defaultT T

	if !isUpdate || src != defaultT {
		*dst = src
	}
}
