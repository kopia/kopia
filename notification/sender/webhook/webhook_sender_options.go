package webhook

import (
	"context"
	"net/url"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/notification/sender"
)

// Options defines Webhook sender options.
type Options struct {
	Endpoint string `json:"endpoint"`
	Method   string `json:"method"`
	Format   string `json:"format"`
	Headers  string `json:"headers"` // newline-separated list of headers (key: value)
}

// ApplyDefaultsAndValidate applies default values and validates the configuration.
func (o *Options) ApplyDefaultsAndValidate(ctx context.Context) error {
	if o.Method == "" {
		o.Method = "POST"
	}

	if err := sender.ValidateMessageFormatAndSetDefault(&o.Format, sender.FormatPlainText); err != nil {
		return errors.Wrap(err, "invalid format")
	}

	u, err := url.ParseRequestURI(o.Endpoint)
	if err != nil {
		return errors.Errorf("invalid endpoint")
	}

	if u.Scheme != "http" && u.Scheme != "https" {
		return errors.Errorf("invalid endpoint scheme, must be http:// or https://")
	}

	if o.Format == "" {
		o.Format = sender.FormatPlainText
	}

	return nil
}

// MergeOptions updates the destination options with the source options.
func MergeOptions(ctx context.Context, src Options, dst *Options, isUpdate bool) error {
	copyOrMerge(&dst.Endpoint, src.Endpoint, isUpdate)
	copyOrMerge(&dst.Method, src.Method, isUpdate)
	copyOrMerge(&dst.Headers, src.Headers, isUpdate)
	copyOrMerge(&dst.Format, src.Format, isUpdate)

	return dst.ApplyDefaultsAndValidate(ctx)
}

func copyOrMerge[T comparable](dst *T, src T, isUpdate bool) {
	var defaultT T

	if !isUpdate || src != defaultT {
		*dst = src
	}
}
