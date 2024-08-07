package webhook

import (
	"context"
	"net/url"

	"github.com/pkg/errors"
)

// Options defines Webhook sender options.
type Options struct {
	Endpoint string `json:"endpoint"`
	Method   string `json:"method"`
	Format   string `json:"format"`
}

// ApplyDefaultsAndValidate applies default values and validates the configuration.
func (o *Options) ApplyDefaultsAndValidate(ctx context.Context) error {
	if o.Method == "" {
		o.Method = "POST"
	}

	if o.Format == "" {
		o.Format = "md"
	}

	u, err := url.ParseRequestURI(o.Endpoint)
	if err != nil {
		return errors.Errorf("invalid endpoint")
	}

	if u.Scheme != "http" && u.Scheme != "https" {
		return errors.Errorf("invalid endpoint scheme, must be http:// or https://")
	}

	return nil
}

// MergeOptions updates the destination options with the source options.
func MergeOptions(src Options, dst *Options, isUpdate bool) {
	copyOrMerge(&dst.Endpoint, src.Endpoint, isUpdate)
	copyOrMerge(&dst.Method, src.Method, isUpdate)
	copyOrMerge(&dst.Format, src.Format, isUpdate)
}

func copyOrMerge[T comparable](dst *T, src T, isUpdate bool) {
	var defaultT T

	if !isUpdate || src != defaultT {
		*dst = src
	}
}
