package testsender

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/notification/sender"
)

// Options defines email notification provider options.
type Options struct {
	Format string `json:"format"` // format of the message, must be "html" or "md"
}

// MergeOptions updates the destination options with the source options.
func MergeOptions(ctx context.Context, src Options, dst *Options, isUpdate bool) error {
	copyOrMerge(&dst.Format, src.Format, isUpdate)

	return dst.ApplyDefaultsAndValidate(ctx)
}

func (o *Options) ApplyDefaultsAndValidate(ctx context.Context) error {
	if err := sender.ValidateMessageFormatAndSetDefault(&o.Format, "html"); err != nil {
		return errors.Wrap(err, "invalid format")
	}

	return nil
}

func copyOrMerge[T comparable](dst *T, src T, isUpdate bool) {
	var defaultT T

	if !isUpdate || src != defaultT {
		*dst = src
	}
}
