package email

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/notification/sender"
)

// Options defines email notification provider options.
type Options struct {
	SMTPServer   string `json:"smtpServer"`
	SMTPPort     int    `json:"smtpPort"`
	SMTPIdentity string `json:"smtpIdentity"` // usually empty, most servers use username/password
	SMTPUsername string `json:"smtpUsername"`
	SMTPPassword string `json:"smtpPassword"`

	From string `json:"from"`
	To   string `json:"to"`
	CC   string `json:"cc"`

	Format string `json:"format"` // format of the message, must be "html" or "md"
}

// MergeOptions updates the destination options with the source options.
func MergeOptions(ctx context.Context, src Options, dst *Options, isUpdate bool) error {
	copyOrMerge(&dst.SMTPServer, src.SMTPServer, isUpdate)
	copyOrMerge(&dst.SMTPPort, src.SMTPPort, isUpdate)
	copyOrMerge(&dst.SMTPIdentity, src.SMTPIdentity, isUpdate)
	copyOrMerge(&dst.SMTPUsername, src.SMTPUsername, isUpdate)
	copyOrMerge(&dst.SMTPPassword, src.SMTPPassword, isUpdate)
	copyOrMerge(&dst.From, src.From, isUpdate)
	copyOrMerge(&dst.To, src.To, isUpdate)
	copyOrMerge(&dst.CC, src.CC, isUpdate)
	copyOrMerge(&dst.Format, src.Format, isUpdate)

	return dst.ApplyDefaultsAndValidate(ctx)
}

// ApplyDefaultsAndValidate applies default values and validates the configuration.
func (o *Options) ApplyDefaultsAndValidate(ctx context.Context) error {
	if o.SMTPPort == 0 {
		o.SMTPPort = defaultSMTPPort
	}

	if o.SMTPServer == "" {
		return errors.Errorf("SMTP server must be provided")
	}

	if o.From == "" {
		return errors.Errorf("From address must be provided")
	}

	if o.To == "" {
		return errors.Errorf("To address must be provided")
	}

	if err := sender.ValidateMessageFormatAndSetDefault(&o.Format, sender.FormatHTML); err != nil {
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
