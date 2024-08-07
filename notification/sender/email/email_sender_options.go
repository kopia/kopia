package email

import (
	"github.com/pkg/errors"
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
func MergeOptions(src Options, dst *Options, isUpdate bool) {
	copyOrMerge(&dst.SMTPServer, src.SMTPServer, isUpdate)
	copyOrMerge(&dst.SMTPPort, src.SMTPPort, isUpdate)
	copyOrMerge(&dst.SMTPIdentity, src.SMTPIdentity, isUpdate)
	copyOrMerge(&dst.SMTPUsername, src.SMTPUsername, isUpdate)
	copyOrMerge(&dst.SMTPPassword, src.SMTPPassword, isUpdate)
	copyOrMerge(&dst.From, src.From, isUpdate)
	copyOrMerge(&dst.To, src.To, isUpdate)
	copyOrMerge(&dst.CC, src.CC, isUpdate)
}

func (o *Options) applyDefaultsAndValidate() error {
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

	if o.Format == "" {
		o.Format = "html"
	}

	return nil
}

func copyOrMerge[T comparable](dst *T, src T, isUpdate bool) {
	var defaultT T

	if !isUpdate || src != defaultT {
		*dst = src
	}
}
