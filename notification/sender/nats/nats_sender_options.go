package nats

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/notification/sender"
)

// Options defines NATS notification sender options.
type Options struct {
	ServerURL string `json:"serverURL"` // e.g. "nats://localhost:4222", may be a comma-separated list for a cluster
	Subject   string `json:"subject"`   // NATS subject (topic) to publish messages to
	Format    string `json:"format"`    // format of the message, must be "html" or "txt"

	// optional authentication, at most one of these is typically set.
	Username        string `json:"username,omitempty"`
	Password        string `json:"password,omitempty"`
	Token           string `json:"token,omitempty"`
	CredentialsFile string `json:"credentialsFile,omitempty"` // path to a NATS .creds (JWT) file
	NKeySeedFile    string `json:"nkeySeedFile,omitempty"`    // path to an NKey seed file

	InsecureSkipVerify bool `json:"insecureSkipVerify,omitempty"`
}

// ApplyDefaultsAndValidate applies default values and validates the configuration.
func (o *Options) ApplyDefaultsAndValidate(_ context.Context) error {
	if o.ServerURL == "" {
		return errors.Errorf("server URL must be provided")
	}

	if o.Subject == "" {
		return errors.Errorf("subject must be provided")
	}

	if err := sender.ValidateMessageFormatAndSetDefault(&o.Format, sender.FormatPlainText); err != nil {
		return errors.Wrap(err, "invalid format")
	}

	return nil
}

// MergeOptions updates the destination options with the source options.
func MergeOptions(ctx context.Context, src Options, dst *Options, isUpdate bool) error {
	copyOrMerge(&dst.ServerURL, src.ServerURL, isUpdate)
	copyOrMerge(&dst.Subject, src.Subject, isUpdate)
	copyOrMerge(&dst.Format, src.Format, isUpdate)
	copyOrMerge(&dst.Username, src.Username, isUpdate)
	copyOrMerge(&dst.Password, src.Password, isUpdate)
	copyOrMerge(&dst.Token, src.Token, isUpdate)
	copyOrMerge(&dst.CredentialsFile, src.CredentialsFile, isUpdate)
	copyOrMerge(&dst.NKeySeedFile, src.NKeySeedFile, isUpdate)
	copyOrMerge(&dst.InsecureSkipVerify, src.InsecureSkipVerify, isUpdate)

	return dst.ApplyDefaultsAndValidate(ctx)
}

func copyOrMerge[T comparable](dst *T, src T, isUpdate bool) {
	var defaultT T

	if !isUpdate || src != defaultT {
		*dst = src
	}
}
