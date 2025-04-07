// Package notification provides a mechanism to send notifications for various events.
package notification

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/notification/notifyprofile"
	"github.com/kopia/kopia/notification/notifytemplate"
	"github.com/kopia/kopia/notification/sender"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/logging"
)

// AdditionalSenders is a list of additional senders that will be used in addition to the senders configured in the repository.
//
//nolint:gochecknoglobals
var AdditionalSenders []sender.Sender

var log = logging.Module("notification")

// TemplateArgs represents the arguments passed to the notification template when rendering.
type TemplateArgs struct {
	Hostname          string
	EventTime         time.Time
	EventArgs         any
	KopiaRepo         string
	KopiaBuildInfo    string
	KopiaBuildVersion string
}

// Severity represents the severity of a notification message.
type Severity = sender.Severity

const (
	// SeverityVerbose includes all notification messages, including frequent and verbose ones.
	SeverityVerbose Severity = -100

	// SeveritySuccess is used for successful operations.
	SeveritySuccess Severity = -10

	// SeverityDefault includes notification messages enabled by default.
	SeverityDefault Severity = 0

	// SeverityReport is used for periodic reports.
	SeverityReport Severity = 0

	// SeverityWarning is used for warnings about potential issues.
	SeverityWarning Severity = 10

	// SeverityError is used for errors that require attention.
	SeverityError Severity = 20
)

// SeverityToNumber maps severity names to numbers.
//
//nolint:gochecknoglobals
var SeverityToNumber = map[string]Severity{
	"verbose": SeverityVerbose,
	"success": SeveritySuccess,
	"report":  SeverityReport,
	"warning": SeverityWarning,
	"error":   SeverityError,
}

// SeverityToString maps severity numbers to names.
//
//nolint:gochecknoglobals
var SeverityToString map[Severity]string

func init() {
	SeverityToString = make(map[Severity]string)

	for k, v := range SeverityToNumber {
		SeverityToString[v] = k
	}
}

func notificationSendersFromRepo(ctx context.Context, rep repo.Repository, severity Severity) ([]sender.Sender, error) {
	profiles, err := notifyprofile.ListProfiles(ctx, rep)
	if err != nil {
		return nil, errors.Wrap(err, "unable to list notification profiles")
	}

	var result []sender.Sender

	for _, p := range profiles {
		if severity < p.MinSeverity {
			continue
		}

		s, err := sender.GetSender(ctx, p.ProfileName, p.MethodConfig.Type, p.MethodConfig.Config)
		if err != nil {
			log(ctx).Warnw("unable to create sender for notification profile", "profile", p.ProfileName, "err", err)
			continue
		}

		result = append(result, s)
	}

	return result, nil
}

// Send sends a notification for the given event.
// Any errors encountered during the process are logged.
func Send(ctx context.Context, rep repo.Repository, templateName string, eventArgs any, sev Severity, opt notifytemplate.Options) {
	// if we're connected to a repository server, send the notification there.
	if rem, ok := rep.(repo.RemoteNotifications); ok {
		jsonData, err := json.Marshal(eventArgs)
		if err != nil {
			log(ctx).Warnw("unable to marshal event args", "err", err)

			return
		}

		if err := rem.SendNotification(ctx, templateName, jsonData, int32(sev)); err != nil {
			log(ctx).Warnw("unable to send notification", "err", err)
		}

		return
	}

	if err := SendInternal(ctx, rep, templateName, eventArgs, sev, opt); err != nil {
		log(ctx).Warnw("unable to send notification", "err", err)
	}
}

// SendInternal sends a notification for the given event and returns an error.
func SendInternal(ctx context.Context, rep repo.Repository, templateName string, eventArgs any, sev Severity, opt notifytemplate.Options) error {
	senders, err := notificationSendersFromRepo(ctx, rep, sev)
	if err != nil {
		return errors.Wrap(err, "unable to get notification senders")
	}

	senders = append(senders, AdditionalSenders...)

	var resultErr error

	for _, s := range senders {
		if err := SendTo(ctx, rep, s, templateName, eventArgs, sev, opt); err != nil {
			resultErr = multierr.Append(resultErr, err)
		}
	}

	return resultErr //nolint:wrapcheck
}

// MakeTemplateArgs wraps event-specific arguments into TemplateArgs object.
func MakeTemplateArgs(eventArgs any) TemplateArgs {
	now := clock.Now()

	h, _ := os.Hostname()
	if h == "" {
		h = "unknown hostname"
	}

	// prepare template arguments
	return TemplateArgs{
		Hostname:          h,
		EventArgs:         eventArgs,
		EventTime:         now,
		KopiaRepo:         repo.BuildGitHubRepo,
		KopiaBuildInfo:    repo.BuildInfo,
		KopiaBuildVersion: repo.BuildVersion,
	}
}

// SendTo sends a notification to the given sender.
func SendTo(ctx context.Context, rep repo.Repository, s sender.Sender, templateName string, eventArgs any, sev Severity, opt notifytemplate.Options) error {
	// execute template
	var bodyBuf bytes.Buffer

	tmpl, err := notifytemplate.ResolveTemplate(ctx, rep, s.ProfileName(), templateName, s.Format())
	if err != nil {
		return errors.Wrap(err, "unable to resolve notification template")
	}

	t, err := notifytemplate.ParseTemplate(tmpl, opt)
	if err != nil {
		return errors.Wrap(err, "unable to parse notification template")
	}

	if err := t.Execute(&bodyBuf, MakeTemplateArgs(eventArgs)); err != nil {
		return errors.Wrap(err, "unable to execute notification template")
	}

	// extract headers from the template
	msg, err := sender.ParseMessage(ctx, &bodyBuf)
	if err != nil {
		return errors.Wrap(err, "unable to parse message from notification template")
	}

	msg.Severity = sev

	var resultErr error

	if err := s.Send(ctx, msg); err != nil {
		resultErr = multierr.Append(resultErr, errors.Wrap(err, "unable to send notification message"))
	}

	return resultErr //nolint:wrapcheck
}

// SendTestNotification sends a test notification to the given sender.
func SendTestNotification(ctx context.Context, rep repo.Repository, s sender.Sender) error {
	log(ctx).Infof("Sending test notification to %v", s.Summary())

	return SendTo(ctx, rep, s, notifytemplate.TestNotification, struct{}{}, SeveritySuccess, notifytemplate.DefaultOptions)
}
