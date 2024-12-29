package cli

import (
	"context"

	"golang.org/x/exp/maps"

	"github.com/alecthomas/kingpin/v2"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/notification"
	"github.com/kopia/kopia/notification/notifyprofile"
	"github.com/kopia/kopia/notification/sender"
	"github.com/kopia/kopia/repo"
)

// commonNotificationOptions is a common configuration for notification methods.
type commonNotificationOptions struct {
	notificationProfileFlag
	sendTestNotification bool
	minSeverity          string
}

func (c *commonNotificationOptions) setup(svc appServices, cmd *kingpin.CmdClause) {
	c.notificationProfileFlag.setup(svc, cmd)
	cmd.Flag("send-test-notification", "Test the notification").BoolVar(&c.sendTestNotification)
	cmd.Flag("min-severity", "Minimum severity").EnumVar(&c.minSeverity, maps.Keys(notification.SeverityToNumber)...)
}

// configureNotificationAction is a helper function that creates a Kingpin action that
// configures a notification method.
// it will read the existing profile, merge the provided options, and save the profile back
// or send a test notification based on the flags.
func configureNotificationAction[T any](
	svc appServices,
	c *commonNotificationOptions,
	senderMethod sender.Method,
	opt *T,
	merge func(ctx context.Context, src T, dst *T, isUpdate bool) error,
) func(ctx *kingpin.ParseContext) error {
	return svc.directRepositoryWriteAction(func(ctx context.Context, rep repo.DirectRepositoryWriter) error {
		var (
			defaultT      T
			mergedOptions *T
		)

		// read the existing profile, if any.
		oldProfile, err := notifyprofile.GetProfile(ctx, rep, c.profileName)
		if err != nil && !errors.Is(err, notifyprofile.ErrNotFound) {
			return errors.Wrap(err, "unable to get notification profile")
		}

		sev := notification.SeverityDefault
		exists := err == nil

		if exists {
			if oldProfile.MethodConfig.Type != senderMethod {
				return errors.Errorf("profile %q already exists but is not of type %q", c.profileName, senderMethod)
			}

			var parsedT T

			if err := oldProfile.MethodConfig.Options(&parsedT); err != nil {
				return errors.Wrapf(err, "profile %q already exists but is not of type %q", c.profileName, senderMethod)
			}

			mergedOptions = &parsedT
			sev = oldProfile.MinSeverity
		} else {
			mergedOptions = &defaultT
		}

		if err := merge(ctx, *opt, mergedOptions, exists); err != nil {
			return errors.Wrap(err, "unable to merge options")
		}

		if c.minSeverity != "" {
			// severity is specified on the command line, override the one from the profile.
			sev = notification.SeverityToNumber[c.minSeverity]
		}

		s, err := sender.GetSender(ctx, c.profileName, senderMethod, mergedOptions)
		if err != nil {
			return errors.Wrap(err, "unable to get notification provider")
		}

		if c.sendTestNotification {
			if err := notification.SendTestNotification(ctx, rep, s); err != nil {
				return errors.Wrap(err, "unable to send test notification")
			}
		}

		log(ctx).Infof("Saving notification profile %q of type %q with severity %q.", c.profileName, senderMethod, notification.SeverityToString[sev])

		return notifyprofile.SaveProfile(ctx, rep, notifyprofile.Config{
			ProfileName: c.profileName,
			MethodConfig: sender.MethodConfig{
				Type:   senderMethod,
				Config: mergedOptions,
			},
			MinSeverity: sev,
		})
	})
}
