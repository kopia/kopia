// Package notifyprofile notification profile management.
package notifyprofile

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/notification/sender"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/repo/manifest"
)

var log = logging.Module("notification/profile")

const profileNameKey = "profile"

const notificationConfigManifestType = "notificationProfile"

// Config is a struct that represents the configuration for a single notification profile.
type Config struct {
	ProfileName  string              `json:"profile"`
	MethodConfig sender.MethodConfig `json:"method"`
	MinSeverity  sender.Severity     `json:"minSeverity"`
}

// Summary contains JSON-serializable summary of a notification profile.
type Summary struct {
	ProfileName string `json:"profile"`
	Type        string `json:"type"`
	Summary     string `json:"summary"`
	MinSeverity int32  `json:"minSeverity"`
}

// ListProfiles returns a list of notification profiles.
func ListProfiles(ctx context.Context, rep repo.Repository) ([]Config, error) {
	profileMetadata, err := rep.FindManifests(ctx,
		map[string]string{
			manifest.TypeLabelKey: notificationConfigManifestType,
		})
	if err != nil {
		return nil, errors.Wrap(err, "unable to list notification profiles")
	}

	var profiles []Config

	for _, m := range profileMetadata {
		var pc Config
		if _, err := rep.GetManifest(ctx, m.ID, &pc); err != nil {
			return nil, errors.Wrap(err, "unable to get notification profile")
		}

		profiles = append(profiles, pc)
	}

	return profiles, nil
}

// ErrNotFound is returned when a profile is not found.
var ErrNotFound = errors.New("profile not found")

// GetProfile returns a notification profile by name.
func GetProfile(ctx context.Context, rep repo.Repository, name string) (Config, error) {
	entries, err := rep.FindManifests(ctx, labelsForProfileName(name))
	if err != nil {
		return Config{}, errors.Wrap(err, "unable to list notification profiles")
	}

	if len(entries) == 0 {
		return Config{}, ErrNotFound
	}

	var pc Config

	_, err = rep.GetManifest(ctx, manifest.PickLatestID(entries), &pc)

	return pc, errors.Wrap(err, "unable to get notification profile")
}

// SaveProfile saves a notification profile.
func SaveProfile(ctx context.Context, rep repo.RepositoryWriter, pc Config) error {
	log(ctx).Debugf("saving notification profile %q with method %v", pc.ProfileName, pc.MethodConfig)

	_, err := rep.ReplaceManifests(ctx, labelsForProfileName(pc.ProfileName), &pc)
	if err != nil {
		return errors.Wrap(err, "unable to save notification profile")
	}

	return nil
}

// DeleteProfile deletes a notification profile.
func DeleteProfile(ctx context.Context, rep repo.RepositoryWriter, name string) error {
	entries, err := rep.FindManifests(ctx, labelsForProfileName(name))
	if err != nil {
		return errors.Wrap(err, "unable to list notification profiles")
	}

	for _, e := range entries {
		if err := rep.DeleteManifest(ctx, e.ID); err != nil {
			return errors.Wrapf(err, "unable to delete notification profile %q", e.ID)
		}
	}

	return nil
}

func labelsForProfileName(name string) map[string]string {
	return map[string]string{
		manifest.TypeLabelKey: notificationConfigManifestType,
		profileNameKey:        name,
	}
}
