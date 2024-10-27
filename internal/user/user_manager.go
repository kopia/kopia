// Package user provides management of user accounts.
package user

import (
	"context"
	"regexp"
	"sort"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
)

// ManifestType is the type of the manifest used to represent user accounts.
const ManifestType = "user"

// UsernameAtHostnameLabel is the manifest label identifying users by username@hostname.
const UsernameAtHostnameLabel = "username"

// ErrUserNotFound is returned to indicate that a user was not found in the system.
var ErrUserNotFound = errors.New("user not found")

// ErrUserAlreadyExists indicates that a user already exist in the system when attempting to create a new one.
var ErrUserAlreadyExists = errors.New("user already exists")

// LoadProfileMap returns the map of all users profiles in the repository by username, using old map as a cache.
func LoadProfileMap(ctx context.Context, rep repo.Repository, old map[string]*Profile) (map[string]*Profile, error) {
	if rep == nil {
		return nil, nil
	}

	entries, err := rep.FindManifests(ctx, map[string]string{manifest.TypeLabelKey: ManifestType})
	if err != nil {
		return nil, errors.Wrap(err, "error listing user manifests")
	}

	result := map[string]*Profile{}

	for _, m := range manifest.DedupeEntryMetadataByLabel(entries, UsernameAtHostnameLabel) {
		user := m.Labels[UsernameAtHostnameLabel]

		// same user info as before
		if o := old[user]; o != nil && o.ManifestID == m.ID {
			result[user] = o
			continue
		}

		p := &Profile{}
		if _, err := rep.GetManifest(ctx, m.ID, p); err != nil {
			return nil, errors.Wrapf(err, "error loading user manifest %v", user)
		}

		p.ManifestID = m.ID

		result[user] = p
	}

	return result, nil
}

// ListUserProfiles gets the list of all user profiles in the system.
func ListUserProfiles(ctx context.Context, rep repo.Repository) ([]*Profile, error) {
	var result []*Profile

	users, err := LoadProfileMap(ctx, rep, nil)
	if err != nil {
		return nil, err
	}

	for _, v := range users {
		result = append(result, v)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Username < result[j].Username
	})

	return result, nil
}

// GetUserProfile returns the user profile with a given username.
// Returns ErrUserNotFound when the user does not exist.
func GetUserProfile(ctx context.Context, r repo.Repository, username string) (*Profile, error) {
	manifests, err := r.FindManifests(ctx, map[string]string{
		manifest.TypeLabelKey:   ManifestType,
		UsernameAtHostnameLabel: username,
	})
	if err != nil {
		return nil, errors.Wrap(err, "error looking for user profile")
	}

	if len(manifests) == 0 {
		return nil, errors.Wrap(ErrUserNotFound, username)
	}

	p := &Profile{}
	if _, err := r.GetManifest(ctx, manifest.PickLatestID(manifests), p); err != nil {
		return nil, errors.Wrap(err, "error loading user profile")
	}

	return p, nil
}

// GetNewProfile returns a profile for a new user with the given username.
// Returns ErrUserAlreadyExists when the user already exists.
func GetNewProfile(ctx context.Context, r repo.Repository, username string) (*Profile, error) {
	if err := ValidateUsername(username); err != nil {
		return nil, err
	}

	manifests, err := r.FindManifests(ctx, map[string]string{
		manifest.TypeLabelKey:   ManifestType,
		UsernameAtHostnameLabel: username,
	})
	if err != nil {
		return nil, errors.Wrap(err, "error looking for user profile")
	}

	if len(manifests) != 0 {
		return nil, errors.Wrap(ErrUserAlreadyExists, username)
	}

	return &Profile{
			Username:            username,
			PasswordHashVersion: defaultPasswordHashVersion,
		},
		nil
}

// validUsernameRegexp matches username@hostname where both username and hostname consist of
// lowercase letters, digits or dashes, underscores or period characters.
var validUsernameRegexp = regexp.MustCompile(`^[a-z0-9\-_.]+@[a-z0-9\-_.]+$`)

// ValidateUsername returns an error if the given username is invalid.
func ValidateUsername(name string) error {
	if name == "" {
		return errors.New("username is required")
	}

	if !validUsernameRegexp.MatchString(name) {
		return errors.New("username must be specified as lowercase 'user@hostname'")
	}

	return nil
}

// SetUserProfile creates or updates user profile.
func SetUserProfile(ctx context.Context, w repo.RepositoryWriter, p *Profile) error {
	if err := ValidateUsername(p.Username); err != nil {
		return err
	}

	id, err := w.ReplaceManifests(ctx, map[string]string{
		manifest.TypeLabelKey:   ManifestType,
		UsernameAtHostnameLabel: p.Username,
	}, p)
	if err != nil {
		return errors.Wrap(err, "error looking for user profile")
	}

	p.ManifestID = id

	return nil
}

// DeleteUserProfile removes user profile with a given username.
func DeleteUserProfile(ctx context.Context, w repo.RepositoryWriter, username string) error {
	if username == "" {
		return errors.New("username is required")
	}

	manifests, err := w.FindManifests(ctx, map[string]string{
		manifest.TypeLabelKey:   ManifestType,
		UsernameAtHostnameLabel: username,
	})
	if err != nil {
		return errors.Wrap(err, "error looking for user profile")
	}

	for _, m := range manifests {
		if err := w.DeleteManifest(ctx, m.ID); err != nil {
			return errors.Wrapf(err, "error deleting user profile %v", username)
		}
	}

	return nil
}
