package auth

import (
	"context"
	"sync"
	"time"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/user"
	"github.com/kopia/kopia/repo"
)

const defaultProfileRefreshFrequency = 10 * time.Second

type repositoryUserAuthenticator struct {
	mu sync.Mutex
	// +checklocks:mu
	lastRep repo.Repository
	// +checklocks:mu
	nextRefreshTime time.Time
	// +checklocks:mu
	userProfiles map[string]*user.Profile
	// +checklocks:mu
	userProfileRefreshFrequency time.Duration
}

// loadProfile refreshes and returns the user profile for the given username,
// acquiring the lock internally. The returned Profile is immutable after
// loading, so it is safe to use after this function returns (no lock is held
// on return).
func (ac *repositoryUserAuthenticator) loadProfile(ctx context.Context, rep repo.Repository, username string) *user.Profile {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	// if the server switched to serving another repository, discard cache.
	if rep != ac.lastRep {
		ac.userProfiles = nil
		ac.lastRep = rep

		// ensure profiles are reloaded below
		ac.nextRefreshTime = time.Time{}
	}

	// see if we're due for a refresh and refresh userProfiles map
	if clock.Now().After(ac.nextRefreshTime) {
		ac.nextRefreshTime = clock.Now().Add(ac.userProfileRefreshFrequency)

		newUsers, err := user.LoadProfileMap(ctx, rep, ac.userProfiles)
		if err != nil {
			log(ctx).Errorf("unable to load userProfiles map: %v", err)
		} else {
			ac.userProfiles = newUsers
		}
	}

	return ac.userProfiles[username]
}

func (ac *repositoryUserAuthenticator) IsValid(ctx context.Context, rep repo.Repository, username, password string) bool {
	// loadProfile acquires the lock internally and returns an immutable Profile
	// pointer. We perform the CPU-intensive password verification outside the
	// lock to avoid serializing concurrent session authentications.
	profile := ac.loadProfile(ctx, rep, username)

	// IsValidPassword can be safely called on nil and the call will take as much time as for a valid user
	// thus not revealing anything about whether the user exists.
	valid, err := profile.IsValidPassword(password)
	if err != nil {
		log(ctx).Debugf("password error for user '%s': %v", username, err)

		return false
	}

	return valid
}

func (ac *repositoryUserAuthenticator) Refresh(_ context.Context) error {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	ac.nextRefreshTime = time.Time{}

	return nil
}

// AuthenticateRepositoryUsers returns authenticator that accepts username/password combinations
// stored in 'user' manifests in the repository.
func AuthenticateRepositoryUsers() Authenticator {
	a := &repositoryUserAuthenticator{
		userProfileRefreshFrequency: defaultProfileRefreshFrequency,
	}

	return a
}
