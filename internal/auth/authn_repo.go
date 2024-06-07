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

func (ac *repositoryUserAuthenticator) IsValid(ctx context.Context, rep repo.Repository, username, password string) bool {
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

	// IsValidPassword can be safely called on nil and the call will take as much time as for a valid user
	// thus not revealing anything about whether the user exists.
	valid, err := ac.userProfiles[username].IsValidPassword(password)
	if err != nil {
		log(ctx).Debugf("password error for user '%s': %v", username, err)

		return false
	}

	return valid
}

func (ac *repositoryUserAuthenticator) Refresh(ctx context.Context) error {
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
