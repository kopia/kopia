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
	lastRep repo.Repository

	mu                          sync.Mutex
	nextRefreshTime             time.Time
	userProfiles                map[string]*user.Profile
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
	if clock.WallClockTime().After(ac.nextRefreshTime) {
		ac.nextRefreshTime = clock.WallClockTime().Add(ac.userProfileRefreshFrequency)

		newUsers, err := user.LoadProfileMap(ctx, rep, ac.userProfiles)
		if err != nil {
			log(ctx).Errorf("unable to load userProfiles map: %v", err)
		} else {
			ac.userProfiles = newUsers
		}
	}

	// IsValidPassword can be safely called on nil and the call will take as much time as for a valid user
	// thus not revealing anything about whether the user exists.
	return ac.userProfiles[username].IsValidPassword(password)
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
