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

// AuthenticateSingleUser returns an Authenticator that only allows one username/password combination.
func (ac *repositoryUserAuthenticator) authenticate(ctx context.Context, rep repo.Repository, username, password string) bool {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	// if the server switched to serving another repository, discard cache.
	if rep != ac.lastRep {
		ac.userProfiles = nil
		ac.lastRep = rep
	}

	// see if we're due for a refresh and refresh userProfiles map
	if clock.Now().After(ac.nextRefreshTime) {
		ac.nextRefreshTime = clock.Now().Add(ac.userProfileRefreshFrequency)

		newUsers, err := user.LoadProfileMap(ctx, rep, ac.userProfiles)
		if err != nil {
			log(ctx).Warningf("unable to load userProfiles map: %v", err)
		} else {
			ac.userProfiles = newUsers
		}
	}

	u := ac.userProfiles[username]
	if u == nil {
		return false
	}

	return u.IsValidPassword(password)
}

// AuthenticateRepositoryUsers returns authenticator that accepts username/password combinations
// stored in 'user' manifests in the repository.
func AuthenticateRepositoryUsers() Authenticator {
	a := &repositoryUserAuthenticator{
		userProfileRefreshFrequency: defaultProfileRefreshFrequency,
	}

	return a.authenticate
}
