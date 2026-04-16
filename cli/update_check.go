package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/mod/semver"

	"github.com/kopia/kopia/internal/atomicfile"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/repo"
)

const (
	checkForUpdatesEnvar = "KOPIA_CHECK_FOR_UPDATES"
	githubTimeout        = 10 * time.Second
)

const (
	latestReleaseGitHubURLFormat = "https://api.github.com/repos/%v/releases/latest"
	checksumsURLFormat           = "https://github.com/%v/releases/download/%v/checksums.txt.sig"
	autoUpdateNotice             = `
NOTICE: Kopia will check for updates on GitHub every 7 days, starting 24 hours after first use.
To disable this behavior, set environment variable ` + checkForUpdatesEnvar + `=false
Alternatively you can remove the file "%v".
`
	updateAvailableNoticeFormat = `
Upgrade of Kopia from %v to %v is available.
Visit https://github.com/%v/releases/latest to download it.

`
)

// updateState is persisted in a JSON file and used to determine when to check for updates
// and whether to notify user about updates.
type updateState struct {
	NextCheckTime    time.Time `json:"nextCheckTimestamp"`
	NextNotifyTime   time.Time `json:"nextNotifyTimestamp"`
	AvailableVersion string    `json:"availableVersion"`
}

// updateStateFilename returns the name of the update state.
func (c *App) updateStateFilename() string {
	return c.repositoryConfigFileName() + ".update-info.json"
}

// writeUpdateState writes update state file.
func (c *App) writeUpdateState(us *updateState) error {
	var buf bytes.Buffer

	if err := json.NewEncoder(&buf).Encode(us); err != nil {
		return errors.Wrap(err, "unable to marshal JSON")
	}

	return errors.Wrap(atomicfile.Write(c.updateStateFilename(), &buf), "error writing update state")
}

func (c *App) removeUpdateState() {
	os.Remove(c.updateStateFilename()) //nolint:errcheck
}

// getUpdateState reads the update state file if available.
func (c *App) getUpdateState() (*updateState, error) {
	f, err := os.Open(c.updateStateFilename())
	if err != nil {
		return nil, errors.Wrap(err, "unable to open update state file")
	}
	defer f.Close() //nolint:errcheck

	us := &updateState{}
	if err := json.NewDecoder(f).Decode(us); err != nil {
		return nil, errors.Wrap(err, "unable to parse update state")
	}

	return us, nil
}

// maybeInitializeUpdateCheck optionally writes update state file with initial update
// set 24 hours from now.
func (c *App) maybeInitializeUpdateCheck(ctx context.Context, co *connectOptions) {
	if co.connectCheckForUpdates {
		us := &updateState{
			NextCheckTime:  clock.Now().Add(c.initialUpdateCheckDelay),
			NextNotifyTime: clock.Now().Add(c.initialUpdateCheckDelay),
		}
		if err := c.writeUpdateState(us); err != nil {
			log(ctx).Debug("error initializing update state")
			return
		}

		log(ctx).Infof(autoUpdateNotice, c.updateStateFilename())
	} else {
		c.removeUpdateState()
	}
}

// getLatestReleaseNameFromGitHub gets the name of the release marked 'latest' on GitHub.
func getLatestReleaseNameFromGitHub(ctx context.Context) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, githubTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf(latestReleaseGitHubURLFormat, repo.BuildGitHubRepo), http.NoBody)
	if err != nil {
		return "", errors.Wrap(err, "unable to get latest release from github")
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", errors.Wrap(err, "unable to get latest release from github")
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		return "", errors.Errorf("invalid status code from GitHub: %v", resp.StatusCode)
	}

	var responseObject struct {
		Name string `json:"name"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&responseObject); err != nil {
		return "", errors.Wrap(err, "invalid GitHub API response")
	}

	return responseObject.Name, nil
}

// verifyGitHubReleaseIsComplete downloads checksum file to verify that the release is complete.
func verifyGitHubReleaseIsComplete(ctx context.Context, releaseName string) error {
	ctx, cancel := context.WithTimeout(ctx, githubTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf(checksumsURLFormat, repo.BuildGitHubRepo, releaseName), http.NoBody)
	if err != nil {
		return errors.Wrap(err, "unable to download releases checksum")
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "unable to download releases checksum")
	}

	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		return errors.Errorf("invalid status code from GitHub: %v", resp.StatusCode)
	}

	return nil
}

func (c *App) maybeCheckForUpdates(ctx context.Context) (string, error) {
	if v := os.Getenv(c.EnvName(checkForUpdatesEnvar)); v != "" {
		// see if environment variable is set to false.
		if b, err := strconv.ParseBool(v); err == nil && !b {
			return "", errors.New("update check disabled")
		}
	}

	us, err := c.getUpdateState()
	if err != nil {
		return "", err
	}

	if err := c.maybeCheckGithub(ctx, us); err != nil {
		return "", errors.Wrap(err, "error checking GitHub")
	}

	log(ctx).Debugf("build version %v, available %v", ensureVPrefix(repo.BuildVersion), ensureVPrefix(us.AvailableVersion))

	if us.AvailableVersion == "" || semver.Compare(ensureVPrefix(repo.BuildVersion), ensureVPrefix(us.AvailableVersion)) >= 0 {
		// no new version available
		return "", nil
	}

	if clock.Now().After(us.NextNotifyTime) {
		us.NextNotifyTime = clock.Now().Add(c.updateAvailableNotifyInterval)
		if err := c.writeUpdateState(us); err != nil {
			return "", errors.Wrap(err, "unable to write update state")
		}

		return us.AvailableVersion, nil
	}

	// no time to notify yet
	return "", nil
}

func (c *App) maybeCheckGithub(ctx context.Context, us *updateState) error {
	if !clock.Now().After(us.NextCheckTime) {
		return nil
	}

	log(ctx).Debug("time for next update check has been reached")

	// before we check for update, write update state file again, so if this fails
	// we won't bother GitHub for a while
	us.NextCheckTime = clock.Now().Add(c.updateCheckInterval)
	if err := c.writeUpdateState(us); err != nil {
		return errors.Wrap(err, "unable to write update state")
	}

	newAvailableVersion, err := getLatestReleaseNameFromGitHub(ctx)
	if err != nil {
		return errors.Wrap(err, "update to get the latest release from GitHub")
	}

	log(ctx).Debugf("latest version on GitHub: %v previous %v", newAvailableVersion, us.AvailableVersion)

	// we got updated version from GitHub, write it in a state file again
	if newAvailableVersion != us.AvailableVersion {
		if err = verifyGitHubReleaseIsComplete(ctx, newAvailableVersion); err != nil {
			return errors.Wrap(err, "unable to validate GitHub release")
		}

		us.AvailableVersion = newAvailableVersion

		if err := c.writeUpdateState(us); err != nil {
			return errors.Wrap(err, "unable to write update state")
		}
	}

	return nil
}

// maybePrintUpdateNotification prints notification about available version.
func (c *App) maybePrintUpdateNotification(ctx context.Context) {
	if repo.BuildGitHubRepo == "" {
		// not built from GH repo.
		return
	}

	updatedVersion, err := c.maybeCheckForUpdates(ctx)
	if err != nil {
		log(ctx).Debugf("unable to check for updates: %v", err)
		return
	}

	if updatedVersion == "" {
		log(ctx).Debug("no updated version available")
		return
	}

	log(ctx).Infof(updateAvailableNoticeFormat, ensureVPrefix(repo.BuildVersion), ensureVPrefix(updatedVersion), repo.BuildGitHubRepo)
}

func ensureVPrefix(s string) string {
	if strings.HasPrefix(s, "v") {
		return s
	}

	return "v" + s
}
