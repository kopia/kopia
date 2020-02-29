package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/fatih/color"
	"github.com/natefinch/atomic"
	"github.com/pkg/errors"
	"golang.org/x/mod/semver"

	"github.com/kopia/kopia/repo"
)

const checkForUpdatesEnvar = "KOPIA_CHECK_FOR_UPDATES"

// hidden flags to control auto-update behavior
var (
	initialUpdateCheckDelay       = app.Flag("initial-update-check-delay", "Initial delay before first time update check").Default("24h").Hidden().Envar("KOPIA_INITIAL_UPDATE_CHECK_DELAY").Duration()
	updateCheckInterval           = app.Flag("update-check-interval", "Interval between update checks").Default("168h").Hidden().Envar("KOPIA_UPDATE_CHECK_INTERVAL").Duration()
	updateAvailableNotifyInterval = app.Flag("update-available-notify-interval", "Interval between update notifications").Default("1h").Hidden().Envar("KOPIA_UPDATE_NOTIFY_INTERVAL").Duration()
)

const (
	latestReleaseGitHubURL = "https://api.github.com/repos/kopia/kopia/releases/latest"
	checksumsURL           = "https://github.com/kopia/kopia/releases/download/%v/checksums.txt.sig"
	autoUpdateNotice       = `
NOTICE: Kopia will check for updates on GitHub every 7 days, starting 24 hours after first use.
To disable this behavior, set environment variable ` + checkForUpdatesEnvar + `=false
Alternatively you can remove the file "%v".

`
	updateAvailableNotice = `
Upgrade of Kopia from %v to %v is available.
Visit https://github.com/kopia/kopia/releases/latest to download it.

`
)

var noticeColor = color.New(color.FgCyan)

// updateState is persisted in a JSON file and used to determine when to check for updates
// and whether to notify user about updates.
type updateState struct {
	NextCheckTime    time.Time `json:"nextCheckTimestamp"`
	NextNotifyTime   time.Time `json:"nextNotifyTimestamp"`
	AvailableVersion string    `json:"availableVersion"`
}

// updateStateFilename returns the name of the update state.
func updateStateFilename() string {
	return filepath.Join(repositoryConfigFileName() + ".update-info.json")
}

// writeUpdateState writes update state file.
func writeUpdateState(us *updateState) error {
	var buf bytes.Buffer

	if err := json.NewEncoder(&buf).Encode(us); err != nil {
		return errors.Wrap(err, "unable to marshal JSON")
	}

	return atomic.WriteFile(updateStateFilename(), &buf)
}

func removeUpdateState() {
	os.Remove(updateStateFilename()) // nolint:errcheck
}

// getUpdateState reads the update state file if available.
func getUpdateState() (*updateState, error) {
	f, err := os.Open(updateStateFilename())
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
func maybeInitializeUpdateCheck(ctx context.Context) {
	if connectCheckForUpdates {
		us := &updateState{
			NextCheckTime:  time.Now().Add(*initialUpdateCheckDelay),
			NextNotifyTime: time.Now().Add(*initialUpdateCheckDelay),
		}
		if err := writeUpdateState(us); err != nil {
			log(ctx).Debugf("error initializing update state")
			return
		}

		noticeColor.Fprintf(os.Stderr, autoUpdateNotice, updateStateFilename()) //nolint:errcheck
	} else {
		removeUpdateState()
	}
}

// getLatestReleaseNameFromGitHub gets the name of the release marked 'latest' on GitHub.
func getLatestReleaseNameFromGitHub() (string, error) {
	resp, err := http.DefaultClient.Get(latestReleaseGitHubURL)
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
		return "", errors.Wrap(err, "invalid Github API response")
	}

	return responseObject.Name, nil
}

// verifyGitHubReleaseIsComplete downloads checksum file to verify that the release is complete.
func verifyGitHubReleaseIsComplete(releaseName string) error {
	u := fmt.Sprintf(checksumsURL, releaseName)

	resp, err := http.DefaultClient.Get(u)
	if err != nil {
		return errors.Wrap(err, "unable to download releases checksum")
	}

	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		return errors.Errorf("invalid status code from GitHub: %v", resp.StatusCode)
	}

	return nil
}

func maybeCheckForUpdates() (string, error) {
	if v := os.Getenv(checkForUpdatesEnvar); v != "" {
		// see if environment variable is set to false.
		if b, err := strconv.ParseBool(v); err == nil && !b {
			return "", errors.Errorf("update check disabled")
		}
	}

	us, err := getUpdateState()
	if err != nil {
		return "", err
	}

	if time.Now().After(us.NextCheckTime) {
		// before we check for update, write update state file again, so if this fails
		// we won't bother GitHub for a while
		us.NextCheckTime = time.Now().Add(*updateCheckInterval)
		if err = writeUpdateState(us); err != nil {
			return "", errors.Wrap(err, "unable to write update state")
		}

		newAvailableVersion, err := getLatestReleaseNameFromGitHub()
		if err != nil {
			return "", errors.Wrap(err, "update to get latest release from GitHub")
		}

		// we got updated version from GitHub, write it in a state file again
		if newAvailableVersion != us.AvailableVersion {
			if err = verifyGitHubReleaseIsComplete(newAvailableVersion); err != nil {
				return "", errors.Wrap(err, "unable to validate GitHub release")
			}

			us.AvailableVersion = newAvailableVersion

			if err := writeUpdateState(us); err != nil {
				return "", errors.Wrap(err, "unable to write update state")
			}
		}
	}

	if us.AvailableVersion == "" || semver.Compare(repo.BuildVersion, us.AvailableVersion) >= 0 {
		// no new version available
		return "", nil
	}

	if time.Now().After(us.NextNotifyTime) {
		us.NextNotifyTime = time.Now().Add(*updateAvailableNotifyInterval)
		if err := writeUpdateState(us); err != nil {
			return "", errors.Wrap(err, "unable to write update state")
		}

		return us.AvailableVersion, nil
	}

	// no time to notify yet
	return "", nil
}

// maybePrintUpdateNotification prints notification about available version.
func maybePrintUpdateNotification(ctx context.Context) {
	updatedVersion, err := maybeCheckForUpdates()
	if err != nil {
		log(ctx).Debugf("unable to check for updates: %v", err)
		return
	}

	if updatedVersion == "" {
		log(ctx).Debugf("no updated version available")
		return
	}

	noticeColor.Fprintf(os.Stderr, updateAvailableNotice, repo.BuildVersion, updatedVersion) //nolint:errcheck
}
