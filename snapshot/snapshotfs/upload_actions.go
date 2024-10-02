package snapshotfs

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot/policy"
)

const (
	actionCommandTimeout    = 3 * time.Minute
	actionScriptPermissions = 0o700
)

// actionContext carries state between before/after actions.
type actionContext struct {
	ActionsEnabled bool
	SnapshotID     string
	SourcePath     string
	SnapshotPath   string
	WorkDir        string
}

func (hc *actionContext) envars(actionType string) []string {
	return []string{
		fmt.Sprintf("KOPIA_ACTION=%v", actionType),
		fmt.Sprintf("KOPIA_SNAPSHOT_ID=%v", hc.SnapshotID),
		fmt.Sprintf("KOPIA_SOURCE_PATH=%v", hc.SourcePath),
		fmt.Sprintf("KOPIA_SNAPSHOT_PATH=%v", hc.SnapshotPath),
		fmt.Sprintf("KOPIA_VERSION=%v", repo.BuildVersion),
	}
}

func (hc *actionContext) ensureInitialized(ctx context.Context, actionType, dirPathOrEmpty string, uploaderEnabled bool) error {
	if dirPathOrEmpty == "" {
		return nil
	}

	if hc.ActionsEnabled {
		// already initialized
		return nil
	}

	if !uploaderEnabled {
		uploadLog(ctx).Infof("Not executing %v action on %v because it's been disabled for this client.", actionType, dirPathOrEmpty)
		return nil
	}

	var randBytes [8]byte

	if _, err := rand.Read(randBytes[:]); err != nil {
		return errors.Wrap(err, "error reading random bytes")
	}

	hc.SnapshotID = hex.EncodeToString(randBytes[:])
	hc.SourcePath = dirPathOrEmpty
	hc.SnapshotPath = hc.SourcePath

	wd, err := os.MkdirTemp("", "kopia-action")
	if err != nil {
		return errors.Wrap(err, "error temporary directory for action execution")
	}

	hc.WorkDir = wd
	hc.ActionsEnabled = true

	return nil
}

func actionScriptExtension() string {
	if runtime.GOOS == "windows" {
		return ".cmd"
	}

	return ".sh"
}

// prepareCommandForAction prepares *exec.Cmd that will run the provided action command in the provided
// working directory.
func prepareCommandForAction(ctx context.Context, actionType string, h *policy.ActionCommand, workDir string) (*exec.Cmd, context.CancelFunc, error) {
	timeout := actionCommandTimeout
	if h.TimeoutSeconds != 0 {
		timeout = time.Duration(h.TimeoutSeconds) * time.Second
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)

	var c *exec.Cmd

	switch {
	case h.Script != "":
		scriptFile := filepath.Join(workDir, actionType+actionScriptExtension())
		if err := os.WriteFile(scriptFile, []byte(h.Script), actionScriptPermissions); err != nil {
			cancel()

			return nil, nil, errors.Wrap(err, "error writing script for execution")
		}

		switch {
		case runtime.GOOS == "windows":
			c = exec.CommandContext(ctx, os.Getenv("COMSPEC"), "/c", scriptFile) //nolint:gosec
		case strings.HasPrefix(h.Script, "#!"):
			// on unix if a script starts with #!, it will run under designated interpreter
			c = exec.CommandContext(ctx, scriptFile) //nolint:gosec
		default:
			c = exec.CommandContext(ctx, "sh", "-e", scriptFile) //nolint:gosec
		}

	case h.Command != "":
		c = exec.CommandContext(ctx, h.Command, h.Arguments...) //nolint:gosec

	default:
		cancel()

		return nil, nil, errors.New("action did not provide either script nor command to run")
	}

	// all actions run inside temporary working directory
	c.Dir = workDir

	return c, cancel, nil
}

// runActionCommand executes the action command passing the provided inputs as environment
// variables. It analyzes the standard output of the command looking for 'key=value'
// where the key is present in the provided outputs map and sets the corresponding map value.
func runActionCommand(
	ctx context.Context,
	actionType string,
	h *policy.ActionCommand,
	inputs []string,
	captures map[string]string,
	workDir string,
) error {
	cmd, cancel, err := prepareCommandForAction(ctx, actionType, h, workDir)
	if err != nil {
		return errors.Wrap(err, "error preparing command")
	}

	defer cancel()

	cmd.Env = append(os.Environ(), inputs...)
	cmd.Stderr = os.Stderr

	if h.Mode == "async" {
		return errors.Wrap(cmd.Start(), "error starting action command asynchronously")
	}

	v, err := cmd.Output()
	if err != nil {
		if h.Mode == "essential" {
			return errors.Wrap(err, "essential action failed")
		}

		uploadLog(ctx).Errorf("error running non-essential action command: %v", err)
	}

	return parseCaptures(v, captures)
}

// parseCaptures analyzes given byte array and updated the provided map values whenever
// map keys match lines inside the byte array. The lines must be formatted as k=v.
func parseCaptures(v []byte, captures map[string]string) error {
	s := bufio.NewScanner(bytes.NewReader(v))
	for s.Scan() {
		//nolint:mnd
		l := strings.SplitN(s.Text(), "=", 2)
		if len(l) <= 1 {
			continue
		}

		key, value := l[0], l[1]
		if _, ok := captures[key]; ok {
			captures[key] = value
		}
	}

	//nolint:wrapcheck
	return s.Err()
}

func (u *Uploader) executeBeforeFolderAction(ctx context.Context, actionType string, h *policy.ActionCommand, dirPathOrEmpty string, hc *actionContext) (fs.Directory, error) {
	if h == nil {
		return nil, nil
	}

	if err := hc.ensureInitialized(ctx, actionType, dirPathOrEmpty, u.EnableActions); err != nil {
		return nil, errors.Wrap(err, "error initializing action context")
	}

	if !hc.ActionsEnabled {
		return nil, nil
	}

	uploadLog(ctx).Debugf("running action %v on %v %#v", actionType, hc.SourcePath, *h)

	captures := map[string]string{
		"KOPIA_SNAPSHOT_PATH": "",
	}

	if err := runActionCommand(ctx, actionType, h, hc.envars(actionType), captures, hc.WorkDir); err != nil {
		return nil, errors.Wrapf(err, "error running '%v' action", actionType)
	}

	if p := captures["KOPIA_SNAPSHOT_PATH"]; p != "" {
		hc.SnapshotPath = p
		d, err := localfs.Directory(hc.SnapshotPath)

		return d, errors.Wrap(err, "error getting local directory specified in KOPIA_SNAPSHOT_PATH")
	}

	return nil, nil
}

func (u *Uploader) executeAfterFolderAction(ctx context.Context, actionType string, h *policy.ActionCommand, dirPathOrEmpty string, hc *actionContext) {
	if h == nil {
		return
	}

	if err := hc.ensureInitialized(ctx, actionType, dirPathOrEmpty, u.EnableActions); err != nil {
		uploadLog(ctx).Errorf("error initializing action context: %v", err)
	}

	if !hc.ActionsEnabled {
		return
	}

	if err := runActionCommand(ctx, actionType, h, hc.envars(actionType), nil, hc.WorkDir); err != nil {
		uploadLog(ctx).Errorf("error running '%v' action: %v", actionType, err)
	}
}

func cleanupActionContext(ctx context.Context, hc *actionContext) {
	if hc.WorkDir != "" {
		if err := os.RemoveAll(hc.WorkDir); err != nil {
			uploadLog(ctx).Debugf("unable to remove action working directory: %v", err)
		}
	}
}
