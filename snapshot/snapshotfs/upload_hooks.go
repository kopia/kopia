package snapshotfs

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/snapshot/policy"
)

const (
	hookCommandTimeout    = 3 * time.Minute
	hookScriptPermissions = 0o700
)

// hookContext carries state between before/after hooks.
type hookContext struct {
	HooksEnabled bool
	SnapshotID   string
	SourcePath   string
	SnapshotPath string
	WorkDir      string
}

func (hc *hookContext) envars() []string {
	return []string{
		fmt.Sprintf("KOPIA_SNAPSHOT_ID=%v", hc.SnapshotID),
		fmt.Sprintf("KOPIA_SOURCE_PATH=%v", hc.SourcePath),
		fmt.Sprintf("KOPIA_SNAPSHOT_PATH=%v", hc.SnapshotPath),
	}
}

func (hc *hookContext) ensureInitialized(dirPathOrEmpty string) error {
	if dirPathOrEmpty == "" {
		return nil
	}

	if hc.HooksEnabled {
		// already initialized
		return nil
	}

	var randBytes [8]byte

	if _, err := rand.Read(randBytes[:]); err != nil {
		return errors.Wrap(err, "error reading random bytes")
	}

	hc.SnapshotID = fmt.Sprintf("%x", randBytes[:])
	hc.SourcePath = dirPathOrEmpty
	hc.SnapshotPath = hc.SourcePath

	wd, err := ioutil.TempDir("", "kopia-hook")
	if err != nil {
		return err
	}

	hc.WorkDir = wd
	hc.HooksEnabled = true

	return nil
}

func hookScriptExtension() string {
	if runtime.GOOS == "windows" {
		return ".cmd"
	}

	return ".sh"
}

// prepareCommandForHook prepares *exec.Cmd that will run the provided hook command in the provided
// working directory.
func prepareCommandForHook(ctx context.Context, hookType string, h *policy.HookCommand, workDir string) (*exec.Cmd, context.CancelFunc, error) {
	timeout := hookCommandTimeout
	if h.TimeoutSeconds != 0 {
		timeout = time.Duration(h.TimeoutSeconds) * time.Second
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)

	var c *exec.Cmd

	switch {
	case h.Script != "":
		scriptFile := filepath.Join(workDir, hookType+hookScriptExtension())
		if err := ioutil.WriteFile(scriptFile, []byte(h.Script), hookScriptPermissions); err != nil {
			cancel()

			return nil, nil, err
		}

		if runtime.GOOS == "windows" {
			c = exec.CommandContext(ctx, "cmd.exe", "/c", scriptFile) // nolint:gosec
		} else {
			// on unix the script must contain #!/bin/sh which will cause it to run under a shell
			c = exec.CommandContext(ctx, scriptFile) // nolint:gosec
		}

	case h.Command != "":
		c = exec.CommandContext(ctx, h.Command, h.Arguments...) // nolint:gosec

	default:
		cancel()

		return nil, nil, errors.Errorf("hook did not provide either script nor command to run")
	}

	// all hooks run inside temporary working directory
	c.Dir = workDir

	return c, cancel, nil
}

// runHookCommand executes the hook command passing the provided inputs as environment
// variables. It analyzes the standard output of the command looking for 'key=value'
// where the key is present in the provided outputs map and sets the corresponding map value.
func runHookCommand(
	ctx context.Context,
	hookType string,
	h *policy.HookCommand,
	inputs []string,
	captures map[string]string,
	workDir string,
) error {
	cmd, cancel, err := prepareCommandForHook(ctx, hookType, h, workDir)
	if err != nil {
		return errors.Wrap(err, "error preparing command")
	}

	defer cancel()

	cmd.Env = append(append([]string(nil), os.Environ()...), inputs...)
	cmd.Stderr = os.Stderr

	if h.Mode == "async" {
		return cmd.Start()
	}

	v, err := cmd.Output()
	if err != nil {
		if h.Mode == "essential" {
			return err
		}

		log(ctx).Warningf("error running non-essential hook command: %v", err)
	}

	return parseCaptures(v, captures)
}

// parseCaptures analyzes given byte array and updated the provided map values whenever
// map keys match lines inside the byte array. The lines must be formatted as k=v.
func parseCaptures(v []byte, captures map[string]string) error {
	s := bufio.NewScanner(bytes.NewReader(v))
	for s.Scan() {
		l := strings.SplitN(s.Text(), "=", 2)
		if len(l) <= 1 {
			continue
		}

		key, value := l[0], l[1]
		if _, ok := captures[key]; ok {
			captures[key] = value
		}
	}

	return s.Err()
}

func executeBeforeFolderHook(ctx context.Context, hookType string, h *policy.HookCommand, dirPathOrEmpty string, hc *hookContext) (fs.Directory, error) {
	if h == nil {
		return nil, nil
	}

	if err := hc.ensureInitialized(dirPathOrEmpty); err != nil {
		return nil, errors.Wrap(err, "error initializing hook context")
	}

	if !hc.HooksEnabled {
		return nil, nil
	}

	log(ctx).Debugf("running hook %v on %v %#v", hookType, hc.SourcePath, *h)

	captures := map[string]string{
		"KOPIA_SNAPSHOT_PATH": "",
	}

	if err := runHookCommand(ctx, hookType, h, hc.envars(), captures, hc.WorkDir); err != nil {
		return nil, errors.Wrapf(err, "error running '%v' hook", hookType)
	}

	if p := captures["KOPIA_SNAPSHOT_PATH"]; p != "" {
		hc.SnapshotPath = p
		return localfs.Directory(hc.SnapshotPath)
	}

	return nil, nil
}

func executeAfterFolderHook(ctx context.Context, hookType string, h *policy.HookCommand, dirPathOrEmpty string, hc *hookContext) {
	if h == nil {
		return
	}

	if err := hc.ensureInitialized(dirPathOrEmpty); err != nil {
		log(ctx).Warningf("error initializing hook context: %v", err)
	}

	if !hc.HooksEnabled {
		return
	}

	if err := runHookCommand(ctx, hookType, h, hc.envars(), nil, hc.WorkDir); err != nil {
		log(ctx).Warningf("error running '%v' hook: %v", hookType, err)
	}
}

func cleanupHookContext(ctx context.Context, hc *hookContext) {
	if hc.WorkDir != "" {
		if err := os.RemoveAll(hc.WorkDir); err != nil {
			log(ctx).Debugf("unable to remove hook working directory: %v", err)
		}
	}
}
