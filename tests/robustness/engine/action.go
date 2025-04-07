//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

package engine

import (
	"bytes"
	"context"
	"errors"
	"log"
	"math/rand"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/tests/robustness"
)

// ExecAction executes the action denoted by the provided ActionKey.
func (e *Engine) ExecAction(ctx context.Context, actionKey ActionKey, opts map[string]string) (map[string]string, error) {
	if opts == nil {
		opts = make(map[string]string)
	}

	e.statsIncrActionCountAndLog(actionKey)

	action := actions[actionKey]
	st := clock.Now()

	logEntry := &LogEntry{
		StartTime:       st,
		EngineTimestamp: e.getTimestampS(),
		Action:          actionKey,
		ActionOpts:      opts,
	}

	// Execute the action n times
	err := robustness.ErrNoOp // Default to no-op error

	// TODO: return more than the last output
	var out map[string]string

	n := robustness.GetOptAsIntOrDefault(ActionRepeaterField, opts, defaultActionRepeats)
	for range n {
		out, err = action.f(ctx, e, opts, logEntry)
		if err != nil {
			break
		}
	}

	// If error was just a no-op, don't bother logging the action
	switch {
	case errors.Is(err, robustness.ErrNoOp):
		e.statsUpdateCounters(statsIncrNoOp)

		return out, err

	case err != nil:
		log.Printf("error=%q", err.Error())
	}

	e.statsUpdatePerAction(actionKey, st, err)
	e.logCompleted(logEntry, err)

	return out, err
}

// RandomAction executes a random action picked by the relative weights given
// in actionOpts[ActionControlActionKey], or uniform probability if that
// key is not present in the input options.
func (e *Engine) RandomAction(ctx context.Context, actionOpts ActionOpts) error {
	actionControlOpts := actionOpts.getActionControlOpts()

	actionName := pickActionWeighted(actionControlOpts, actions)
	if string(actionName) == "" {
		return robustness.ErrNoActionPicked
	}

	_, err := e.ExecAction(ctx, actionName, actionOpts[actionName])
	err = e.CheckErrRecovery(ctx, err, actionOpts)

	return err
}

// CheckErrRecovery tries to recover from no space left error
// by deleting data directories.
func (e *Engine) CheckErrRecovery(ctx context.Context, incomingErr error, actionOpts ActionOpts) (outgoingErr error) {
	outgoingErr = incomingErr

	if incomingErr == nil {
		return nil
	}

	ctrl := actionOpts.getActionControlOpts()

	if errIsNotEnoughSpace(incomingErr) && ctrl[ThrowNoSpaceOnDeviceErrField] == "" {
		// no space left on device
		// Delete everything in the data directory
		outgoingErr = e.FileWriter.DeleteEverything(ctx)
		if outgoingErr != nil {
			return outgoingErr
		}

		e.statsUpdateCounters(statsIncrDataPurge)

		// Restore a previous snapshot to the data directory
		restoreActionKey := RestoreIntoDataDirectoryActionKey
		_, outgoingErr = e.ExecAction(ctx, restoreActionKey, actionOpts[restoreActionKey])

		if errors.Is(outgoingErr, robustness.ErrNoOp) {
			outgoingErr = nil
		} else {
			e.statsUpdateCounters(statsIncrDataRestore)
		}
	}

	if outgoingErr == nil {
		e.statsUpdateCounters(statsIncrErrorRecovery)
	}

	return outgoingErr
}

// List of action keys.
const (
	ActionControlActionKey            ActionKey = "action-control"
	SnapshotDirActionKey              ActionKey = "snapshot-root"
	RestoreSnapshotActionKey          ActionKey = "restore-random-snapID"
	DeleteRandomSnapshotActionKey     ActionKey = "delete-random-snapID"
	WriteRandomFilesActionKey         ActionKey = "write-random-files"
	DeleteRandomSubdirectoryActionKey ActionKey = "delete-random-subdirectory"
	DeleteDirectoryContentsActionKey  ActionKey = "delete-files"
	RestoreIntoDataDirectoryActionKey ActionKey = "restore-into-data-dir"
	GCActionKey                       ActionKey = "run-gc"
)

// ActionOpts is a structure that designates the options for
// picking and running an action.
type ActionOpts map[ActionKey]map[string]string

func (actionOpts ActionOpts) getActionControlOpts() map[string]string {
	if actionOpts != nil && actionOpts[ActionControlActionKey] != nil {
		return actionOpts[ActionControlActionKey]
	}

	return defaultActionControls()
}

// Action is a unit of functionality that can be executed by
// the engine.
type Action struct {
	f func(ctx context.Context, eng *Engine, opts map[string]string, l *LogEntry) (out map[string]string, err error)
}

// ActionKey refers to an action that can be executed by the engine.
type ActionKey string

var actions = map[ActionKey]Action{
	SnapshotDirActionKey:              {f: snapshotDirAction},
	RestoreSnapshotActionKey:          {f: restoreSnapshotAction},
	DeleteRandomSnapshotActionKey:     {f: deleteRandomSnapshotAction},
	GCActionKey:                       {f: gcAction},
	WriteRandomFilesActionKey:         {f: writeRandomFilesAction},
	DeleteRandomSubdirectoryActionKey: {f: deleteRandomSubdirectoryAction},
	DeleteDirectoryContentsActionKey:  {f: deleteDirectoryContentsAction},
	RestoreIntoDataDirectoryActionKey: {f: restoreIntoDataDirectoryAction},
}

func snapshotDirAction(ctx context.Context, e *Engine, opts map[string]string, l *LogEntry) (out map[string]string, err error) {
	snapPath := e.FileWriter.DataDirectory(ctx)
	if opts != nil && opts[SubPathOptionName] != "" {
		snapPath = filepath.Join(snapPath, opts[SubPathOptionName])
	}

	log.Printf("Creating snapshot of directory %s", snapPath)

	snapID, err := e.Checker.TakeSnapshot(ctx, snapPath, opts)

	setLogEntryCmdOpts(l, map[string]string{
		"snap-dir": snapPath,
		"snapID":   snapID,
	})

	return map[string]string{
		SnapshotIDField: snapID,
	}, err
}

func restoreSnapshotAction(ctx context.Context, e *Engine, opts map[string]string, l *LogEntry) (out map[string]string, err error) {
	snapID, err := e.getSnapIDOptOrRandLive(opts)
	if err != nil {
		return nil, err
	}

	setLogEntryCmdOpts(l, map[string]string{"snapID": snapID})

	log.Printf("Restoring snapshot %s", snapID)

	b := &bytes.Buffer{}

	err = e.Checker.RestoreSnapshot(ctx, snapID, b, opts)
	if err != nil {
		log.Print(b.String())
	}

	return nil, err
}

func deleteRandomSnapshotAction(ctx context.Context, e *Engine, opts map[string]string, l *LogEntry) (out map[string]string, err error) {
	// Do not delete snapshot when it is the only available snapshot.
	// This will ensure that the repository under test in robustness tests
	// grows over long term.
	snapIDList := e.Checker.GetLiveSnapIDs()
	if len(snapIDList) <= 1 {
		log.Println("No snapshots available for deletion")

		return nil, robustness.ErrNoOp
	}

	snapID, err := e.getSnapIDOptOrRandLive(opts)
	if err != nil {
		return nil, err
	}

	log.Printf("Deleting snapshot %s", snapID)

	setLogEntryCmdOpts(l, map[string]string{"snapID": snapID})

	return nil, e.Checker.DeleteSnapshot(ctx, snapID, opts)
}

func gcAction(ctx context.Context, e *Engine, opts map[string]string, l *LogEntry) (out map[string]string, err error) {
	return nil, e.TestRepo.RunGC(ctx, opts)
}

func writeRandomFilesAction(ctx context.Context, e *Engine, opts map[string]string, l *LogEntry) (out map[string]string, err error) {
	out, err = e.FileWriter.WriteRandomFiles(ctx, opts)
	setLogEntryCmdOpts(l, out)

	return
}

func deleteRandomSubdirectoryAction(ctx context.Context, e *Engine, opts map[string]string, l *LogEntry) (out map[string]string, err error) {
	out, err = e.FileWriter.DeleteRandomSubdirectory(ctx, opts)
	setLogEntryCmdOpts(l, out)

	return
}

func deleteDirectoryContentsAction(ctx context.Context, e *Engine, opts map[string]string, l *LogEntry) (out map[string]string, err error) {
	out, err = e.FileWriter.DeleteDirectoryContents(ctx, opts)
	setLogEntryCmdOpts(l, out)

	return
}

func restoreIntoDataDirectoryAction(ctx context.Context, e *Engine, opts map[string]string, l *LogEntry) (out map[string]string, err error) {
	snapID, err := e.getSnapIDOptOrRandLive(opts)
	if err != nil {
		return nil, err
	}

	log.Printf("Restoring snap ID %v into data directory\n", snapID)

	setLogEntryCmdOpts(l, map[string]string{"snapID": snapID})

	b := &bytes.Buffer{}

	if err := e.Checker.RestoreSnapshotToPath(ctx, snapID, e.FileWriter.DataDirectory(ctx), b, opts); err != nil {
		log.Print(b.String())

		return nil, err //nolint:wrapcheck
	}

	return nil, nil
}

// Action constants.
const (
	defaultActionRepeats = 1
)

// Option field names.
const (
	ActionRepeaterField          = "repeat-action"
	ThrowNoSpaceOnDeviceErrField = "throw-no-space-error"
	SnapshotIDField              = "snapshot-ID"
	SubPathOptionName            = "sub-path"
)

func defaultActionControls() map[string]string {
	ret := make(map[string]string, len(actions))

	for actionKey := range actions {
		switch actionKey {
		case RestoreIntoDataDirectoryActionKey:
			// Don't restore into data directory by default
			ret[string(actionKey)] = strconv.Itoa(0)
		default:
			ret[string(actionKey)] = strconv.Itoa(1)
		}
	}

	return ret
}

func pickActionWeighted(actionControlOpts map[string]string, actionList map[ActionKey]Action) ActionKey {
	var keepKey ActionKey

	sum := 0

	for actionName := range actionList {
		weight := robustness.GetOptAsIntOrDefault(string(actionName), actionControlOpts, 0)
		if weight == 0 {
			continue
		}

		sum += weight
		if rand.Intn(sum) < weight { //nolint:gosec
			keepKey = actionName
		}
	}

	return keepKey
}

func errIsNotEnoughSpace(err error) bool {
	return errors.Is(err, robustness.ErrCannotPerformIO) || strings.Contains(err.Error(), noSpaceOnDeviceMatchStr)
}

func (e *Engine) getSnapIDOptOrRandLive(opts map[string]string) (snapID string, err error) {
	snapID = opts[SnapshotIDField]
	if snapID != "" {
		return snapID, nil
	}

	snapIDList := e.Checker.GetLiveSnapIDs()
	if len(snapIDList) == 0 {
		log.Println("No snapshots available for deletion")

		return "", robustness.ErrNoOp
	}

	return snapIDList[rand.Intn(len(snapIDList))], nil //nolint:gosec
}
