//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

package engine

import (
	"fmt"
	"strings"
	"time"

	"github.com/kopia/kopia/internal/clock"
)

// Log keeps track of the actions taken by the engine.
type Log struct {
	runOffset int
	Log       []*LogEntry
}

// LogEntry is an entry for the engine log.
type LogEntry struct {
	StartTime       time.Time
	EndTime         time.Time
	EngineTimestamp int64
	Action          ActionKey
	Error           string
	Idx             int64
	ActionOpts      map[string]string
	CmdOpts         map[string]string
}

func (l *LogEntry) String() string {
	b := &strings.Builder{}

	const timeResol = 100 * time.Millisecond

	fmt.Fprintf(b, "%4v t=%ds %s (%s): %v -> error=%s\n",
		l.Idx,
		l.EngineTimestamp,
		formatTime(l.StartTime),
		l.EndTime.Sub(l.StartTime).Round(timeResol),
		l.Action,
		l.Error,
	)

	return b.String()
}

func formatTime(tm time.Time) string {
	return tm.Format("2006/01/02 15:04:05 MST")
}

// StringThisRun returns a string of only the log entries generated
// by actions in this run of the engine.
func (elog *Log) StringThisRun() string {
	b := &strings.Builder{}

	for _, l := range elog.Log[elog.runOffset:] {
		fmt.Fprint(b, l.String())
	}

	return b.String()
}

func (elog *Log) String() string {
	b := &strings.Builder{}

	fmt.Fprintf(b, "Log size:    %10v\n", len(elog.Log))
	fmt.Fprintf(b, "========\n")

	for _, l := range elog.Log {
		fmt.Fprint(b, l.String())
	}

	return b.String()
}

// AddEntry adds a LogEntry to the Log.
func (elog *Log) AddEntry(l *LogEntry) {
	l.Idx = int64(len(elog.Log))
	elog.Log = append(elog.Log, l)
}

// AddCompleted finalizes a log entry at the time it is called
// and with the provided error, before adding it to the Log.
func (elog *Log) AddCompleted(logEntry *LogEntry, err error) {
	logEntry.EndTime = clock.Now()
	if err != nil {
		logEntry.Error = err.Error()
	}

	elog.AddEntry(logEntry)

	if len(elog.Log) == 0 {
		panic("Did not get added")
	}
}

// FindLast finds the most recent log entry with the provided ActionKey.
func (elog *Log) FindLast(actionKey ActionKey) *LogEntry {
	return elog.findLastUntilIdx(actionKey, 0)
}

// FindLastThisRun finds the most recent log entry with the provided ActionKey,
// limited to the current run only.
func (elog *Log) FindLastThisRun(actionKey ActionKey) (found *LogEntry) {
	return elog.findLastUntilIdx(actionKey, elog.runOffset)
}

func (elog *Log) findLastUntilIdx(actionKey ActionKey, limitIdx int) *LogEntry {
	for i := len(elog.Log) - 1; i >= limitIdx; i-- {
		entry := elog.Log[i]
		if entry != nil && entry.Action == actionKey {
			return entry
		}
	}

	return nil
}

func setLogEntryCmdOpts(l *LogEntry, opts map[string]string) {
	if l == nil {
		return
	}

	l.CmdOpts = opts
}

func (e *Engine) logCompleted(logEntry *LogEntry, err error) {
	e.logMux.Lock()
	defer e.logMux.Unlock()

	e.EngineLog.AddCompleted(logEntry, err)
}
