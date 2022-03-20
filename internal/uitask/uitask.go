package uitask

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/logging"
)

// Status describes the status of UI Task.
type Status string

// Supported task statuses.
const (
	StatusRunning   Status = "RUNNING"
	StatusCanceling Status = "CANCELING"
	StatusCanceled  Status = "CANCELED"
	StatusSuccess   Status = "SUCCESS"
	StatusFailed    Status = "FAILED"
)

// IsFinished returns true if the given status is finished.
func (s Status) IsFinished() bool {
	switch s {
	case StatusCanceled, StatusSuccess, StatusFailed:
		return true
	default:
		return false
	}
}

// LogLevel represents the log level associated with LogEntry.
type LogLevel int

// supported log levels.
const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarning
	LogLevelError
)

// LogEntry contains one output from a single log statement.
type LogEntry struct {
	Timestamp float64  `json:"ts"` // unix timestamp possibly with fractional seconds.
	Module    string   `json:"mod"`
	Level     LogLevel `json:"level"`
	Text      string   `json:"msg"`
}

// Info represents information about a task (running or finished).
type Info struct {
	TaskID       string                  `json:"id"`
	StartTime    time.Time               `json:"startTime"`
	EndTime      *time.Time              `json:"endTime,omitempty"`
	Kind         string                  `json:"kind"` // Maintenance, Snapshot, Restore, etc.
	Description  string                  `json:"description"`
	Status       Status                  `json:"status"`
	ProgressInfo string                  `json:"progressInfo"`
	ErrorMessage string                  `json:"errorMessage,omitempty"`
	Counters     map[string]CounterValue `json:"counters"`
	LogLines     []LogEntry              `json:"-"`
	Error        error                   `json:"-"`

	sequenceNumber int
}

// runningTaskInfo encapsulates running task.
type runningTaskInfo struct {
	Info

	maxLogMessages int // +checklocksignore

	mu sync.Mutex
	// +checklocks:mu
	taskCancel []context.CancelFunc
}

// CurrentTaskID implements the Controller interface.
func (t *runningTaskInfo) CurrentTaskID() string {
	return t.TaskID
}

// OnCancel implements the Controller interface.
func (t *runningTaskInfo) OnCancel(f context.CancelFunc) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.Status != StatusCanceling {
		t.taskCancel = append(t.taskCancel, f)
	} else {
		// already canceled, run the function immediately on a goroutine without holding a lock
		go f()
	}
}

func (t *runningTaskInfo) cancel() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.Status == StatusRunning {
		t.Status = StatusCanceling
		for _, c := range t.taskCancel {
			// run cancelation functions on their own goroutines
			go c()
		}

		t.taskCancel = nil
	}
}

// ReportProgressInfo implements the Controller interface.
func (t *runningTaskInfo) ReportProgressInfo(pi string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.ProgressInfo = pi
}

// ReportCounters implements the Controller interface.
func (t *runningTaskInfo) ReportCounters(c map[string]CounterValue) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.Counters = cloneCounters(c)
}

// info returns a copy of task information while holding a lock.
func (t *runningTaskInfo) info() Info {
	t.mu.Lock()
	defer t.mu.Unlock()

	i := t.Info
	i.Counters = cloneCounters(i.Counters)

	return i
}

func (t *runningTaskInfo) loggerForModule(module string) logging.Logger {
	return runningTaskLogger{t, module}
}

func (t *runningTaskInfo) addLogEntry(module string, level LogLevel, msg string, args []interface{}) {
	// do not store noisy output from format log.
	if module == content.FormatLogModule {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.Status.IsFinished() {
		return
	}

	t.LogLines = append(t.LogLines, LogEntry{
		Timestamp: float64(clock.Now().UnixNano()) / 1e9,
		Level:     level,
		Module:    module,
		Text:      fmt.Sprintf(msg, args...),
	})
	if len(t.LogLines) > t.maxLogMessages {
		t.LogLines = t.LogLines[1:]
	}
}

func (t *runningTaskInfo) log() []LogEntry {
	t.mu.Lock()
	defer t.mu.Unlock()

	return append([]LogEntry(nil), t.LogLines...)
}

type runningTaskLogger struct {
	r      *runningTaskInfo
	module string
}

func (l runningTaskLogger) Debugf(msg string, args ...interface{}) {
	l.r.addLogEntry(l.module, LogLevelDebug, msg, args)
}

func (l runningTaskLogger) Debugw(msg string, keyValuePairs ...interface{}) {
	l.r.addLogEntry(l.module, LogLevelDebug, logging.DebugMessageWithKeyValuePairs(msg, keyValuePairs), nil)
}

func (l runningTaskLogger) Infof(msg string, args ...interface{}) {
	l.r.addLogEntry(l.module, LogLevelInfo, msg, args)
}

func (l runningTaskLogger) Warnf(msg string, args ...interface{}) {
	l.r.addLogEntry(l.module, LogLevelWarning, msg, args)
}

func (l runningTaskLogger) Errorf(msg string, args ...interface{}) {
	l.r.addLogEntry(l.module, LogLevelError, msg, args)
}

var _ logging.Logger = runningTaskLogger{}
