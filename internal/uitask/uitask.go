package uitask

import (
	"bytes"
	"context"
	"encoding/json"
	"sync"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

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
	LogLines     []json.RawMessage       `json:"-"`
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
			// run cancellation functions on their own goroutines
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

// those are JSON keys expected by the UI log viewer.
const (
	uiLogTimeKey    = "ts"
	uiLogModuleKey  = "mod"
	uiLogMessageKey = "msg"
	uiLogLevelKey   = "level"
)

func (t *runningTaskInfo) loggerForModule(module string) logging.Logger {
	return zap.New(
		zapcore.NewCore(
			zapcore.NewJSONEncoder(zapcore.EncoderConfig{
				TimeKey:        uiLogTimeKey,
				LevelKey:       uiLogLevelKey,
				NameKey:        uiLogModuleKey,
				MessageKey:     uiLogMessageKey,
				LineEnding:     zapcore.DefaultLineEnding,
				EncodeName:     zapcore.FullNameEncoder,
				EncodeLevel:    uiLevelEncoder,
				EncodeTime:     zapcore.EpochTimeEncoder,
				EncodeDuration: zapcore.StringDurationEncoder,
				EncodeCaller:   zapcore.ShortCallerEncoder,
				SkipLineEnding: true,
			}),
			t,
			zapcore.DebugLevel,
		),
	).Sugar().Named(module)
}

func uiLevelEncoder(l zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
	switch l {
	case zapcore.InfoLevel:
		enc.AppendInt(int(LogLevelInfo))
	case zapcore.WarnLevel:
		enc.AppendInt(int(LogLevelWarning))
	case zapcore.ErrorLevel:
		enc.AppendInt(int(LogLevelError))
	default:
		enc.AppendInt(int(LogLevelDebug))
	}
}

//nolint:gochecknoglobals
var containsFormatLogModule = []byte(`"` + uiLogModuleKey + `":"` + content.FormatLogModule + `"`)

func (t *runningTaskInfo) addLogEntry(le json.RawMessage) {
	// do not store noisy output from format log.
	// compare JSON bytes to avoid having to parse.
	if bytes.Contains(le, containsFormatLogModule) {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.Status.IsFinished() {
		return
	}

	t.LogLines = append(t.LogLines, le)
	if len(t.LogLines) > t.maxLogMessages {
		t.LogLines = t.LogLines[1:]
	}
}

func (t *runningTaskInfo) log() []json.RawMessage {
	t.mu.Lock()
	defer t.mu.Unlock()

	return append([]json.RawMessage(nil), t.LogLines...)
}

func (t *runningTaskInfo) Write(p []byte) (int, error) {
	n := len(p)

	p = append([]byte(nil), p...)

	t.addLogEntry(json.RawMessage(p))

	return n, nil
}

func (t *runningTaskInfo) Sync() error {
	return nil
}
