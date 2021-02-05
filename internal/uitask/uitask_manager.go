// Package uitask provided management of in-process long-running tasks that are exposed to the UI.
package uitask

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/repo/logging"
)

const (
	maxFinishedTasks      = 50
	maxLogMessagesPerTask = 1000
)

// Manager manages UI tasks.
type Manager struct {
	mu         sync.Mutex
	nextTaskID int
	running    map[string]*runningTaskInfo
	finished   map[string]*Info

	MaxFinishedTasks      int
	MaxLogMessagesPerTask int
}

// Controller allows the task to communicate with task manager and receive signals.
type Controller interface {
	CurrentTaskID() string
	OnCancel(cancelFunc context.CancelFunc)
}

// TaskFunc represents a task function.
type TaskFunc func(ctx context.Context, ctrl Controller) error

// Run executes the provided task in the current goroutine while allowing it to be externally examined and canceled.
func (m *Manager) Run(ctx context.Context, kind, description string, task TaskFunc) error {
	r := &runningTaskInfo{
		Info: Info{
			Kind:        kind,
			Description: description,
			Status:      StatusRunning,
		},
		maxLogMessages: m.MaxLogMessagesPerTask,
	}

	ctx = logging.WithLogger(ctx, r.loggerForModule)

	m.startTask(r)

	err := task(ctx, r)
	m.completeTask(r, err)

	return err
}

// ListTasks lists all running and some recently-finished tasks up to configured limits.
func (m *Manager) ListTasks() []Info {
	m.mu.Lock()
	defer m.mu.Unlock()

	var res []Info

	for _, v := range m.running {
		res = append(res, v.Info)
	}

	for _, v := range m.finished {
		res = append(res, *v)
	}

	// most recent first
	sort.Slice(res, func(i, j int) bool {
		return res[i].StartTime.After(res[j].StartTime)
	})

	return res
}

// TaskSummary returns the summary (number of tasks by status).
func (m *Manager) TaskSummary() map[Status]int {
	m.mu.Lock()
	defer m.mu.Unlock()

	s := map[Status]int{
		StatusRunning: len(m.running),
	}

	for _, v := range m.finished {
		s[v.Status]++
	}

	return s
}

// TaskLog retrieves the log from the task.
func (m *Manager) TaskLog(taskID string) []LogEntry {
	m.mu.Lock()
	defer m.mu.Unlock()

	if r := m.running[taskID]; r != nil {
		return r.log()
	}

	if f, ok := m.finished[taskID]; ok {
		return append([]LogEntry(nil), f.LogLines...)
	}

	return nil
}

// GetTask retrieves the task info.
func (m *Manager) GetTask(taskID string) (Info, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if r := m.running[taskID]; r != nil {
		return r.Info, true
	}

	if f, ok := m.finished[taskID]; ok {
		return *f, true
	}

	return Info{}, false
}

// CancelTask retrieves the log from the task.
func (m *Manager) CancelTask(taskID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	t := m.running[taskID]
	if t == nil {
		return
	}

	t.cancel()
}

func (m *Manager) startTask(r *runningTaskInfo) string {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.nextTaskID++

	taskID := fmt.Sprintf("%x", m.nextTaskID)
	r.StartTime = clock.Now()
	r.TaskID = taskID
	m.running[taskID] = r

	return taskID
}

func (m *Manager) completeTask(r *runningTaskInfo, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.Status != StatusCanceled {
		if err != nil {
			r.Status = StatusFailed
			r.ErrorMessage = err.Error()
		} else {
			r.Status = StatusSuccess
		}
	}

	now := clock.Now()
	r.EndTime = &now

	delete(m.running, r.TaskID)
	m.finished[r.TaskID] = &r.Info

	// delete oldest finished tasks up to configured limit.
	for len(m.finished) > m.MaxFinishedTasks {
		var (
			oldestStartTime time.Time
			oldestID        string
		)

		for _, v := range m.finished {
			if oldestStartTime.IsZero() || v.StartTime.Before(oldestStartTime) {
				oldestStartTime = v.StartTime
				oldestID = v.TaskID
			}
		}

		delete(m.finished, oldestID)
	}
}

// NewManager creates new UI Task Manager.
func NewManager() *Manager {
	return &Manager{
		running:  map[string]*runningTaskInfo{},
		finished: map[string]*Info{},

		MaxLogMessagesPerTask: maxLogMessagesPerTask,
		MaxFinishedTasks:      maxFinishedTasks,
	}
}
