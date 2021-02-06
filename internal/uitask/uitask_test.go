package uitask_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/uitask"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/logging"
)

var (
	log        = logging.GetContextLoggerFunc("uitasktest")
	ignoredLog = logging.GetContextLoggerFunc(content.FormatLogModule)
)

func TestUITask(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	m := uitask.NewManager()

	m.MaxLogMessagesPerTask = 3
	m.MaxFinishedTasks = 3

	verifyTaskList(t, m, nil)

	var tid1a, tid1, tid2, tid3, tid4, tid5 string

	m.Run(ctx, "some-kind", "test-1", func(ctx context.Context, ctrl uitask.Controller) error {
		tid1a = ctrl.CurrentTaskID()

		tsk, ok := m.GetTask(tid1a)
		if !ok {
			t.Fatalf("task not found")
		}

		if got, want := tsk.Description, "test-1"; got != want {
			t.Fatalf("invalid task description %v, want %v", got, want)
		}

		if got, want := tsk.Status, uitask.StatusRunning; got != want {
			t.Fatalf("invalid task status %v, want %v", got, want)
		}

		verifyTaskList(t, m, map[string]uitask.Status{
			tid1a: uitask.StatusRunning,
		})

		verifyTaskLog(t, m, tid1a, nil)
		log(ctx).Debugf("first")
		ignoredLog(ctx).Debugf("this is ignored")
		log(ctx).Infof("iii")
		verifyTaskLog(t, m, tid1a, []string{
			"first",
			"iii",
		})
		log(ctx).Warningf("www")
		log(ctx).Errorf("eee")

		// 'first' has aged out
		verifyTaskLog(t, m, tid1a, []string{
			"iii",
			"www",
			"eee",
		})

		log(ctx).Noticef("nnn")
		log(ctx).Fatalf("fff")
		verifyTaskLog(t, m, tid1a, []string{
			"eee",
			"nnn",
			"fff",
		})

		return nil
	})

	tid1 = getTaskID(t, m, "test-1")
	if tid1 != tid1a {
		t.Fatalf("task ID has changed after completion: %v vs %v", tid1, tid1a)
	}

	tsk, ok := m.GetTask(tid1)
	if !ok {
		t.Fatalf("task not found")
	}

	if got, want := tsk.Description, "test-1"; got != want {
		t.Fatalf("invalid task description %v, want %v", got, want)
	}

	if got, want := tsk.Status, uitask.StatusSuccess; got != want {
		t.Fatalf("invalid task status %v, want %v", got, want)
	}

	// get non-existent task.
	if _, ok := m.GetTask(uuid.New().String()); ok {
		t.Fatalf("task unexpectedly found")
	}

	verifyTaskLog(t, m, uuid.New().String(), nil)

	// task log still available after task finished.
	verifyTaskLog(t, m, tid1, []string{
		"eee",
		"nnn",
		"fff",
	})

	m.Run(ctx, "some-kind", "test-2", func(ctx context.Context, ctrl uitask.Controller) error {
		tid2 = ctrl.CurrentTaskID()
		verifyTaskList(t, m, map[string]uitask.Status{
			tid1: uitask.StatusSuccess,
			tid2: uitask.StatusRunning,
		})

		return nil
	})

	m.Run(ctx, "some-kind", "test-3", func(ctx context.Context, ctrl uitask.Controller) error {
		tid3 = ctrl.CurrentTaskID()
		verifyTaskList(t, m, map[string]uitask.Status{
			tid1: uitask.StatusSuccess,
			tid2: uitask.StatusSuccess,
			tid3: uitask.StatusRunning,
		})

		if diff := cmp.Diff(m.TaskSummary(), map[uitask.Status]int{
			uitask.StatusRunning: 1,
			uitask.StatusSuccess: 2,
		}); diff != "" {
			t.Fatalf("unexpected summary: %v", diff)
		}

		return errors.Errorf("some error")
	})

	verifyTaskList(t, m, map[string]uitask.Status{
		tid1: uitask.StatusSuccess,
		tid2: uitask.StatusSuccess,
		tid3: uitask.StatusFailed,
	})

	m.Run(ctx, "some-kind", "test-4", func(ctx context.Context, ctrl uitask.Controller) error {
		tid4 = ctrl.CurrentTaskID()
		return nil
	})

	// test-1 is aged out
	verifyTaskList(t, m, map[string]uitask.Status{
		tid2: uitask.StatusSuccess,
		tid3: uitask.StatusFailed,
		tid4: uitask.StatusSuccess,
	})

	m.Run(ctx, "some-kind", "test-5", func(ctx context.Context, ctrl uitask.Controller) error {
		tid5 = ctrl.CurrentTaskID()
		return nil
	})

	// test-2 is aged out
	verifyTaskList(t, m, map[string]uitask.Status{
		tid3: uitask.StatusFailed,
		tid4: uitask.StatusSuccess,
		tid5: uitask.StatusSuccess,
	})

	if diff := cmp.Diff(m.TaskSummary(), map[uitask.Status]int{
		uitask.StatusRunning: 0,
		uitask.StatusFailed:  1,
		uitask.StatusSuccess: 2,
	}); diff != "" {
		t.Fatalf("unexpected summary: %v", diff)
	}
}

func verifyTaskList(t *testing.T, m *uitask.Manager, wantStatuses map[string]uitask.Status) {
	t.Helper()

	tasks := m.ListTasks()
	if got, want := len(tasks), len(wantStatuses); got != want {
		t.Fatalf("invalid task list length: %v, want %v", got, want)
	}

	for taskID, wantStatus := range wantStatuses {
		if got := mustFindTask(t, tasks, taskID).Status; got != wantStatus {
			t.Fatalf("task %v status was %v, wanted %v", taskID, got, wantStatus)
		}
	}
}

func TestUITaskCancel_AfterOnCancel(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	m := uitask.NewManager()

	ch := make(chan string)

	go func() {
		childTaskID := <-ch

		time.Sleep(time.Second)
		m.CancelTask(childTaskID)
	}()

	m.Run(ctx, "some-kind", "test-1", func(ctx context.Context, ctrl uitask.Controller) error {
		// send my task ID to the goroutine which will cancel our task
		ch <- ctrl.CurrentTaskID()
		canceled := make(chan struct{})
		ctrl.OnCancel(func() {
			close(canceled)
		})

		<-canceled

		return nil
	})
}

func TestUITaskCancel_BeforeOnCancel(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	m := uitask.NewManager()

	ch := make(chan string)

	go func() {
		childTaskID := <-ch
		m.CancelTask(childTaskID)
	}()

	m.Run(ctx, "some-kind", "test-1", func(ctx context.Context, ctrl uitask.Controller) error {
		// send my task ID to the goroutine which will cancel our task
		ch <- ctrl.CurrentTaskID()
		time.Sleep(1 * time.Second)
		canceled := make(chan struct{})
		ctrl.OnCancel(func() {
			close(canceled)
		})

		<-canceled

		return nil
	})
}

func getTaskID(t *testing.T, m *uitask.Manager, desc string) string {
	t.Helper()

	// nolint:gocritic
	for _, tsk := range m.ListTasks() {
		if tsk.Description == desc {
			return tsk.TaskID
		}
	}

	t.Fatalf("task with description %v was not found", desc)

	return ""
}

func verifyTaskLog(t *testing.T, m *uitask.Manager, taskID string, want []string) {
	t.Helper()

	if got, want := logText(m.TaskLog(taskID)), strings.Join(want, "\n"); got != want {
		t.Fatalf("invalid task log %v, want %v", got, want)
	}
}

func logText(items []uitask.LogEntry) string {
	var result []string

	for _, it := range items {
		result = append(result, it.Text)
	}

	return strings.Join(result, "\n")
}

func mustFindTask(t *testing.T, tasks []uitask.Info, tid string) uitask.Info {
	t.Helper()

	// nolint:gocritic
	for _, tsk := range tasks {
		if tsk.TaskID == tid {
			return tsk
		}
	}

	t.Fatalf("task %v not found among %v", tid, tasks)

	return uitask.Info{}
}
