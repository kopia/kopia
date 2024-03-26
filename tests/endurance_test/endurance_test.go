package endurance_test

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/webdav"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/faketime"
	"github.com/kopia/kopia/tests/testdirtree"
	"github.com/kopia/kopia/tests/testenv"
)

const (
	maxSourcesPerEnduranceRunner = 3
	enduranceRunnerCount         = 3
)

var (
	// We will simulate 2 weeks of running with clock moving faster than usual.
	startTime         = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	simulatedDuration = 14 * 24 * time.Hour
	endTime           = startTime.Add(simulatedDuration)
)

type webdavDirWithFakeClock struct {
	webdav.Dir

	fts *testenv.FakeTimeServer
}

func (d webdavDirWithFakeClock) OpenFile(ctx context.Context, fname string, flags int, mode os.FileMode) (webdav.File, error) {
	f, err := d.Dir.OpenFile(ctx, fname, flags, mode)
	if err != nil {
		return nil, err
	}

	if flags&os.O_RDONLY != 0 {
		// file was readonly
		return f, nil
	}

	// change file time after creation to simulate fake time scale.
	osf := f.(*os.File)
	now := d.fts.Now()

	if err := os.Chtimes(osf.Name(), now, now); err != nil {
		log.Printf("unable to change file time: %v", err)
	}

	return f, nil
}

func TestEndurance(t *testing.T) {
	runner := testenv.NewExeRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	tmpDir, err := os.MkdirTemp("", "endurance")
	if err != nil {
		t.Fatalf("unable to get temp dir: %v", err)
	}

	defer os.RemoveAll(tmpDir)

	testTime := faketime.NewClockTimeWithOffset(startTime.Sub(clock.Now()))
	fts := testenv.NewFakeTimeServer(testTime.NowFunc())

	ft := httptest.NewServer(fts)
	defer ft.Close()

	e.Environment["KOPIA_FAKE_CLOCK_ENDPOINT"] = ft.URL

	sts := httptest.NewServer(&webdav.Handler{
		FileSystem: webdavDirWithFakeClock{webdav.Dir(tmpDir), fts},
		LockSystem: webdav.NewMemLS(),
	})
	defer sts.Close()

	e.RunAndExpectSuccess(t, "repo", "create", "webdav", "--url", sts.URL)

	var failureCount atomic.Int32

	rwMutex := &sync.RWMutex{}

	t.Run("Runners", func(t *testing.T) {
		for i := range enduranceRunnerCount {
			t.Run(fmt.Sprintf("Runner-%v", i), func(t *testing.T) {
				t.Parallel()
				defer func() {
					if t.Failed() {
						failureCount.Add(1)
					}
				}()

				enduranceRunner(t, i, ft.URL, sts.URL, &failureCount, rwMutex, testTime)
			})
		}
	})

	e.RunAndExpectSuccess(t, "snapshot", "list", "-a", "-i")
	e.RunAndExpectSuccess(t, "blob", "list")
}

type runnerState struct {
	dirs                []string
	snapshottedAnything bool
	runnerID            int
	fakeClock           *faketime.ClockTimeWithOffset
}

type action func(t *testing.T, e *testenv.CLITest, s *runnerState)

type actionInfo struct {
	name      string
	act       action
	weight    int
	exclusive bool
}

// actions is a set of actions by each actor performed in TestIndexBlobManagerStress with weights.
var actions = []*actionInfo{
	{"actionSnapshotExisting", actionSnapshotExisting, 50, false},
	{"actionSnapshotAll", actionSnapshotAll, 30, false},
	{"actionAddNewSource", actionAddNewSource, 1, false},
	{"actionMutateDirectoryTree", actionMutateDirectoryTree, 1, false},
	{"actionSnapshotVerify", actionSnapshotVerify, 10, false},
	{"actionContentVerify", actionContentVerify, 5, false},
	{"actionMaintenance", actionMaintenance, 5, true},
	{"actionSmallClockJump", actionSmallClockJump, 500, false},
	{"actionMediumClockJump", actionMediumClockJump, 10, true},
	{"actionLargeClockJump", actionLargeClockJump, 10, true},
}

func actionSnapshotExisting(t *testing.T, e *testenv.CLITest, s *runnerState) {
	t.Helper()

	randomPath := s.dirs[rand.Intn(len(s.dirs))]
	e.RunAndExpectSuccess(t, "snapshot", "create", randomPath)

	s.snapshottedAnything = true
}

func actionSnapshotAll(t *testing.T, e *testenv.CLITest, s *runnerState) {
	t.Helper()

	if !s.snapshottedAnything {
		return
	}

	e.RunAndExpectSuccess(t, "snapshot", "create", "--all")
}

func actionSnapshotVerify(t *testing.T, e *testenv.CLITest, s *runnerState) {
	t.Helper()

	if !s.snapshottedAnything {
		return
	}

	e.RunAndExpectSuccess(t, "snapshot", "verify")
}

func actionContentVerify(t *testing.T, e *testenv.CLITest, s *runnerState) {
	t.Helper()

	if !s.snapshottedAnything {
		return
	}

	e.RunAndExpectSuccess(t, "content", "verify")
}

func actionMaintenance(t *testing.T, e *testenv.CLITest, s *runnerState) {
	t.Helper()

	if s.runnerID == 0 {
		e.RunAndExpectSuccess(t, "maintenance", "run", "--full")
	}
}

func actionSmallClockJump(t *testing.T, e *testenv.CLITest, s *runnerState) {
	t.Helper()

	s.fakeClock.Advance(5 * time.Minute)
}

func actionMediumClockJump(t *testing.T, e *testenv.CLITest, s *runnerState) {
	t.Helper()

	s.fakeClock.Advance(11 * time.Minute)
}

func actionLargeClockJump(t *testing.T, e *testenv.CLITest, s *runnerState) {
	t.Helper()

	s.fakeClock.Advance(31 * time.Minute)
}

func actionAddNewSource(t *testing.T, e *testenv.CLITest, s *runnerState) {
	t.Helper()

	if len(s.dirs) >= maxSourcesPerEnduranceRunner {
		return
	}

	srcDir, err := os.MkdirTemp("", "kopiasrc")
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	s.dirs = append(s.dirs, srcDir)

	testdirtree.CreateDirectoryTree(srcDir, testdirtree.MaybeSimplifyFilesystem(testdirtree.DirectoryTreeOptions{
		Depth:                  3,
		MaxSubdirsPerDirectory: 10,
		MaxFilesPerDirectory:   10,
		MaxFileSize:            100,
	}), &testdirtree.DirectoryTreeCounters{})
}

func actionMutateDirectoryTree(t *testing.T, e *testenv.CLITest, s *runnerState) {
	t.Helper()

	randomPath := s.dirs[rand.Intn(len(s.dirs))]

	testdirtree.CreateDirectoryTree(randomPath, testdirtree.DirectoryTreeOptions{
		Depth:                  2,
		MaxSubdirsPerDirectory: 10,
		MaxFilesPerDirectory:   10,
		MaxFileSize:            100,
	}, &testdirtree.DirectoryTreeCounters{})
}

func pickRandomEnduranceTestAction() *actionInfo {
	sum := 0
	for _, a := range actions {
		sum += a.weight
	}

	n := rand.Intn(sum)
	for _, a := range actions {
		if n < a.weight {
			return a
		}

		n -= a.weight
	}

	panic("impossible")
}

func enduranceRunner(t *testing.T, runnerID int, fakeTimeServer, webdavServer string, failureCount *atomic.Int32, lock *sync.RWMutex, fakeClock *faketime.ClockTimeWithOffset) {
	t.Helper()

	nowFunc := fakeClock.NowFunc()

	runner := testenv.NewExeRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	e.Environment["KOPIA_FAKE_CLOCK_ENDPOINT"] = fakeTimeServer
	e.Environment["KOPIA_CHECK_FOR_UPDATES"] = "false"

	e.RunAndExpectSuccess(t, "repo", "connect", "webdav", "--url", webdavServer, "--override-username="+fmt.Sprintf("runner-%v", runnerID))

	if runnerID == 0 {
		e.RunAndExpectSuccess(t, "maintenance", "set", "--enable-full=true", "--full-interval=4h", "--owner=me")
	}

	var s runnerState

	s.runnerID = runnerID
	s.fakeClock = fakeClock

	actionAddNewSource(t, e, &s)

	for now, k := nowFunc(), 0; now.Before(endTime); now, k = nowFunc(), k+1 {
		if failureCount.Load() != 0 {
			t.Logf("Aborting early because of failures.")
			break
		}

		percent := 100 * now.Sub(startTime).Seconds() / endTime.Sub(startTime).Seconds()

		ai := pickRandomEnduranceTestAction()

		t.Logf("runner %v ITERATION %v NOW=%v (%.2f %%), running %v exclusive=%v", runnerID, k, now.UTC(), percent, ai.name, ai.exclusive)

		runOneIterationUsingLock(t, ai, e, &s, lock)
	}
}

func runOneIterationUsingLock(t *testing.T, ai *actionInfo, e *testenv.CLITest, s *runnerState, lock *sync.RWMutex) {
	t.Helper()

	if ai.exclusive {
		lock.Lock()
		defer lock.Unlock()
	} else {
		lock.RLock()
		defer lock.RUnlock()
	}

	ai.act(t, e, s)
}
