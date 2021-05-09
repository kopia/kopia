package endurance_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/webdav"

	"github.com/kopia/kopia/tests/testdirtree"
	"github.com/kopia/kopia/tests/testenv"
)

const (
	maxSourcesPerEnduranceRunner = 3
	enduranceRunnerCount         = 3
)

var (
	// We will simulate 2 weeks of running with clock moving by a lot every time it's read.
	startTime         = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	simulatedDuration = 14 * 24 * time.Hour
	endTime           = startTime.Add(simulatedDuration)
	tickIncrement     = 350 * time.Millisecond
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
	e := testenv.NewCLITest(t, runner)

	tmpDir, err := ioutil.TempDir("", "endurance")
	if err != nil {
		t.Fatalf("unable to get temp dir: %v", err)
	}

	defer os.RemoveAll(tmpDir)

	fts := testenv.NewFakeTimeServer(startTime, tickIncrement)

	ft := httptest.NewServer(fts)
	defer ft.Close()

	runner.Environment = append(runner.Environment, "KOPIA_FAKE_CLOCK_ENDPOINT="+ft.URL)

	sts := httptest.NewServer(&webdav.Handler{
		FileSystem: webdavDirWithFakeClock{webdav.Dir(tmpDir), fts},
		LockSystem: webdav.NewMemLS(),
	})
	defer sts.Close()

	e.RunAndExpectSuccess(t, "repo", "create", "webdav", "--url", sts.URL)

	failureCount := new(int32)

	t.Run("Runners", func(t *testing.T) {
		for i := 0; i < enduranceRunnerCount; i++ {
			i := i

			t.Run(fmt.Sprintf("Runner-%v", i), func(t *testing.T) {
				t.Parallel()
				defer func() {
					if t.Failed() {
						atomic.AddInt32(failureCount, 1)
					}
				}()

				enduranceRunner(t, i, ft.URL, sts.URL, failureCount, fts.Now)
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
}

type action func(t *testing.T, e *testenv.CLITest, s *runnerState)

// actionsTestIndexBlobManagerStress is a set of actionsTestIndexBlobManagerStress by each actor performed in TestIndexBlobManagerStress with weights.
var actionsTestIndexBlobManagerStress = []struct {
	a      action
	weight int
}{
	{actionSnapshotExisting, 50},
	{actionSnapshotAll, 30},
	{actionAddNewSource, 1},
	{actionMutateDirectoryTree, 1},
	{actionSnapshotVerify, 10},
	{actionContentVerify, 5},
	{actionMaintenance, 5},
}

func actionSnapshotExisting(t *testing.T, e *testenv.CLITest, s *runnerState) {
	t.Helper()

	randomPath := s.dirs[rand.Intn(len(s.dirs))]
	e.RunAndExpectSuccess(t, "snapshot", "create", randomPath, "--no-progress")

	s.snapshottedAnything = true
}

func actionSnapshotAll(t *testing.T, e *testenv.CLITest, s *runnerState) {
	t.Helper()

	if !s.snapshottedAnything {
		return
	}

	e.RunAndExpectSuccess(t, "snapshot", "create", "--all", "--no-progress")
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

func actionAddNewSource(t *testing.T, e *testenv.CLITest, s *runnerState) {
	t.Helper()

	if len(s.dirs) >= maxSourcesPerEnduranceRunner {
		return
	}

	srcDir, err := ioutil.TempDir("", "kopiasrc")
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

func pickRandomEnduranceTestAction() action {
	sum := 0
	for _, a := range actionsTestIndexBlobManagerStress {
		sum += a.weight
	}

	n := rand.Intn(sum)
	for _, a := range actionsTestIndexBlobManagerStress {
		if n < a.weight {
			return a.a
		}

		n -= a.weight
	}

	panic("impossible")
}

func enduranceRunner(t *testing.T, runnerID int, fakeTimeServer, webdavServer string, failureCount *int32, nowFunc func() time.Time) {
	t.Helper()

	runner := testenv.NewExeRunner(t)
	e := testenv.NewCLITest(t, runner)

	runner.Environment = append(runner.Environment,
		"KOPIA_FAKE_CLOCK_ENDPOINT="+fakeTimeServer,
		"KOPIA_CHECK_FOR_UPDATES=false",
	)

	e.RunAndExpectSuccess(t, "repo", "connect", "webdav", "--url", webdavServer, "--override-username="+fmt.Sprintf("runner-%v", runnerID))

	if runnerID == 0 {
		e.RunAndExpectSuccess(t, "gc", "set", "--enable-full=true", "--full-interval=4h", "--owner=me")
	}

	var s runnerState

	s.runnerID = runnerID

	actionAddNewSource(t, e, &s)

	for now, k := nowFunc(), 0; now.Before(endTime); now, k = nowFunc(), k+1 {
		if atomic.LoadInt32(failureCount) != 0 {
			t.Logf("Aborting early because of failures.")
			break
		}

		percent := 100 * now.Sub(startTime).Seconds() / endTime.Sub(startTime).Seconds()

		t.Logf("ITERATION %v NOW=%v (%.2f %%)", k, now, percent)

		act := pickRandomEnduranceTestAction()
		act(t, e, &s)
	}
}
