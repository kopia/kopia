package endurance_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"golang.org/x/net/webdav"

	"github.com/kopia/kopia/tests/testenv"
)

const (
	maxSourcesPerEnduranceRunner = 3
	enduranceRunnerCount         = 3
	runnerIterations             = 1000
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
	e := testenv.NewCLITest(t)

	tmpDir, err := ioutil.TempDir("", "endurance")
	if err != nil {
		t.Fatalf("unable to get temp dir: %v", err)
	}

	defer os.RemoveAll(tmpDir)

	fts := testenv.NewFakeTimeServer(time.Date(2000, 1, 1, 0, 0, 0, 0, time.Local), 100*time.Millisecond)

	ft := httptest.NewServer(fts)
	defer ft.Close()

	e.Environment = append(e.Environment, "KOPIA_FAKE_CLOCK_ENDPOINT="+ft.URL)

	sts := httptest.NewServer(&webdav.Handler{
		FileSystem: webdavDirWithFakeClock{webdav.Dir(tmpDir), fts},
		LockSystem: webdav.NewMemLS(),
	})
	defer sts.Close()

	e.RunAndExpectSuccess(t, "repo", "create", "webdav", "--url", sts.URL)

	t.Run("Runners", func(t *testing.T) {
		for i := 0; i < enduranceRunnerCount; i++ {
			i := i

			t.Run(fmt.Sprintf("Runner-%v", i), func(t *testing.T) {
				t.Parallel()

				enduranceRunner(t, i, ft.URL, sts.URL)
			})
		}
	})

	e.RunAndExpectSuccess(t, "snapshot", "list", "-a", "-i")
	e.RunAndExpectSuccess(t, "blob", "list")
}

type runnerState struct {
	dirs                []string
	snapshottedAnything bool
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
}

func actionSnapshotExisting(t *testing.T, e *testenv.CLITest, s *runnerState) {
	randomPath := s.dirs[rand.Intn(len(s.dirs))]
	e.RunAndExpectSuccess(t, "snapshot", "create", randomPath, "--no-progress")

	s.snapshottedAnything = true
}

func actionSnapshotAll(t *testing.T, e *testenv.CLITest, s *runnerState) {
	if !s.snapshottedAnything {
		return
	}

	e.RunAndExpectSuccess(t, "snapshot", "create", "--all", "--no-progress")
}

func actionSnapshotVerify(t *testing.T, e *testenv.CLITest, s *runnerState) {
	if !s.snapshottedAnything {
		return
	}

	e.RunAndExpectSuccess(t, "snapshot", "verify")
}

func actionContentVerify(t *testing.T, e *testenv.CLITest, s *runnerState) {
	if !s.snapshottedAnything {
		return
	}

	e.RunAndExpectSuccess(t, "content", "verify")
}

func actionAddNewSource(t *testing.T, e *testenv.CLITest, s *runnerState) {
	if len(s.dirs) >= maxSourcesPerEnduranceRunner {
		return
	}

	srcDir, err := ioutil.TempDir("", "kopiasrc")
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	s.dirs = append(s.dirs, srcDir)

	testenv.CreateDirectoryTree(srcDir, testenv.DirectoryTreeOptions{
		Depth:                  3,
		MaxSubdirsPerDirectory: 10,
		MaxFilesPerDirectory:   10,
		MaxFileSize:            100,
	}, &testenv.DirectoryTreeCounters{})
}

func actionMutateDirectoryTree(t *testing.T, e *testenv.CLITest, s *runnerState) {
	randomPath := s.dirs[rand.Intn(len(s.dirs))]

	testenv.CreateDirectoryTree(randomPath, testenv.DirectoryTreeOptions{
		Depth:                  2,
		MaxSubdirsPerDirectory: 10,
		MaxFilesPerDirectory:   10,
		MaxFileSize:            100,
	}, &testenv.DirectoryTreeCounters{})
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

func enduranceRunner(t *testing.T, runnerID int, fakeTimeServer, webdavServer string) {
	e := testenv.NewCLITest(t)

	e.PassthroughStderr = true

	e.Environment = append(e.Environment,
		"KOPIA_FAKE_CLOCK_ENDPOINT="+fakeTimeServer,
		"KOPIA_CHECK_FOR_UPDATES=false",
	)

	e.RunAndExpectSuccess(t, "repo", "connect", "webdav", "--url", webdavServer, "--override-username="+fmt.Sprintf("runner-%v", runnerID))

	if runnerID == 0 {
		e.RunAndExpectSuccess(t, "gc", "set", "--enable-full=true", "--full-interval=4h", "--owner=me")
	}

	var s runnerState

	actionAddNewSource(t, e, &s)

	for k := 0; k < runnerIterations; k++ {
		t.Logf("ITERATION %v / %v", k, runnerIterations)

		act := pickRandomEnduranceTestAction()
		act(t, e, &s)
	}
}
