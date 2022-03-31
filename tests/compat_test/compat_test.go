package compat_test

import (
	"os"
	"testing"

	"github.com/kopia/kopia/tests/testenv"
)

var (
	kopiaCurrentExe = os.Getenv("KOPIA_CURRENT_EXE")
	kopia08exe      = os.Getenv("KOPIA_08_EXE")
)

func TestRepoCreatedWith08CanBeOpenedWithCurrent(t *testing.T) {
	t.Parallel()

	if kopiaCurrentExe == "" {
		t.Skip()
	}

	runnerCurrent := testenv.NewExeRunnerWithBinary(t, kopiaCurrentExe)
	runner08 := testenv.NewExeRunnerWithBinary(t, kopia08exe)

	// create repository using v0.8
	e1 := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner08)
	e1.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e1.RepoDir)
	e1.RunAndExpectSuccess(t, "snap", "create", ".")

	// able to open it using current
	e2 := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runnerCurrent)
	e2.RunAndExpectSuccess(t, "repo", "connect", "filesystem", "--path", e1.RepoDir)
	e2.RunAndExpectSuccess(t, "snap", "ls")

	// upgrade
	e2.RunAndExpectSuccess(t, "repository", "upgrade",
		"--upgrade-owner-id", "owner",
		"--io-drain-timeout", "1s", "--force",
		"--status-poll-interval", "1s",
		"--max-clock-drift", "1s")

	// now 0.8 can't open it anymore
	e3 := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner08)
	e3.RunAndExpectFailure(t, "repo", "connect", "filesystem", "--path", e1.RepoDir)
}

func TestRepoCreatedWithCurrentWithFormatVersion1CanBeOpenedWith08(t *testing.T) {
	t.Parallel()

	if kopiaCurrentExe == "" {
		t.Skip()
	}

	runnerCurrent := testenv.NewExeRunnerWithBinary(t, kopiaCurrentExe)
	runner08 := testenv.NewExeRunnerWithBinary(t, kopia08exe)

	// create repository using current, setting format version to v1
	e1 := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runnerCurrent)
	e1.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e1.RepoDir, "--format-version=1")
	e1.RunAndExpectSuccess(t, "snap", "create", ".")

	// able to open it using 0.8
	e2 := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner08)
	e2.RunAndExpectSuccess(t, "repo", "connect", "filesystem", "--path", e1.RepoDir)
	e1.RunAndExpectSuccess(t, "snap", "ls")
}

func TestRepoCreatedWithCurrentCannotBeOpenedWith08(t *testing.T) {
	t.Parallel()

	if kopiaCurrentExe == "" {
		t.Skip()
	}

	runnerCurrent := testenv.NewExeRunnerWithBinary(t, kopiaCurrentExe)
	runner08 := testenv.NewExeRunnerWithBinary(t, kopia08exe)

	// create repository using current, using default format version (v2)
	e1 := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runnerCurrent)
	e1.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e1.RepoDir)
	e1.RunAndExpectSuccess(t, "snap", "create", ".")

	// can't to open it using 0.8
	e2 := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner08)
	e2.RunAndExpectFailure(t, "repo", "connect", "filesystem", "--path", e1.RepoDir)
}
