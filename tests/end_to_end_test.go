package tests

import (
	"encoding/hex"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const repoPassword = "qWQPJ2hiiLgWRRCr"

type testenv struct {
	repoDir   string
	configDir string
	dataDir   string
	exe       string

	fixedArgs   []string
	environment []string
}

type sourceInfo struct {
	user      string
	host      string
	path      string
	snapshots []snapshotInfo
}

type snapshotInfo struct {
	objectID string
	time     string
}

func newTestEnv(t *testing.T) *testenv {
	exe := os.Getenv("KOPIA_EXE")
	if exe == "" {
		t.Skip("KOPIA_EXE not set in the environment, skipping test")
	}

	repoDir, err := ioutil.TempDir("", "kopia-repo")
	if err != nil {
		t.Fatalf("can't create temp directory: %v", err)
	}

	configDir, err := ioutil.TempDir("", "kopia-config")
	if err != nil {
		t.Fatalf("can't create temp directory: %v", err)
	}

	dataDir, err := ioutil.TempDir("", "kopia-data")
	if err != nil {
		t.Fatalf("can't create temp directory: %v", err)
	}

	return &testenv{
		repoDir:   repoDir,
		configDir: configDir,
		dataDir:   dataDir,
		exe:       exe,
		fixedArgs: []string{
			// use per-test config file, to avoid clobbering current user's setup.
			"--config-file", filepath.Join(configDir, ".kopia.config"),
		},
		environment: []string{"KOPIA_PASSWORD=" + repoPassword},
	}
}

func (e *testenv) cleanup() {
	if e.repoDir != "" {
		os.RemoveAll(e.repoDir)
	}
	if e.configDir != "" {
		os.RemoveAll(e.configDir)
	}
	if e.dataDir != "" {
		os.RemoveAll(e.dataDir)
	}
}

func TestRepo(t *testing.T) {
	e := newTestEnv(t)
	defer e.cleanup()

	e.runAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.repoDir)
	e.runAndExpectSuccess(t, "repo", "disconnect")
	e.runAndExpectSuccess(t, "repo", "connect", "filesystem", "--path", e.repoDir)

	e.runAndExpectSuccess(t, "snapshot", "create", ".")
	e.runAndExpectSuccess(t, "snapshot", "list", ".")

	dir1 := filepath.Join(e.dataDir, "dir1")
	createDirectory(dir1, 3)
	e.runAndExpectSuccess(t, "snapshot", "create", dir1)
	e.runAndExpectSuccess(t, "snapshot", "create", dir1)

	dir2 := filepath.Join(e.dataDir, "dir2")
	createDirectory(dir2, 3)
	e.runAndExpectSuccess(t, "snapshot", "create", dir2)
	e.runAndExpectSuccess(t, "snapshot", "create", dir2)
	sources := listSnapshotsAndExpectSuccess(t, e)
	if got, want := len(sources), 3; got != want {
		t.Errorf("unexpected number of sources: %v, want %v in %#v", got, want, sources)
	}

	// expect 5 blocks, each snapshot creation adds one index block.
	e.runAndVerifyOutputLineCount(t, 5, "blockindex", "ls")
	e.runAndExpectSuccess(t, "blockindex", "optimize")
	e.runAndVerifyOutputLineCount(t, 1, "blockindex", "ls")
	e.runAndVerifyOutputLineCount(t, 6, "blockindex", "ls", "--all")

	e.runAndExpectSuccess(t, "snapshot", "create", ".", dir1, dir2)
	e.runAndVerifyOutputLineCount(t, 2, "blockindex", "ls")

	//t.Fail()
}

func (e *testenv) runAndExpectSuccess(t *testing.T, args ...string) []string {
	stdout, err := e.run(t, args...)
	if err != nil {
		t.Fatalf("'kopia %v' failed with %v", strings.Join(args, " "), err)
	}
	return stdout
}

func (e *testenv) runAndVerifyOutputLineCount(t *testing.T, wantLines int, args ...string) []string {
	lines := e.runAndExpectSuccess(t, args...)
	if len(lines) != wantLines {
		t.Errorf("unexpected list of results of 'kopia %v': %v (%v lines), wanted %v", strings.Join(args, " "), lines, len(lines), wantLines)
	}
	return lines
}

func (e *testenv) run(t *testing.T, args ...string) ([]string, error) {
	t.Logf("running 'kopia %v'", strings.Join(args, " "))
	cmdArgs := append(append([]string(nil), e.fixedArgs...), args...)
	c := exec.Command(e.exe, cmdArgs...)
	c.Env = append(os.Environ(), e.environment...)
	o, err := c.CombinedOutput()
	t.Logf("finished 'kopia %v' with err=%v and output:\n%v", strings.Join(args, " "), err, string(o))
	return splitLines(string(o)), err
}

func listSnapshotsAndExpectSuccess(t *testing.T, e *testenv, targets ...string) []sourceInfo {
	lines := e.runAndExpectSuccess(t, append([]string{"snapshot", "list"}, targets...)...)
	return mustParseSnapshots(t, lines)
}

func createDirectory(dirname string, depth int) error {
	if err := os.MkdirAll(dirname, 0700); err != nil {
		return err
	}

	if depth > 0 {
		numSubDirs := rand.Intn(10) + 1
		for i := 0; i < numSubDirs; i++ {
			subdirName := randomName()

			if err := createDirectory(filepath.Join(dirname, subdirName), depth-1); err != nil {
				return err
			}
		}
	}

	numFiles := rand.Intn(10) + 1
	for i := 0; i < numFiles; i++ {
		fileName := randomName()

		if err := createRandomFile(filepath.Join(dirname, fileName)); err != nil {
			return err
		}
	}

	return nil
}

func createRandomFile(filename string) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	length := rand.Int63n(100000)
	io.Copy(f, io.LimitReader(rand.New(rand.NewSource(1)), length))

	return nil
}

func mustParseSnapshots(t *testing.T, lines []string) []sourceInfo {
	var result []sourceInfo

	var currentSource *sourceInfo

	for _, l := range lines {
		if l == "" {
			continue
		}

		if strings.HasPrefix(l, "  ") {
			if currentSource == nil {
				t.Errorf("snapshot without a source: %q", l)
				return nil
			}
			currentSource.snapshots = append(currentSource.snapshots, mustParseSnaphotInfo(t, l[2:]))
			continue
		}

		s := mustParseSourceInfo(t, l)
		result = append(result, s)
		currentSource = &result[len(result)-1]
	}

	return result
}

func randomName() string {
	b := make([]byte, rand.Intn(10)+3)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func mustParseSnaphotInfo(t *testing.T, l string) snapshotInfo {
	return snapshotInfo{}
}

func mustParseSourceInfo(t *testing.T, l string) sourceInfo {
	p1 := strings.Index(l, "@")
	p2 := strings.Index(l, ":")
	if p1 >= 0 && p2 > p1 {
		return sourceInfo{user: l[0:p1], host: l[p1+1 : p2], path: l[p2+1:]}
	}

	t.Fatalf("can't parse source info: %q", l)
	return sourceInfo{}
}

func splitLines(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}

	var result []string
	for _, l := range strings.Split(s, "\n") {
		result = append(result, strings.TrimRight(l, "\r"))
	}
	return result
}
