// Package testenv contains environment for use in testing.
package testenv

import (
	"bufio"
	cryptorand "crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
)

const (
	repoPassword        = "qWQPJ2hiiLgWRRCr" // nolint:gosec
	maxOutputLinesToLog = 40
)

// CLITest encapsulates state for a CLI-based test.
type CLITest struct {
	startTime time.Time

	RepoDir   string
	ConfigDir string
	Exe       string

	fixedArgs   []string
	environment []string
}

// SourceInfo reprents a single source (user@host:/path) with its snapshots.
type SourceInfo struct {
	User      string
	Host      string
	Path      string
	Snapshots []SnapshotInfo
}

// SnapshotInfo represents a single snapshot information.
type SnapshotInfo struct {
	ObjectID   string
	SnapshotID string
	Time       time.Time
}

// NewCLITest creates a new instance of *CLITest
func NewCLITest(t *testing.T) *CLITest {
	Exe := os.Getenv("KOPIA_EXE")
	if Exe == "" {
		// Exe = "kopia"
		t.Skip()
	}

	RepoDir, err := ioutil.TempDir("", "kopia-repo")
	if err != nil {
		t.Fatalf("can't create temp directory: %v", err)
	}

	ConfigDir, err := ioutil.TempDir("", "kopia-config")
	if err != nil {
		t.Fatalf("can't create temp directory: %v", err)
	}

	fixedArgs := []string{
		// use per-test config file, to avoid clobbering current user's setup.
		"--config-file", filepath.Join(ConfigDir, ".kopia.config"),
	}

	if runtime.GOOS == "darwin" {
		// this prevents kopia from spawning `security` subprocess which speeds up the test on macOS.
		fixedArgs = append(fixedArgs, "--no-use-keychain")
	}

	return &CLITest{
		startTime:   time.Now(),
		RepoDir:     RepoDir,
		ConfigDir:   ConfigDir,
		Exe:         Exe,
		fixedArgs:   fixedArgs,
		environment: []string{"KOPIA_PASSWORD=" + repoPassword},
	}
}

// Cleanup cleans up the test environment unless the test has failed.
func (e *CLITest) Cleanup(t *testing.T) {
	if t.Failed() {
		t.Logf("skipped cleanup for failed test, examine repository: %v", e.RepoDir)
		return
	}

	if e.RepoDir != "" {
		os.RemoveAll(e.RepoDir) //nolint:errcheck
	}

	if e.ConfigDir != "" {
		os.RemoveAll(e.ConfigDir) //nolint:errcheck
	}
}

// RunAndExpectSuccess runs the given command, expects it to succeed and returns its output lines.
func (e *CLITest) RunAndExpectSuccess(t *testing.T, args ...string) []string {
	t.Helper()

	stdout, _, err := e.Run(t, args...)
	if err != nil {
		t.Fatalf("'kopia %v' failed with %v", strings.Join(args, " "), err)
	}

	return stdout
}

// RunAndProcessStderr runs the given command, and streams its output line-by-line to a given function until it returns false.
func (e *CLITest) RunAndProcessStderr(t *testing.T, callback func(line string) bool, args ...string) *exec.Cmd {
	t.Helper()

	t.Logf("running 'kopia %v'", strings.Join(args, " "))
	cmdArgs := append(append([]string(nil), e.fixedArgs...), args...)

	// nolint:gosec
	c := exec.Command(e.Exe, cmdArgs...)
	c.Env = append(os.Environ(), e.environment...)

	stderrPipe, err := c.StderrPipe()
	if err != nil {
		t.Fatalf("can't set up stderr pipe reader")
	}

	if err := c.Start(); err != nil {
		t.Fatalf("unable to start")
	}

	scanner := bufio.NewScanner(stderrPipe)
	for scanner.Scan() {
		if !callback(scanner.Text()) {
			break
		}
	}

	// complete the scan in background without processing lines.
	go func() {
		for scanner.Scan() {
		}
	}()

	return c
}

// RunAndExpectSuccessWithErrOut runs the given command, expects it to succeed and returns its stdout and stderr lines.
func (e *CLITest) RunAndExpectSuccessWithErrOut(t *testing.T, args ...string) (stdout, stderr []string) {
	t.Helper()

	stdout, stderr, err := e.Run(t, args...)
	if err != nil {
		t.Fatalf("'kopia %v' failed with %v", strings.Join(args, " "), err)
	}

	return stdout, stderr
}

// RunAndExpectFailure runs the given command, expects it to fail and returns its output lines.
func (e *CLITest) RunAndExpectFailure(t *testing.T, args ...string) []string {
	t.Helper()

	stdout, _, err := e.Run(t, args...)
	if err == nil {
		t.Fatalf("'kopia %v' succeeded, but expected failure", strings.Join(args, " "))
	}

	return stdout
}

// RunAndVerifyOutputLineCount runs the given command and asserts it returns the given number of output lines, then returns them.
func (e *CLITest) RunAndVerifyOutputLineCount(t *testing.T, wantLines int, args ...string) []string {
	t.Helper()

	lines := e.RunAndExpectSuccess(t, args...)
	if len(lines) != wantLines {
		t.Errorf("unexpected list of results of 'kopia %v': %v (%v lines), wanted %v", strings.Join(args, " "), lines, len(lines), wantLines)
	}

	return lines
}

// Run executes kopia with given arguments and returns the output lines.
func (e *CLITest) Run(t *testing.T, args ...string) (stdout, stderr []string, err error) {
	t.Helper()
	t.Logf("running 'kopia %v'", strings.Join(args, " "))
	// nolint:gosec
	cmdArgs := append(append([]string(nil), e.fixedArgs...), args...)

	// nolint:gosec
	c := exec.Command(e.Exe, cmdArgs...)
	c.Env = append(os.Environ(), e.environment...)

	stderrPipe, err := c.StderrPipe()
	if err != nil {
		t.Fatalf("can't set up stderr pipe reader")
	}

	var errOut []byte

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()

		errOut, err = ioutil.ReadAll(stderrPipe)
	}()

	o, err := c.Output()

	wg.Wait()
	t.Logf("finished 'kopia %v' with err=%v and output:\n%v\nstderr:\n%v\n", strings.Join(args, " "), err, trimOutput(string(o)), trimOutput(string(errOut)))

	return splitLines(string(o)), splitLines(string(errOut)), err
}

func trimOutput(s string) string {
	lines := splitLines(s)
	if len(lines) <= maxOutputLinesToLog {
		return s
	}

	lines2 := append([]string(nil), lines[0:(maxOutputLinesToLog/2)]...)
	lines2 = append(lines2, fmt.Sprintf("/* %v lines removed */", len(lines)-maxOutputLinesToLog))
	lines2 = append(lines2, lines[len(lines)-(maxOutputLinesToLog/2):]...)

	return strings.Join(lines2, "\n")
}

// ListSnapshotsAndExpectSuccess lists given snapshots and parses the output.
func (e *CLITest) ListSnapshotsAndExpectSuccess(t *testing.T, targets ...string) []SourceInfo {
	lines := e.RunAndExpectSuccess(t, append([]string{"snapshot", "list", "-l", "--manifest-id"}, targets...)...)
	return mustParseSnapshots(t, lines)
}

// DirEntry represents directory entry.
type DirEntry struct {
	Name     string
	ObjectID string
}

// ListDirectory lists a given directory and returns directory entries.
func (e *CLITest) ListDirectory(t *testing.T, targets ...string) []DirEntry {
	lines := e.RunAndExpectSuccess(t, append([]string{"ls", "-l"}, targets...)...)
	return mustParseDirectoryEntries(lines)
}

func mustParseDirectoryEntries(lines []string) []DirEntry {
	var result []DirEntry

	for _, l := range lines {
		parts := strings.Fields(l)

		result = append(result, DirEntry{
			Name:     parts[6],
			ObjectID: parts[5],
		})
	}

	return result
}

// DirectoryTreeOptions lists options for CreateDirectoryTree
type DirectoryTreeOptions struct {
	Depth                  int
	MaxSubdirsPerDirectory int
	MaxFilesPerDirectory   int
	MaxFileSize            int
	MinNameLength          int
	MaxNameLength          int
}

// DirectoryTreeCounters stores stats about files and directories created by CreateDirectoryTree
type DirectoryTreeCounters struct {
	Files         int
	Directories   int
	TotalFileSize int64
	MaxFileSize   int64
}

// MustCreateDirectoryTree creates a directory tree of a given depth with random files.
func MustCreateDirectoryTree(t *testing.T, dirname string, options DirectoryTreeOptions) {
	t.Helper()

	var counters DirectoryTreeCounters
	if err := createDirectoryTreeInternal(dirname, options, &counters); err != nil {
		t.Error(err)
	}
}

// CreateDirectoryTree creates a directory tree of a given depth with random files.
func CreateDirectoryTree(dirname string, options DirectoryTreeOptions, counters *DirectoryTreeCounters) error {
	if counters == nil {
		counters = &DirectoryTreeCounters{}
	}

	return createDirectoryTreeInternal(dirname, options, counters)
}

// MustCreateRandomFile creates a new file at the provided path with randomized contents.
// It will fail with a test error if the creation does not succeed.
func MustCreateRandomFile(t *testing.T, filePath string, options DirectoryTreeOptions, counters *DirectoryTreeCounters) {
	if err := CreateRandomFile(filePath, options, counters); err != nil {
		t.Error(err)
	}
}

// CreateRandomFile creates a new file at the provided path with randomized contents
func CreateRandomFile(filePath string, options DirectoryTreeOptions, counters *DirectoryTreeCounters) error {
	if counters == nil {
		counters = &DirectoryTreeCounters{}
	}

	return createRandomFile(filePath, options, counters)
}

// createDirectoryTreeInternal creates a directory tree of a given depth with random files.
func createDirectoryTreeInternal(dirname string, options DirectoryTreeOptions, counters *DirectoryTreeCounters) error {
	if err := os.MkdirAll(dirname, 0700); err != nil {
		return errors.Wrapf(err, "unable to create directory %v", dirname)
	}

	counters.Directories++

	if options.Depth > 0 && options.MaxSubdirsPerDirectory > 0 {
		childOptions := options
		childOptions.Depth--

		numSubDirs := rand.Intn(options.MaxSubdirsPerDirectory) + 1
		for i := 0; i < numSubDirs; i++ {
			subdirName := randomName(options)

			if err := createDirectoryTreeInternal(filepath.Join(dirname, subdirName), childOptions, counters); err != nil {
				return errors.Wrap(err, "unable to create subdirectory")
			}
		}
	}

	if options.MaxFilesPerDirectory > 0 {
		numFiles := rand.Intn(options.MaxFilesPerDirectory) + 1
		for i := 0; i < numFiles; i++ {
			fileName := randomName(options)

			if err := createRandomFile(filepath.Join(dirname, fileName), options, counters); err != nil {
				return errors.Wrap(err, "unable to create random file")
			}
		}
	}

	return nil
}
func createRandomFile(filename string, options DirectoryTreeOptions, counters *DirectoryTreeCounters) error {
	f, err := os.Create(filename)
	if err != nil {
		return errors.Wrap(err, "unable to create random file")
	}
	defer f.Close() //nolint:errcheck

	maxFileSize := int64(intOrDefault(options.MaxFileSize, 100000))

	length := rand.Int63n(maxFileSize)

	_, err = io.Copy(f, io.LimitReader(rand.New(rand.NewSource(time.Now().UnixNano())), length))
	if err != nil {
		return errors.Wrap(err, "file create error")
	}

	counters.Files++
	counters.TotalFileSize += length

	if length > counters.MaxFileSize {
		counters.MaxFileSize = length
	}

	return nil
}

func mustParseSnapshots(t *testing.T, lines []string) []SourceInfo {
	var result []SourceInfo

	var currentSource *SourceInfo

	for _, l := range lines {
		if l == "" {
			continue
		}

		if strings.HasPrefix(l, "  ") {
			if currentSource == nil {
				t.Errorf("snapshot without a source: %q", l)
				return nil
			}

			currentSource.Snapshots = append(currentSource.Snapshots, mustParseSnaphotInfo(t, l[2:]))

			continue
		}

		s := mustParseSourceInfo(t, l)
		result = append(result, s)
		currentSource = &result[len(result)-1]
	}

	return result
}

func randomName(opt DirectoryTreeOptions) string {
	maxNameLength := intOrDefault(opt.MaxNameLength, 15)
	minNameLength := intOrDefault(opt.MinNameLength, 3)

	b := make([]byte, rand.Intn(maxNameLength-minNameLength)+minNameLength)
	cryptorand.Read(b) // nolint:errcheck

	return hex.EncodeToString(b)
}

func mustParseSnaphotInfo(t *testing.T, l string) SnapshotInfo {
	parts := strings.Split(l, " ")

	ts, err := time.Parse("2006-01-02 15:04:05 MST", strings.Join(parts[0:3], " "))
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	manifestField := parts[7]
	snapID := strings.TrimPrefix(manifestField, "manifest:")

	return SnapshotInfo{
		Time:       ts,
		ObjectID:   parts[3],
		SnapshotID: snapID,
	}
}

func mustParseSourceInfo(t *testing.T, l string) SourceInfo {
	p1 := strings.Index(l, "@")

	p2 := strings.Index(l, ":")

	if p1 >= 0 && p2 > p1 {
		return SourceInfo{User: l[0:p1], Host: l[p1+1 : p2], Path: l[p2+1:]}
	}

	t.Fatalf("can't parse source info: %q", l)

	return SourceInfo{}
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

// AssertNoError fails the test if a given error is not nil.
func AssertNoError(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Fatalf("err: %v", err)
	}
}

// CheckNoError fails the test if a given error is not nil.
func CheckNoError(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Errorf("err: %v", err)
	}
}

func intOrDefault(a, b int) int {
	if a > 0 {
		return a
	}

	return b
}
