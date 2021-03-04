// Package testenv contains Environment for use in testing.
package testenv

import (
	"bufio"
	"bytes"
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
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/iocopy"
	"github.com/kopia/kopia/internal/testutil"
)

const (
	// TestRepoPassword is a password for repositories created in tests.
	TestRepoPassword = "qWQPJ2hiiLgWRRCr"

	maxOutputLinesToLog = 4000
)

// CLITest encapsulates state for a CLI-based test.
type CLITest struct {
	startTime time.Time

	RepoDir   string
	ConfigDir string
	Exe       string

	fixedArgs   []string
	Environment []string

	DefaultRepositoryCreateFlags []string

	PassthroughStderr bool // this is for debugging only

	NextCommandStdin io.Reader // this is used for stdin source tests

	LogsDir string
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

// NewCLITest creates a new instance of *CLITest.
func NewCLITest(t *testing.T) *CLITest {
	t.Helper()

	exe := os.Getenv("KOPIA_EXE")
	if exe == "" {
		// exe = "kopia"
		t.Skip()
	}

	configDir := testutil.TempDirectory(t)

	cleanName := strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(
		t.Name(),
		"/", "_"), "\\", "_"), ":", "_")

	logsDir := filepath.Join(os.TempDir(), "kopia-logs", cleanName+"."+clock.Now().Local().Format("20060102150405"))

	t.Cleanup(func() {
		if t.Failed() && os.Getenv("KOPIA_DISABLE_LOG_DUMP_ON_FAILURE") == "" {
			dumpLogs(t, logsDir)
		}

		os.RemoveAll(logsDir)
	})

	fixedArgs := []string{
		// use per-test config file, to avoid clobbering current user's setup.
		"--config-file", filepath.Join(configDir, ".kopia.config"),
		"--log-dir", logsDir,
	}

	// disable the use of keyring
	switch runtime.GOOS {
	case "darwin":
		fixedArgs = append(fixedArgs, "--no-use-keychain")
	case "windows":
		fixedArgs = append(fixedArgs, "--no-use-credential-manager")
	case "linux":
		fixedArgs = append(fixedArgs, "--no-use-keyring")
	}

	var formatFlags []string

	if testutil.ShouldReduceTestComplexity() {
		formatFlags = []string{
			"--encryption", "CHACHA20-POLY1305-HMAC-SHA256",
			"--block-hash", "BLAKE2S-256",
		}
	}

	return &CLITest{
		startTime:                    clock.Now(),
		RepoDir:                      testutil.TempDirectory(t),
		ConfigDir:                    configDir,
		Exe:                          filepath.FromSlash(exe),
		fixedArgs:                    fixedArgs,
		DefaultRepositoryCreateFlags: formatFlags,
		LogsDir:                      logsDir,
		Environment: []string{
			"KOPIA_PASSWORD=" + TestRepoPassword,
			"KOPIA_ADVANCED_COMMANDS=enabled",
		},
	}
}

func dumpLogs(t *testing.T, dirname string) {
	t.Helper()

	entries, err := ioutil.ReadDir(dirname)
	if err != nil {
		t.Errorf("unable to read %v: %v", dirname, err)

		return
	}

	for _, e := range entries {
		if e.IsDir() {
			dumpLogs(t, filepath.Join(dirname, e.Name()))
			continue
		}

		dumpLogFile(t, filepath.Join(dirname, e.Name()))
	}
}

func dumpLogFile(t *testing.T, fname string) {
	t.Helper()

	data, err := ioutil.ReadFile(fname)
	if err != nil {
		t.Error(err)
		return
	}

	t.Logf("LOG FILE: %v %v", fname, trimOutput(string(data)))
}

// RemoveDefaultPassword prevents KOPIA_PASSWORD from being passed to kopia.
func (e *CLITest) RemoveDefaultPassword() {
	var newEnv []string

	for _, s := range e.Environment {
		if !strings.HasPrefix(s, "KOPIA_PASSWORD=") {
			newEnv = append(newEnv, s)
		}
	}

	e.Environment = newEnv
}

// RunAndExpectSuccess runs the given command, expects it to succeed and returns its output lines.
func (e *CLITest) RunAndExpectSuccess(t *testing.T, args ...string) []string {
	t.Helper()

	stdout, _, err := e.Run(t, false, args...)
	if err != nil {
		t.Fatalf("'kopia %v' failed with %v", strings.Join(args, " "), err)
	}

	return stdout
}

// RunAndProcessStderr runs the given command, and streams its output line-by-line to a given function until it returns false.
func (e *CLITest) RunAndProcessStderr(t *testing.T, callback func(line string) bool, args ...string) *exec.Cmd {
	t.Helper()

	c := exec.Command(e.Exe, e.cmdArgs(args)...)
	c.Env = append(os.Environ(), e.Environment...)
	t.Logf("running '%v %v'", c.Path, c.Args)

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
			t.Logf("[stderr] %v", scanner.Text())
		}
	}()

	return c
}

// RunAndExpectSuccessWithErrOut runs the given command, expects it to succeed and returns its stdout and stderr lines.
func (e *CLITest) RunAndExpectSuccessWithErrOut(t *testing.T, args ...string) (stdout, stderr []string) {
	t.Helper()

	stdout, stderr, err := e.Run(t, false, args...)
	if err != nil {
		t.Fatalf("'kopia %v' failed with %v", strings.Join(args, " "), err)
	}

	return stdout, stderr
}

// RunAndExpectFailure runs the given command, expects it to fail and returns its output lines.
func (e *CLITest) RunAndExpectFailure(t *testing.T, args ...string) []string {
	t.Helper()

	stdout, _, err := e.Run(t, true, args...)
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

func (e *CLITest) cmdArgs(args []string) []string {
	var suffix []string

	// detect repository creation and override DefaultRepositoryCreateFlags for best
	// performance on the current platform.
	if len(args) >= 2 && (args[0] == "repo" && args[1] == "create") {
		suffix = e.DefaultRepositoryCreateFlags
	}

	return append(append(append([]string(nil), e.fixedArgs...), args...), suffix...)
}

// Run executes kopia with given arguments and returns the output lines.
func (e *CLITest) Run(t *testing.T, expectedError bool, args ...string) (stdout, stderr []string, err error) {
	t.Helper()

	c := exec.Command(e.Exe, e.cmdArgs(args)...)
	c.Env = append(os.Environ(), e.Environment...)

	t.Logf("running '%v %v'", c.Path, c.Args)

	errOut := &bytes.Buffer{}
	c.Stderr = errOut

	if e.PassthroughStderr {
		c.Stderr = os.Stderr
	}

	c.Stdin = e.NextCommandStdin
	e.NextCommandStdin = nil

	o, err := c.Output()

	if err != nil && !expectedError {
		t.Logf("finished 'kopia %v' with err=%v (expected=%v) and output:\n%v\nstderr:\n%v\n", strings.Join(args, " "), err, expectedError, trimOutput(string(o)), trimOutput(errOut.String()))
	}

	return splitLines(string(o)), splitLines(errOut.String()), err
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
	t.Helper()

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
	t.Helper()

	lines := e.RunAndExpectSuccess(t, append([]string{"ls", "-l"}, targets...)...)

	return mustParseDirectoryEntries(lines)
}

// ListDirectoryRecursive lists a given directory recursively and returns directory entries.
func (e *CLITest) ListDirectoryRecursive(t *testing.T, targets ...string) []DirEntry {
	t.Helper()

	lines := e.RunAndExpectSuccess(t, append([]string{"ls", "-lr"}, targets...)...)

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

// DirectoryTreeOptions lists options for CreateDirectoryTree.
type DirectoryTreeOptions struct {
	Depth                              int
	MaxSubdirsPerDirectory             int
	MaxFilesPerDirectory               int
	MaxSymlinksPerDirectory            int
	MaxFileSize                        int
	MinNameLength                      int
	MaxNameLength                      int
	NonExistingSymlinkTargetPercentage int // 0..100
}

// MaybeSimplifyFilesystem applies caps to the provided DirectoryTreeOptions to reduce
// test time on ARM.
func MaybeSimplifyFilesystem(o DirectoryTreeOptions) DirectoryTreeOptions {
	if !testutil.ShouldReduceTestComplexity() {
		return o
	}

	if o.Depth > 2 {
		o.Depth = 2
	}

	if o.MaxFilesPerDirectory > 5 {
		o.MaxFilesPerDirectory = 5
	}

	if o.MaxSubdirsPerDirectory > 3 {
		o.MaxFilesPerDirectory = 3
	}

	if o.MaxSymlinksPerDirectory > 3 {
		o.MaxSymlinksPerDirectory = 3
	}

	if o.MaxFileSize > 100000 {
		o.MaxFileSize = 100000
	}

	return o
}

// DirectoryTreeCounters stores stats about files and directories created by CreateDirectoryTree.
type DirectoryTreeCounters struct {
	Files         int
	Directories   int
	Symlinks      int
	TotalFileSize int64
	MaxFileSize   int64
}

// MustCreateDirectoryTree creates a directory tree of a given depth with random files.
func MustCreateDirectoryTree(t *testing.T, dirname string, options DirectoryTreeOptions) {
	t.Helper()

	var counters DirectoryTreeCounters
	if err := createDirectoryTreeInternal(dirname, options, &counters); err != nil {
		t.Fatal(err)
	}

	t.Logf("created directory tree %#v", counters)
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
	t.Helper()

	if err := CreateRandomFile(filePath, options, counters); err != nil {
		t.Fatal(err)
	}
}

// CreateRandomFile creates a new file at the provided path with randomized contents.
func CreateRandomFile(filePath string, options DirectoryTreeOptions, counters *DirectoryTreeCounters) error {
	if counters == nil {
		counters = &DirectoryTreeCounters{}
	}

	return createRandomFile(filePath, options, counters)
}

// createDirectoryTreeInternal creates a directory tree of a given depth with random files.
func createDirectoryTreeInternal(dirname string, options DirectoryTreeOptions, counters *DirectoryTreeCounters) error {
	if err := os.MkdirAll(dirname, 0o700); err != nil {
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

	var fileNames []string

	if options.MaxFilesPerDirectory > 0 {
		numFiles := rand.Intn(options.MaxFilesPerDirectory) + 1
		for i := 0; i < numFiles; i++ {
			fileName := randomName(options)

			if err := createRandomFile(filepath.Join(dirname, fileName), options, counters); err != nil {
				return errors.Wrap(err, "unable to create random file")
			}

			fileNames = append(fileNames, fileName)
		}
	}

	if options.MaxSymlinksPerDirectory > 0 {
		numSymlinks := rand.Intn(options.MaxSymlinksPerDirectory) + 1
		for i := 0; i < numSymlinks; i++ {
			fileName := randomName(options)

			if err := createRandomSymlink(filepath.Join(dirname, fileName), fileNames, options, counters); err != nil {
				return errors.Wrap(err, "unable to create random symlink")
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
	defer f.Close()

	maxFileSize := int64(intOrDefault(options.MaxFileSize, 100000))

	length := rand.Int63n(maxFileSize)

	_, err = iocopy.Copy(f, io.LimitReader(rand.New(rand.NewSource(clock.Now().UnixNano())), length))
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

func createRandomSymlink(filename string, existingFiles []string, options DirectoryTreeOptions, counters *DirectoryTreeCounters) error {
	counters.Symlinks++

	if len(existingFiles) == 0 || rand.Intn(100) < options.NonExistingSymlinkTargetPercentage {
		return os.Symlink(randomName(options), filename)
	}

	return os.Symlink(existingFiles[rand.Intn(len(existingFiles))], filename)
}

func mustParseSnapshots(t *testing.T, lines []string) []SourceInfo {
	t.Helper()

	var (
		result        []SourceInfo
		currentSource *SourceInfo
	)

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

var globalRandomNameCounter = new(int32)

func randomName(opt DirectoryTreeOptions) string {
	maxNameLength := intOrDefault(opt.MaxNameLength, 15)
	minNameLength := intOrDefault(opt.MinNameLength, 3)

	l := rand.Intn(maxNameLength-minNameLength+1) + minNameLength
	b := make([]byte, (l+1)/2)

	cryptorand.Read(b)

	return fmt.Sprintf("%v.%v", hex.EncodeToString(b)[:l], atomic.AddInt32(globalRandomNameCounter, 1))
}

func mustParseSnaphotInfo(t *testing.T, l string) SnapshotInfo {
	t.Helper()

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
	t.Helper()

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
