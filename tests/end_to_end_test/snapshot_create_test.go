package endtoend_test

import (
	"os"
	"path"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/kylelemons/godebug/pretty"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/cli"
	"github.com/kopia/kopia/internal/cachedir"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
	"github.com/kopia/kopia/tests/clitestutil"
	"github.com/kopia/kopia/tests/testenv"
)

func TestSnapshotCreate(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	// create some snapshots using different hostname/username
	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir, "--override-hostname=foo", "--override-username=foo")
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir3)
	e.RunAndExpectSuccess(t, "snapshot", "list", sharedTestDataDir3)
	e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "connect", "filesystem", "--path", e.RepoDir)

	e.RunAndExpectSuccess(t, "snapshot", "create", ".")
	e.RunAndExpectSuccess(t, "snapshot", "list", ".")

	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)

	var man1, man2 snapshot.Manifest

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir2, "--json"), &man1)
	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir2, "--json"), &man2)

	require.NotEmpty(t, man1.ID)
	require.NotEmpty(t, man2.ID)
	require.NotEmpty(t, man1.RootEntry.ObjectID)
	require.NotEqual(t, man1.ID, man2.ID)
	require.Equal(t, man1.RootEntry.ObjectID, man2.RootEntry.ObjectID)

	var manifests []cli.SnapshotManifest

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "snapshot", "list", "-a", "--json"), &manifests)
	require.Len(t, manifests, 6)

	var manifests2 []cli.SnapshotManifest

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "snapshot", "list", "-a", "--json", "--max-results=1"), &manifests2)

	uniqueSources := map[snapshot.SourceInfo]bool{}
	for _, m := range manifests2 {
		uniqueSources[m.Source] = true
	}

	// make sure we got one snapshot per source.
	require.Len(t, manifests2, len(uniqueSources))

	sources := clitestutil.ListSnapshotsAndExpectSuccess(t, e)
	require.Len(t, sources, 3)

	// test ignore-identical-snapshot
	e.RunAndExpectSuccess(t, "policy", "set", "--global", "--ignore-identical-snapshots", "true")
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir2)

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "snapshot", "list", "-a", "--json"), &manifests)
	require.Len(t, manifests, 6)
}

func TestTagging(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1, "--tags", "testkey1:testkey2")
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)

	var manifests []cli.SnapshotManifest

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "snapshot", "list", "-a", "--json"), &manifests)

	if got, want := len(manifests), 2; got != want {
		t.Fatalf("unexpected number of snapshots %v want %v", got, want)
	}

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "snapshot", "list", "-a", "--tags", "testkey1:testkey2", "--json"), &manifests)

	if got, want := len(manifests), 1; got != want {
		t.Fatalf("unexpected number of snapshots %v want %v", got, want)
	}
}

func TestSnapshotInterval(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	e.RunAndExpectFailure(t, "snapshot", "create", "--checkpoint-interval=1h", sharedTestDataDir1)
	e.RunAndExpectFailure(t, "snapshot", "create", "--checkpoint-interval=46m", sharedTestDataDir1)
	e.RunAndExpectFailure(t, "snapshot", "create", "--checkpoint-interval=45m1s", sharedTestDataDir1)
	e.RunAndExpectSuccess(t, "snapshot", "create", "--checkpoint-interval=45m", sharedTestDataDir1)
}

func TestTaggingBadTags(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	for _, tc := range [][]string{
		{"--tags", "testkey1:testkey2", "--tags", "testkey1:testkey2"},
		{"--tags", "badtag"},
	} {
		args := []string{"snapshot", "create", sharedTestDataDir1}
		args = append(args, tc...)
		e.RunAndExpectFailure(t, args...)
	}
}

func TestStartTimeOverride(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1, "--start-time", "2000-01-01 01:01:00 UTC")
	sources := clitestutil.ListSnapshotsAndExpectSuccess(t, e)

	gotTime := sources[0].Snapshots[0].Time
	wantTime, _ := time.Parse("2006-01-02 15:04:05 MST", "2000-01-01 01:01:00 UTC")

	if !gotTime.Equal(wantTime) {
		t.Errorf("unexpected start time returned: %v, want %v in %#v", gotTime, wantTime, sources)
	}

	e.RunAndExpectFailure(t, "snapshot", "create", sharedTestDataDir1, "--start-time", "2000-01-01")
}

func TestEndTimeOverride(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1, "--end-time", "2000-01-01 01:01:00 UTC")
	sources := clitestutil.ListSnapshotsAndExpectSuccess(t, e)

	gotTime := sources[0].Snapshots[0].Time
	wantTime, _ := time.Parse("2006-01-02 15:04:05 MST", "2000-01-01 01:01:00 UTC")

	// If we set the end time then start time should be calculated to be <total snapshot time> seconds before it
	if !gotTime.Before(wantTime) {
		t.Errorf("end time unexpectedly before start time: %v, wanted before %v in %#v", gotTime, wantTime, sources)
	}

	e.RunAndExpectFailure(t, "snapshot", "create", sharedTestDataDir1, "--end-time", "2000-01-01")
}

func TestInvalidTimeOverride(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)
	e.RunAndExpectFailure(t, "snapshot", "create", sharedTestDataDir1, "--start-time", "2000-01-01 01:01:00 UTC", "--end-time", "1999-01-01 01:01:00 UTC")
}

func TestSnapshottingCacheDirectory(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)
	lines := e.RunAndExpectSuccess(t, "cache", "info")
	cachePath := filepath.Dir(strings.Split(lines[0], ": ")[0])

	// verify cache marker exists
	if _, err := os.Stat(filepath.Join(cachePath, cachedir.CacheDirMarkerFile)); err != nil {
		t.Fatal(err)
	}

	e.RunAndExpectSuccess(t, "snapshot", "create", cachePath)
	snapshots := clitestutil.ListSnapshotsAndExpectSuccess(t, e, cachePath)

	rootID := snapshots[0].Snapshots[0].ObjectID
	if got, want := len(e.RunAndExpectSuccess(t, "ls", rootID)), 0; got != want {
		t.Errorf("invalid number of files in snapshot of cache dir %v, want %v", got, want)
	}
}

//nolint:maintidx
func TestSnapshotCreateWithIgnore(t *testing.T) {
	cases := []struct {
		desc     string
		files    []testFileEntry
		expected []string
	}{
		{
			desc: "ignore_all_recursive",
			files: []testFileEntry{
				{
					Name: "/.kopiaignore",
					Content: []string{
						"**",
					},
				},
				{Name: "foo/bar/file1.txt"},
				{Name: "foo/bar/file2.txt"},
				{Name: "foo/file1.txt"},
				{Name: "foo/file2.txt"},
				{Name: "file1.txt"},
				{Name: "file2.txt"},
			},
			expected: []string{},
		},
		{
			desc: "ignore_all_but_text_files",
			files: []testFileEntry{
				{
					Name: "/.kopiaignore",
					Content: []string{
						"*",
						"!*.txt",
						"!*/",
					},
				},
				{Name: "foo/bar/file.txt"},
				{Name: "foo/bar/file.png"},
				{Name: "foo/file.txt"},
				{Name: "foo/file.jpg"},
				{Name: "car/car.jpg"},
				{Name: "file.txt"},
				{Name: "file.bmp"},
			},
			expected: []string{
				"foo/",
				"foo/bar/",
				"car/", // all files in this are ignored, but the directory is still included.
				"foo/bar/file.txt",
				"foo/file.txt",
				"file.txt",
			},
		},
		{
			desc: "ignore_rooted_vs_unrooted_1",
			files: []testFileEntry{
				{
					Name: "/.kopiaignore",
					Content: []string{
						"/A/",
					},
				},
				{Name: "file.txt"},
				{Name: "/A/file.txt"},
				{Name: "/A/AA/file.txt"},
				{Name: "/A/B/A/file.txt"},
				{Name: "/B/A/file.txt"},
			},
			expected: []string{
				".kopiaignore",
				"file.txt",
				"B/A/file.txt",
			},
		},
		{
			desc: "ignore_rooted_vs_unrooted_2",
			files: []testFileEntry{
				{
					Name: "/.kopiaignore",
					Content: []string{
						"A/",
					},
				},
				{Name: "file.txt"},
				{Name: "/A/file.txt"},
				{Name: "/A/AA/file.txt"},
				{Name: "/A/B/A/file.txt"},
				{Name: "/B/A/file.txt"},
			},
			expected: []string{
				".kopiaignore",
				"file.txt",
				"B/", // directory is empty because all contents are ignored, but the empty directory is still included.
			},
		},
		{
			desc: "ignore_rooted_vs_unrooted_3",
			files: []testFileEntry{
				{
					Name: "/.kopiaignore",
					Content: []string{
						"B/A/",
					},
				},
				{Name: "file.txt"},
				{Name: "/A/file.txt"},
				{Name: "/A/AA/file.txt"},
				{Name: "/A/B/A/file.txt"},
				{Name: "/B/A/file.txt"},
			},
			expected: []string{
				".kopiaignore",
				"file.txt",
				"A/file.txt",
				"A/AA/file.txt",
				"A/B/A/file.txt",
				"B/", // directory is empty because all contents are ignored, but the empty directory is still included.
			},
		},
		{
			desc: "ignore_rooted_vs_unrooted_4",
			files: []testFileEntry{
				{
					Name: "/.kopiaignore",
					Content: []string{
						"/B/A/",
					},
				},
				{Name: "file.txt"},
				{Name: "/A/file.txt"},
				{Name: "/A/AA/file.txt"},
				{Name: "/A/B/A/file.txt"},
				{Name: "/B/A/file.txt"},
			},
			expected: []string{
				".kopiaignore",
				"file.txt",
				"A/file.txt",
				"A/AA/file.txt",
				"A/B/A/file.txt",
				"B/", // directory is empty because all contents are ignored, but the empty directory is still included.
			},
		},
		{
			desc: "ignore_rooted_vs_unrooted_5",
			files: []testFileEntry{
				{
					Name: "/.kopiaignore",
					Content: []string{
						"**/B/A/",
					},
				},
				{Name: "file.txt"},
				{Name: "/A/file.txt"},
				{Name: "/A/AA/file.txt"},
				{Name: "/A/B/A/file.txt"},
				{Name: "/B/A/file.txt"},
			},
			expected: []string{
				".kopiaignore",
				"file.txt",
				"A/file.txt",
				"A/AA/file.txt",
				"A/B/", // directory is empty because all contents are ignored, but the empty directory is still included.
				"B/",   // directory is empty because all contents are ignored, but the empty directory is still included.
			},
		},
		{
			desc: "ignore_rooted_vs_unrooted_6",
			files: []testFileEntry{
				{
					Name: "/.kopiaignore",
					Content: []string{
						"# This is a comment  ",
						"/test1",
					},
				},
				{Name: "A/test1"},
				{Name: "test1"},
			},
			expected: []string{
				".kopiaignore",
				"A/test1",
			},
		},
		{
			desc: "multiple_ignore_files_1",
			files: []testFileEntry{
				{
					Name: "/.kopiaignore",
					Content: []string{
						"# Exclude everything except *.txt files anywhere.",
						"*",
						"!*.txt  ", // trailing spaces intentional
						"!*/",
						"AB/",
					},
				},
				{
					Name: "A/.kopiaignore",
					Content: []string{
						"*.txt",
						"# Negate *.go from the file above in the hierarchy.",
						"!*.go",
					},
				},
				{Name: "A/file.txt"},
				{Name: "A/file.go"},
				{Name: "A/AA/file.txt"},
				{Name: "A/AA/file.go"},
				{Name: "A/AB/file.txt"},
				{Name: "A/AB/file.go"},
				{Name: "B/file.txt"},
				{Name: "B/file.go"},
				{Name: "B/AA/file.txt"},
				{Name: "B/AA/file.go"},
				{Name: "B/AB/file.txt"},
				{Name: "B/AB/file.go"},
			},
			expected: []string{
				"A/file.go",
				"A/AA/file.go",
				"B/file.txt",
				"B/AA/file.txt",
			},
		},
		{
			desc: "multiple_ignore_files_2",
			files: []testFileEntry{
				{
					Name: "/.kopiaignore",
					Content: []string{
						"# Exclude everything except *.txt files anywhere.",
						"*",
						"!*.txt  ", // trailing spaces intentional
						"!*/",
						"AB/",
					},
				},
				{
					Name: "A/.kopiaignore",
					Content: []string{
						"*.txt",
						"# Negate *.go from the file above in the hierarchy.",
						"!*.go",
						"!/AB/",
					},
				},
				{
					Name: "A/AB/.kopiaignore",
					Content: []string{
						"!*.txt",
					},
				},
				{Name: "A/file.txt"},
				{Name: "A/file.go"},
				{Name: "A/AA/file.txt"},
				{Name: "A/AA/file.go"},
				{Name: "A/AB/file.txt"},
				{Name: "A/AB/file.go"},
				{Name: "B/file.txt"},
				{Name: "B/file.go"},
				{Name: "B/AA/file.txt"},
				{Name: "B/AA/file.go"},
				{Name: "B/AB/file.txt"},
				{Name: "B/AB/file.go"},
			},
			expected: []string{
				"A/file.go",
				"A/AA/file.go",
				"A/AB/file.go",
				"A/AB/file.txt",
				"B/file.txt",
				"B/AA/file.txt",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			runner := testenv.NewInProcRunner(t)
			e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

			baseDir := testutil.TempDirectory(t)

			if err := createFileStructure(baseDir, tc.files); err != nil {
				t.Fatal("Failed to create file structure", err)
			}

			defer e.RunAndExpectSuccess(t, "repo", "disconnect")
			e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)
			e.RunAndExpectSuccess(t, "snapshot", "create", baseDir)
			sources := clitestutil.ListSnapshotsAndExpectSuccess(t, e)
			oid := sources[0].Snapshots[0].ObjectID
			entries := clitestutil.ListDirectoryRecursive(t, e, oid)

			var output []string
			for _, s := range entries {
				output = append(output, s.Name)
			}

			// Automatically add all directories of files specified so we don't have to specify all
			// directories and subdirectories in the expected list.
			var expected []string
			for _, ex := range tc.expected {
				expected = appendIfMissing(expected, ex)
				if !strings.HasSuffix(ex, "/") {
					for d, _ := path.Split(ex); d != ""; d, _ = path.Split(d) {
						expected = appendIfMissing(expected, d)
						d = strings.TrimSuffix(d, "/")
					}
				}
			}

			sort.Strings(output)
			sort.Strings(expected)
			if diff := pretty.Compare(output, expected); diff != "" {
				t.Errorf("unexpected directory tree, diff(-got,+want): %v\n", diff)
			}
		})
	}
}

func TestSnapshotCreateAllWithManualSnapshot(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")
	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir2)

	sourceSnapshotCount := len(e.RunAndExpectSuccess(t, "snapshot", "list", "-a"))

	// set manual field in the scheduling policy for `sharedTestDataDir1`
	e.RunAndExpectSuccess(t, "policy", "set", "--manual", sharedTestDataDir1)

	// make sure the policy is visible in the policy list, includes global policy
	e.RunAndVerifyOutputLineCount(t, 2, "policy", "list")

	// create snapshot for all sources
	e.RunAndExpectSuccess(t, "snapshot", "create", "--all")

	// snapshot count must increase by 1 since `sharedTestDataDir1` is ignored
	expectedSnapshotCount := sourceSnapshotCount + 1
	e.RunAndVerifyOutputLineCount(t, expectedSnapshotCount, "snapshot", "list", "--show-identical", "-a")
}

func TestSnapshotCreateWithStdinStream(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")
	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	// Create a temporary pipe file with test data
	content := []byte("Streaming Temporary file content")

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("error creating pipe file: %v", err)
	}

	if _, err = w.Write(content); err != nil {
		t.Fatalf("error writing to pipe file: %v", err)
	}

	w.Close()

	streamFileName := "stream-file"

	runner.SetNextStdin(r)

	e.RunAndExpectSuccess(t, "snapshot", "create", "rootdir", "--stdin-file", streamFileName)

	// Make sure the scheduling policy with manual field is set and visible in the policy list, includes global policy
	e.RunAndVerifyOutputLineCount(t, 2, "policy", "list")

	// Obtain snapshot root id and use it for restore
	si := clitestutil.ListSnapshotsAndExpectSuccess(t, e)
	if got, want := len(si), 1; got != want {
		t.Fatalf("got %v sources, wanted %v", got, want)
	}

	if got, want := len(si[0].Snapshots), 1; got != want {
		t.Fatalf("got %v snapshots, wanted %v", got, want)
	}

	rootID := si[0].Snapshots[0].ObjectID

	// Restore using <root-id>/stream-file directly
	restoredStreamFile := path.Join(testutil.TempDirectory(t), streamFileName)
	e.RunAndExpectSuccess(t, "snapshot", "restore", rootID+"/"+streamFileName, restoredStreamFile)

	// Compare restored data with content
	rFile, err := os.Open(restoredStreamFile)
	if err != nil {
		t.Fatalf("error opening restored file: %v", err)
	}

	gotContent := make([]byte, len(content))

	if _, err := rFile.Read(gotContent); err != nil {
		t.Fatalf("error reading restored file: %v", err)
	}

	if !reflect.DeepEqual(gotContent, content) {
		t.Fatalf("did not get expected file contents: (actual) %v != %v (expected)", gotContent, content)
	}
}

func appendIfMissing(slice []string, i string) []string {
	for _, ele := range slice {
		if ele == i {
			return slice
		}
	}

	return append(slice, i)
}

type testFileEntry struct {
	Name    string
	Content []string
}

func createFileStructure(baseDir string, files []testFileEntry) error {
	for _, ent := range files {
		fullPath := path.Join(baseDir, ent.Name)

		if strings.HasSuffix(ent.Name, "/") {
			err := os.MkdirAll(fullPath, 0o777)
			if err != nil {
				return errors.Errorf("failed to create directory %v: %v", fullPath, err)
			}
		} else {
			dir, _ := path.Split(fullPath)
			if dir != "" {
				if _, err := os.Stat(dir); os.IsNotExist(err) {
					err := os.MkdirAll(dir, 0o777)
					if err != nil {
						return errors.Errorf("failed to create directory %v: %v", dir, err)
					}
				}
			}

			f, err := os.Create(fullPath)
			if err != nil {
				return errors.Errorf("failed to create file %v: %v", fullPath, err)
			}

			if ent.Content != nil {
				for _, line := range ent.Content {
					f.WriteString(line)
					f.WriteString("\n")
				}
			} else {
				f.WriteString("Test data\n")
			}

			f.Close()
		}
	}

	return nil
}

func TestSnapshotCreateAllFlushPerSource(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir, "--override-hostname=foo", "--override-username=foo")
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir2)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir3)
	indexList1 := e.RunAndExpectSuccess(t, "index", "ls")
	metadataBlobList1 := e.RunAndExpectSuccess(t, "blob", "ls", "--prefix=q")

	// by default snapshot flushes once, at the end and creates one index.
	e.RunAndExpectSuccess(t, "snapshot", "create", "--all")

	indexList2 := e.RunAndExpectSuccess(t, "index", "ls")
	metadataBlobList2 := e.RunAndExpectSuccess(t, "blob", "ls", "--prefix=q")

	require.Len(t, indexList2, len(indexList1)+1)
	require.Len(t, metadataBlobList2, len(metadataBlobList1)+1)

	// snapshot with --flush-per-source, since there are 3 soufces, we'll have 3 index blobs
	e.RunAndExpectSuccess(t, "snapshot", "create", "--all", "--flush-per-source")

	indexList3 := e.RunAndExpectSuccess(t, "index", "ls")
	metadataBlobList3 := e.RunAndExpectSuccess(t, "blob", "ls", "--prefix=q")

	require.Len(t, indexList3, len(indexList2)+3)
	require.Len(t, metadataBlobList3, len(metadataBlobList2)+3)
}

func TestSnapshotCreateAllSnapshotPath(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir, "--override-hostname=foo", "--override-username=foo")
	e.RunAndExpectSuccess(t, "snapshot", "create", "--override-source", "bar@bar:/foo/bar", sharedTestDataDir1)
	e.RunAndExpectSuccess(t, "snapshot", "create", "--override-source", "bar@bar:C:\\foo\\baz", sharedTestDataDir2)
	e.RunAndExpectSuccess(t, "snapshot", "create", "--override-source", "/foo/bar", sharedTestDataDir3)

	// Make sure the scheduling policy with manual field is set and visible in the policy list, includes global policy
	var plist []policy.TargetWithPolicy

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "policy", "list", "--json"), &plist)

	if got, want := len(plist), 4; got != want {
		t.Fatalf("got %v policies, wanted %v", got, want)
	}

	// all non-global policies should be manual
	for _, p := range plist {
		if (p.Target != snapshot.SourceInfo{}) {
			require.True(t, p.Policy.SchedulingPolicy.Manual)
		}
	}

	si := clitestutil.ListSnapshotsAndExpectSuccess(t, e, "--all")
	if got, want := len(si), 3; got != want {
		t.Fatalf("got %v sources, wanted %v", got, want)
	}

	require.Equal(t, "bar", si[0].User)
	require.Equal(t, "bar", si[0].Host)
	require.Equal(t, "/foo/bar", si[0].Path)

	require.Equal(t, "bar", si[1].User)
	require.Equal(t, "bar", si[1].Host)
	require.Equal(t, "C:\\foo\\baz", si[1].Path)

	require.Equal(t, "foo", si[2].User)
	require.Equal(t, "foo", si[2].Host)

	if runtime.GOOS == "windows" {
		require.Regexp(t, `[A-Z]:\\foo\\bar`, si[2].Path)
	} else {
		require.Equal(t, "/foo/bar", si[2].Path)
	}
}

func TestSnapshotCreateWithAllAndPath(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	// creating a snapshot with a directory and --all should fail
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir2)
	e.RunAndExpectFailure(t, "snapshot", "create", sharedTestDataDir1, "--all")
}
