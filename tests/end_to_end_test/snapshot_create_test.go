package endtoend_test

import (
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/kylelemons/godebug/pretty"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/tests/testenv"
)

func TestSnapshotCreate(t *testing.T) {
	t.Parallel()

	e := testenv.NewCLITest(t)

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

	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir2)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir2)

	sources := e.ListSnapshotsAndExpectSuccess(t)
	// will only list snapshots we created, not foo@foo
	if got, want := len(sources), 3; got != want {
		t.Errorf("unexpected number of sources: %v, want %v in %#v", got, want, sources)
	}
}

func TestStartTimeOverride(t *testing.T) {
	t.Parallel()

	e := testenv.NewCLITest(t)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1, "--start-time", "2000-01-01 01:01:00 UTC")
	sources := e.ListSnapshotsAndExpectSuccess(t)

	gotTime := sources[0].Snapshots[0].Time
	wantTime, _ := time.Parse("2006-01-02 15:04:05 MST", "2000-01-01 01:01:00 UTC")

	if !gotTime.Equal(wantTime) {
		t.Errorf("unexpected start time returned: %v, want %v in %#v", gotTime, wantTime, sources)
	}

	e.RunAndExpectFailure(t, "snapshot", "create", sharedTestDataDir1, "--start-time", "2000-01-01")
}

func TestEndTimeOverride(t *testing.T) {
	t.Parallel()

	e := testenv.NewCLITest(t)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1, "--end-time", "2000-01-01 01:01:00 UTC")
	sources := e.ListSnapshotsAndExpectSuccess(t)

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

	e := testenv.NewCLITest(t)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)
	e.RunAndExpectFailure(t, "snapshot", "create", sharedTestDataDir1, "--start-time", "2000-01-01 01:01:00 UTC", "--end-time", "1999-01-01 01:01:00 UTC")
}

func TestSnapshottingCacheDirectory(t *testing.T) {
	t.Parallel()

	e := testenv.NewCLITest(t)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)
	lines := e.RunAndExpectSuccess(t, "cache", "info")
	cachePath := filepath.Dir(strings.Split(lines[0], ": ")[0])

	// verify cache marker exists
	if _, err := os.Stat(filepath.Join(cachePath, repo.CacheDirMarkerFile)); err != nil {
		t.Fatal(err)
	}

	e.RunAndExpectSuccess(t, "snapshot", "create", cachePath)
	snapshots := e.ListSnapshotsAndExpectSuccess(t, cachePath)

	rootID := snapshots[0].Snapshots[0].ObjectID
	if got, want := len(e.RunAndExpectSuccess(t, "ls", rootID)), 0; got != want {
		t.Errorf("invalid number of files in snapshot of cache dir %v, want %v", got, want)
	}
}

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
		// {
		// 	desc: "multiple_ignore_files_1",
		// 	files: []testFileEntry{
		// 		{
		// 			Name: "/.kopiaignore",
		// 			Content: []string{
		// 				"# Exclude everything except *.txt files anywhere.",
		// 				"*",
		// 				"!*.txt  ", // trailing spaces intentional
		// 				"!*/",
		// 				"AB/",
		// 			},
		// 		},
		// 		{
		// 			Name: "A/.kopiaignore",
		// 			Content: []string{
		// 				"*.txt",
		// 				"# Negate *.go from the file above in the hierarchy.",
		// 				"!*.go",
		// 			},
		// 		},
		// 		{Name: "A/file.txt"},
		// 		{Name: "A/file.go"},
		// 		{Name: "A/AA/file.txt"},
		// 		{Name: "A/AA/file.go"},
		// 		{Name: "A/AB/file.txt"},
		// 		{Name: "A/AB/file.go"},
		// 		{Name: "B/file.txt"},
		// 		{Name: "B/file.go"},
		// 		{Name: "B/AA/file.txt"},
		// 		{Name: "B/AA/file.go"},
		// 		{Name: "B/AB/file.txt"},
		// 		{Name: "B/AB/file.go"},
		// 	},
		// 	expected: []string{
		// 		"A/file.go",
		// 		"A/AA/file.go",
		// 		"B/file.txt",
		// 		"B/AA/file.txt",
		// 	},
		// },
		// {
		// 	desc: "multiple_ignore_files_2",
		// 	files: []testFileEntry{
		// 		{
		// 			Name: "/.kopiaignore",
		// 			Content: []string{
		// 				"# Exclude everything except *.txt files anywhere.",
		// 				"*",
		// 				"!*.txt  ", // trailing spaces intentional
		// 				"!*/",
		// 				"AB/",
		// 			},
		// 		},
		// 		{
		// 			Name: "A/.kopiaignore",
		// 			Content: []string{
		// 				"*.txt",
		// 				"# Negate *.go from the file above in the hierarchy.",
		// 				"!*.go",
		// 				"!/AB/",
		// 			},
		// 		},
		// 		{
		// 			Name: "A/AB/.kopiaignore",
		// 			Content: []string{
		// 				"!*.txt",
		// 			},
		// 		},
		// 		{Name: "A/file.txt"},
		// 		{Name: "A/file.go"},
		// 		{Name: "A/AA/file.txt"},
		// 		{Name: "A/AA/file.go"},
		// 		{Name: "A/AB/file.txt"},
		// 		{Name: "A/AB/file.go"},
		// 		{Name: "B/file.txt"},
		// 		{Name: "B/file.go"},
		// 		{Name: "B/AA/file.txt"},
		// 		{Name: "B/AA/file.go"},
		// 		{Name: "B/AB/file.txt"},
		// 		{Name: "B/AB/file.go"},
		// 	},
		// 	expected: []string{
		// 		"A/file.go",
		// 		"A/AA/file.go",
		// 		"A/AB/file.go",
		// 		"A/AB/file.txt",
		// 		"B/file.txt",
		// 		"B/AA/file.txt",
		// 	},
		// },
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			e := testenv.NewCLITest(t)

			baseDir := t.TempDir()

			if err := createFileStructure(baseDir, tc.files); err != nil {
				t.Fatal("Failed to create file structure", err)
			}

			defer e.RunAndExpectSuccess(t, "repo", "disconnect")
			e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)
			e.RunAndExpectSuccess(t, "snapshot", "create", baseDir)
			sources := e.ListSnapshotsAndExpectSuccess(t)
			oid := sources[0].Snapshots[0].ObjectID
			entries := e.ListDirectoryRecursive(t, oid)

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
						if strings.HasSuffix(d, "/") {
							d = d[:len(d)-1]
						}
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
			err := os.MkdirAll(fullPath, 0777)
			if err != nil {
				return errors.Errorf("failed to create directory %v: %v", fullPath, err)
			}
		} else {
			dir, _ := path.Split(fullPath)
			if dir != "" {
				if _, err := os.Stat(dir); os.IsNotExist(err) {
					err := os.MkdirAll(dir, 0777)
					if err != nil {
						return errors.Errorf("failed to create directory %v: %v", dir, err)
					}
				}
			}

			f, err := os.Create(fullPath)
			if err != nil {
				return errors.Errorf("failed to create file %v: %v", fullPath, err)
			}

			defer f.Close()

			if ent.Content != nil {
				for _, line := range ent.Content {
					f.WriteString(line)
					f.WriteString("\n")
				}
			} else {
				f.WriteString("Test data\n")
			}
		}
	}

	return nil
}
