package ignorefs_test

import (
	"bytes"
	"sort"
	"testing"

	"github.com/kylelemons/godebug/pretty"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/ignorefs"
	"github.com/kopia/kopia/internal/mockfs"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/snapshot/policy"
)

var (
	dummyFileContents      = []byte("dummy")
	tooLargeFileContents   = bytes.Repeat([]byte("dummy"), 1000000) // 5MB
	notSoLargeFileContents = tooLargeFileContents[0 : len(tooLargeFileContents)-1]
)

func setupFilesystem() *mockfs.Directory {
	root := mockfs.NewDirectory()
	root.AddFile("file1", dummyFileContents, 0)
	root.AddFile("file2", dummyFileContents, 0)
	root.AddFile("file3", notSoLargeFileContents, 0)
	root.AddFile("ignored-by-rule", dummyFileContents, 0)
	root.AddFile("largefile1", tooLargeFileContents, 0)

	d1 := root.AddDir("bin", 0)
	d2 := root.AddDir("pkg", 0)
	d3 := root.AddDir("src", 0)

	d1.AddFile("some-bin", dummyFileContents, 0)
	d2.AddFile("some-pkg", dummyFileContents, 0)

	d4 := d3.AddDir("some-src", 0)
	d4.AddFile("f1", dummyFileContents, 0)

	return root
}

var defaultPolicy = policy.BuildTree(map[string]*policy.Policy{
	".": {
		FilesPolicy: policy.FilesPolicy{
			DotIgnoreFiles: []string{
				".kopiaignore",
			},
			MaxFileSize: int64(len(tooLargeFileContents)) - 1,
			IgnoreRules: []string{
				"*-by-rule",
			},
		},
	},
}, policy.DefaultPolicy)

var rootAndSrcPolicy = policy.BuildTree(map[string]*policy.Policy{
	".": {
		FilesPolicy: policy.FilesPolicy{
			DotIgnoreFiles: []string{
				".kopiaignore",
			},
			MaxFileSize: int64(len(tooLargeFileContents)) - 1,
			IgnoreRules: []string{
				"*-by-rule",
			},
		},
	},
	"./src": {
		FilesPolicy: policy.FilesPolicy{
			DotIgnoreFiles: []string{
				".newignore",
			},
			IgnoreRules: []string{
				"some-*",
			},
		},
	},
}, policy.DefaultPolicy)

var cases = []struct {
	desc         string
	policyTree   *policy.Tree
	setup        func(root *mockfs.Directory)
	addedFiles   []string
	ignoredFiles []string
}{
	{desc: "null policy, missing dotignore"},
	{
		desc:       "default policy missing dotignore",
		policyTree: defaultPolicy,
		addedFiles: nil,
		ignoredFiles: []string{
			"./ignored-by-rule",
			"./largefile1",
		},
	},
	{
		desc:       "default policy, have dotignore",
		policyTree: defaultPolicy,
		setup: func(root *mockfs.Directory) {
			root.AddFileLines(".kopiaignore", []string{"file[12]"}, 0)
		},
		addedFiles: []string{"./.kopiaignore"},
		ignoredFiles: []string{
			"./ignored-by-rule",
			"./largefile1",
			"./file1",
			"./file2",
		},
	},
	{
		desc:       "default policy, have dotignore #2",
		policyTree: defaultPolicy,
		setup: func(root *mockfs.Directory) {
			root.AddFileLines(".kopiaignore", []string{
				"pkg",
				"file*",
			}, 0)
		},
		addedFiles: []string{"./.kopiaignore"},
		ignoredFiles: []string{
			"./ignored-by-rule",
			"./largefile1",
			"./file1",
			"./file2",
			"./file3",
			"./pkg/",
			"./pkg/some-pkg",
		},
	},
	{
		desc:       "default policy, have dotignore #3",
		policyTree: defaultPolicy,
		setup: func(root *mockfs.Directory) {
			root.AddFileLines(".kopiaignore", []string{
				"pkg",
				"file*",
			}, 0)
		},
		addedFiles: []string{"./.kopiaignore"},
		ignoredFiles: []string{
			"./ignored-by-rule",
			"./largefile1",
			"./file1",
			"./file2",
			"./file3",
			"./pkg/",
			"./pkg/some-pkg",
		},
	},
	{
		desc:       "default policy, have dotignore #4",
		policyTree: defaultPolicy,
		setup: func(root *mockfs.Directory) {
			root.AddFileLines(".kopiaignore", []string{
				"file[12]",
				"**/some-src",
				"bin/",
			}, 0)
		},
		addedFiles: []string{"./.kopiaignore"},
		ignoredFiles: []string{
			"./ignored-by-rule",
			"./largefile1",
			"./file1",
			"./file2",
			"./bin/",
			"./bin/some-bin",
			"./src/some-src/",
			"./src/some-src/f1",
		},
	},
	{
		desc:       "two policies, nested policy excludes files",
		policyTree: rootAndSrcPolicy,
		ignoredFiles: []string{
			"./ignored-by-rule",
			"./largefile1",
			"./src/some-src/", // excluded by policy at './src'
			"./src/some-src/f1",
		},
	},
	{
		desc: "non-root policy excludes files",
		setup: func(root *mockfs.Directory) {
			root.Subdir("src").AddFileLines(".extraignore", []string{
				"zzz",
			}, 0)
			root.Subdir("src").AddFile("yyy", dummyFileContents, 0)
			root.Subdir("src").AddFile("zzz", dummyFileContents, 0)         // ignored by .extraignore
			root.Subdir("src").AddFile("another-yyy", dummyFileContents, 0) // ignored by policy rule
			root.AddFile("zzz", dummyFileContents, 0)                       // not ignored, at parent level
		},
		policyTree: policy.BuildTree(map[string]*policy.Policy{
			"./src": {
				FilesPolicy: policy.FilesPolicy{
					IgnoreRules: []string{
						"some-*",
						"another-*",
					},
					DotIgnoreFiles: []string{
						".extraignore",
					},
				},
			},
		}, policy.DefaultPolicy),
		addedFiles: []string{
			"./src/.extraignore",
			"./src/yyy",
			"./zzz",
		},
		ignoredFiles: []string{
			"./src/some-src/", // excluded by policy at './src'
			"./src/some-src/f1",
		},
	},
}

func TestIgnoreFS(t *testing.T) {
	for _, tc := range cases {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			root := setupFilesystem()
			originalFiles := walkTree(t, root)

			if tc.setup != nil {
				tc.setup(root)
			}
			ifs := ignorefs.New(root, tc.policyTree)

			expectedFiles := addAndSubtractFiles(originalFiles, tc.addedFiles, tc.ignoredFiles)
			verifyDirectoryTree(t, ifs, expectedFiles)
		})
	}
}

func addAndSubtractFiles(original, added, removed []string) []string {
	m := map[string]bool{}
	for _, ri := range removed {
		m[ri] = true
	}

	var result []string

	for _, ai := range added {
		if !m[ai] {
			m[ai] = true

			result = append(result, ai)
		}
	}

	for _, oi := range original {
		if !m[oi] {
			result = append(result, oi)
		}
	}

	sort.Strings(result)

	return result
}

func walkTree(t *testing.T, dir fs.Directory) []string {
	var output []string

	var walk func(path string, d fs.Directory) error

	walk = func(path string, d fs.Directory) error {
		output = append(output, path+"/")

		entries, err := d.Readdir(testlogging.Context(t))
		if err != nil {
			return err
		}

		for _, e := range entries {
			relPath := path + "/" + e.Name()

			if subdir, ok := e.(fs.Directory); ok {
				if err := walk(relPath, subdir); err != nil {
					return err
				}
			} else {
				output = append(output, relPath)
			}
		}

		return nil
	}

	if err := walk(".", dir); err != nil {
		t.Fatalf("error walking tree: %v", err)
	}

	return output
}

func verifyDirectoryTree(t *testing.T, dir fs.Directory, expected []string) {
	t.Helper()

	output := walkTree(t, dir)

	if diff := pretty.Compare(output, expected); diff != "" {
		t.Errorf("unexpected directory tree, diff(-got,+want): %v\n", diff)
	}
}
