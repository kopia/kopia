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

	dev1 := fs.DeviceInfo{Dev: 1, Rdev: 0}
	dev2 := fs.DeviceInfo{Dev: 2, Rdev: 0}

	d1 := root.AddDir("bin", 0)
	d2 := root.AddDirDevice("pkg", 0, dev1)
	d3 := root.AddDirDevice("src", 0, dev2)

	d1.AddFile("some-bin", dummyFileContents, 0)
	d2.AddFileDevice("some-pkg", dummyFileContents, 0, dev1)

	d4 := d3.AddDirDevice("some-src", 0, dev2)
	d4.AddFileDevice("f1", dummyFileContents, 0, dev2)

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

var trueValue = true

var oneFileSystemPolicy = policy.BuildTree(map[string]*policy.Policy{
	".": {
		FilesPolicy: policy.FilesPolicy{
			OneFileSystem: &trueValue,
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
	{
		desc:       "policy with one-file-system",
		policyTree: oneFileSystemPolicy,
		addedFiles: nil,
		ignoredFiles: []string{
			"./pkg/",
			"./pkg/some-pkg",
			"./src/",
			"./src/some-src/",
			"./src/some-src/f1",
		},
	},
	{
		desc: "absolut match",
		setup: func(root *mockfs.Directory) {
			root.Subdir("src").AddFileLines(".extraignore", []string{
				"/sub/*.foo",
			}, 0)
			root.Subdir("src").AddDir("sub", 0)
			root.Subdir("src").Subdir("sub").AddFile("a.foo", dummyFileContents, 0) // ignored by .extraignore
			root.Subdir("src").Subdir("sub").AddFile("b.fooX", dummyFileContents, 0)
			root.Subdir("src").Subdir("sub").AddFile("foo", dummyFileContents, 0)
			root.Subdir("src").AddFile("c.foo", dummyFileContents, 0) // not ignored, at parent level
		},
		policyTree: policy.BuildTree(map[string]*policy.Policy{
			"./src": {
				FilesPolicy: policy.FilesPolicy{
					DotIgnoreFiles: []string{
						".extraignore",
					},
				},
			},
		}, policy.DefaultPolicy),
		addedFiles: []string{
			"./src/.extraignore",
			"./src/sub/",
			"./src/sub/b.fooX",
			"./src/sub/foo",
			"./src/c.foo",
		},
		ignoredFiles: []string{
			"./src/sub/a.foo",
		},
	},
	// Requeres major refactoring of ignore logic: https://github.com/kopia/kopia/pull/496#issuecomment-678790009
	// {
	// 	desc: "exclude include",
	// 	setup: func(root *mockfs.Directory) {
	// 		root.Subdir("src").AddFileLines(".extraignore", []string{
	// 			"/sub/*.foo",
	// 			"!/sub/special.foo",
	// 		}, 0)
	// 		root.Subdir("src").AddDir("sub", 0)
	// 		root.Subdir("src").Subdir("sub").AddFile("ignore.foo", dummyFileContents, 0) // ignored by wildcard rule
	// 		root.Subdir("src").Subdir("sub").AddFile("special.foo", dummyFileContents, 0) // explicitly included
	// 	},
	// 	policyTree: policy.BuildTree(map[string]*policy.Policy{
	// 		"./src": {
	// 			FilesPolicy: policy.FilesPolicy{
	// 				DotIgnoreFiles: []string{
	// 					".extraignore",
	// 				},
	// 			},
	// 		},
	// 	}, policy.DefaultPolicy),
	// 	addedFiles: []string{
	// 		"./src/.extraignore",
	// 		"./src/sub/",
	// 		"./src/sub/special.foo",
	// 	},
	// 	ignoredFiles: []string{
	// 		"./src/sub/ignore.foo",
	// 	},
	// },
	//  Not supported according to spec: https://git-scm.com/docs/gitignore#_pattern_format
	// {
	// 	desc: "exclude include wildcard",
	// 	setup: func(root *mockfs.Directory) {
	// 		root.Subdir("src").AddFileLines(".extraignore", []string{
	// 		  ".config/",
	// 			"!.config/App/**/special/",
	// 		}, 0)
	// 		root.Subdir("src").AddDir(".config", 0)
	// 		root.Subdir("src").Subdir(".config").AddDir("App", 0)
	// 		root.Subdir("src").Subdir(".config").Subdir("App").AddDir("some", 0)
	// 		root.Subdir("src").Subdir(".config").Subdir("App").Subdir("some").AddDir("thing", 0)
	// 		root.Subdir("src").Subdir(".config").Subdir("App").Subdir("some").Subdir("thing").AddFile("ignored_file.txt", dummyFileContents, 0)
	// 		root.Subdir("src").Subdir(".config").Subdir("App").Subdir("some").Subdir("thing").AddDir("special", 0)
	// 		root.Subdir("src").Subdir(".config").Subdir("App").Subdir("some").Subdir("thing").Subdir("special").AddFile("included_file.txt", dummyFileContents, 0)
	// 	},
	// 	policyTree: policy.BuildTree(map[string]*policy.Policy{
	// 		"./src": {
	// 			FilesPolicy: policy.FilesPolicy{
	// 				DotIgnoreFiles: []string{
	// 					".extraignore",
	// 				},
	// 			},
	// 		},
	// 	}, policy.DefaultPolicy),
	// 	addedFiles: []string{
	// 		"./src/.config/App/",
	// 		"./src/.config/App/some/",
	// 		"./src/.config/App/some/thing/",
	// 		"./src/.config/App/some/thing/special/",
	// 		"./src/.config/App/some/thing/special/included_file.txt",
	// 	},
	// 	ignoredFiles: []string{
	// 		"./src/.config/App/some/thing/ignored_file.txt",
	// 	},
	// },
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
