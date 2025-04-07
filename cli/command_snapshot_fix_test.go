package cli_test

import (
	"bytes"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/cli"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/snapshotfs"
	"github.com/kopia/kopia/tests/testenv"
)

//nolint:maintidx
func TestSnapshotFix(t *testing.T) {
	srcDir1 := testutil.TempDirectory(t)

	if testutil.ShouldReduceTestComplexity() {
		return
	}

	// 300 bytes
	mustWriteFileWithRepeatedData(t, filepath.Join(srcDir1, "small-file1"), 1, bytes.Repeat([]byte{1, 2, 3}, 100))
	mustWriteFileWithRepeatedData(t, filepath.Join(srcDir1, "small-file1-dup"), 1, bytes.Repeat([]byte{1, 2, 3}, 100))
	mustWriteFileWithRepeatedData(t, filepath.Join(srcDir1, "small-file2"), 1, bytes.Repeat([]byte{1, 2, 4}, 100))

	require.NoError(t, os.MkdirAll(filepath.Join(srcDir1, "dir1"), 0o700))
	require.NoError(t, os.MkdirAll(filepath.Join(srcDir1, "dir2"), 0o700))

	// 3 x 3 x 1_000_000 bytes = 9 MB
	mustWriteFileWithRepeatedData(t, filepath.Join(srcDir1, "large-file1"), 3, bytes.Repeat([]byte{1, 2, 3}, 1000000))
	mustWriteFileWithRepeatedData(t, filepath.Join(srcDir1, "large-file1-dup"), 3, bytes.Repeat([]byte{1, 2, 3}, 1000000))
	mustWriteFileWithRepeatedData(t, filepath.Join(srcDir1, "large-file2"), 3, bytes.Repeat([]byte{1, 2, 4}, 1000000))

	mustWriteFileWithRepeatedData(t, filepath.Join(srcDir1, "dir1", "small-file1"), 1, bytes.Repeat([]byte{1, 1, 2, 3}, 100))
	mustWriteFileWithRepeatedData(t, filepath.Join(srcDir1, "dir1", "small-file1-dup"), 1, bytes.Repeat([]byte{1, 1, 2, 3}, 100))
	mustWriteFileWithRepeatedData(t, filepath.Join(srcDir1, "dir1", "small-file2"), 1, bytes.Repeat([]byte{1, 1, 2, 4}, 100))
	mustWriteFileWithRepeatedData(t, filepath.Join(srcDir1, "dir1", "large-file1"), 3, bytes.Repeat([]byte{1, 1, 2, 3}, 1000000))
	mustWriteFileWithRepeatedData(t, filepath.Join(srcDir1, "dir1", "large-file1-dup"), 3, bytes.Repeat([]byte{1, 1, 2, 3}, 1000000))
	mustWriteFileWithRepeatedData(t, filepath.Join(srcDir1, "dir1", "large-file2"), 3, bytes.Repeat([]byte{1, 1, 2, 4}, 1000000))

	mustWriteFileWithRepeatedData(t, filepath.Join(srcDir1, "dir2", "small-file1"), 1, bytes.Repeat([]byte{2, 1, 2, 3}, 100))
	mustWriteFileWithRepeatedData(t, filepath.Join(srcDir1, "dir2", "small-file1-dup"), 1, bytes.Repeat([]byte{2, 1, 2, 3}, 100))
	mustWriteFileWithRepeatedData(t, filepath.Join(srcDir1, "dir2", "small-file2"), 1, bytes.Repeat([]byte{2, 1, 2, 4}, 100))
	mustWriteFileWithRepeatedData(t, filepath.Join(srcDir1, "dir2", "large-file1"), 3, bytes.Repeat([]byte{2, 1, 2, 3}, 1000000))
	mustWriteFileWithRepeatedData(t, filepath.Join(srcDir1, "dir2", "large-file1-dup"), 3, bytes.Repeat([]byte{2, 1, 2, 3}, 1000000))
	mustWriteFileWithRepeatedData(t, filepath.Join(srcDir1, "dir2", "large-file2"), 3, bytes.Repeat([]byte{2, 1, 2, 4}, 1000000))

	cases := []struct {
		name                    string
		flags                   []string
		modifyRepoAfterSnapshot func(env *testenv.CLITest, man *snapshot.Manifest, fileMap map[string]*snapshot.DirEntry)
		initiallyCorrupted      bool
		wantRecoveredFiles      []string
		wantRootStub            bool
		wantFixFail             bool
		wantFailVerify          bool
	}{
		{
			name:                    "FixInvalidFiles_NoOp",
			modifyRepoAfterSnapshot: func(env *testenv.CLITest, man *snapshot.Manifest, fileMap map[string]*snapshot.DirEntry) {},
			flags:                   []string{"invalid-files"},
			wantRecoveredFiles: []string{
				"dir1",
				"dir1/large-file1",
				"dir1/large-file1-dup",
				"dir1/large-file2",
				"dir1/small-file1",
				"dir1/small-file1-dup",
				"dir1/small-file2",
				"dir2",
				"dir2/large-file1",
				"dir2/large-file1-dup",
				"dir2/large-file2",
				"dir2/small-file1",
				"dir2/small-file1-dup",
				"dir2/small-file2",
				"large-file1",
				"large-file1-dup",
				"large-file2",
				"small-file1",
				"small-file1-dup",
				"small-file2",
			},
		},
		{
			name: "FixInvalidFiles_MissingRootDirStub",
			modifyRepoAfterSnapshot: func(env *testenv.CLITest, man *snapshot.Manifest, fileMap map[string]*snapshot.DirEntry) {
				forgetContents(t, env,
					man.RootObjectID().String())
			},
			initiallyCorrupted: true,
			flags:              []string{"invalid-files"},
			wantRootStub:       true,
		},
		{
			name: "FixInvalidFiles_MissingRootDirFail",
			modifyRepoAfterSnapshot: func(env *testenv.CLITest, man *snapshot.Manifest, fileMap map[string]*snapshot.DirEntry) {
				forgetContents(t, env,
					man.RootObjectID().String())
			},
			initiallyCorrupted: true,
			flags:              []string{"invalid-files", "--invalid-directory-handling=fail"},
			wantFixFail:        true,
		},
		{
			name: "FixInvalidFiles_MissingRootDirKeep",
			modifyRepoAfterSnapshot: func(env *testenv.CLITest, man *snapshot.Manifest, fileMap map[string]*snapshot.DirEntry) {
				forgetContents(t, env,
					man.RootObjectID().String())
			},
			initiallyCorrupted: true,
			flags:              []string{"invalid-files", "--invalid-directory-handling=keep"},
			wantFailVerify:     true,
		},
		{
			name: "FixInvalidFiles_MissingShortContentFileRemove",
			modifyRepoAfterSnapshot: func(env *testenv.CLITest, man *snapshot.Manifest, fileMap map[string]*snapshot.DirEntry) {
				forgetContents(t, env,
					fileMap["small-file1"].ObjectID.String(),
					fileMap["dir1/small-file1"].ObjectID.String())
			},
			initiallyCorrupted: true,
			// recovered files
			flags: []string{"invalid-files", "--invalid-file-handling=remove"},
			wantRecoveredFiles: []string{
				"dir1",
				"dir1/large-file1",
				"dir1/large-file1-dup",
				"dir1/large-file2",
				"dir1/small-file2",
				"dir2",
				"dir2/large-file1",
				"dir2/large-file1-dup",
				"dir2/large-file2",
				"dir2/small-file1",
				"dir2/small-file1-dup",
				"dir2/small-file2",
				"large-file1",
				"large-file1-dup",
				"large-file2",
				"small-file2",
			},
		},
		{
			name: "FixInvalidFiles_MissingShortContentFileStub",
			modifyRepoAfterSnapshot: func(env *testenv.CLITest, man *snapshot.Manifest, fileMap map[string]*snapshot.DirEntry) {
				forgetContents(t, env,
					fileMap["small-file1"].ObjectID.String(),
					fileMap["dir1/small-file1"].ObjectID.String())
			},
			initiallyCorrupted: true,
			flags:              []string{"invalid-files"},
			// recovered files
			wantRecoveredFiles: []string{
				".INVALID.small-file1",
				".INVALID.small-file1-dup",
				"dir1",
				"dir1/.INVALID.small-file1",
				"dir1/.INVALID.small-file1-dup",
				"dir1/large-file1",
				"dir1/large-file1-dup",
				"dir1/large-file2",
				"dir1/small-file2",
				"dir2",
				"dir2/large-file1",
				"dir2/large-file1-dup",
				"dir2/large-file2",
				"dir2/small-file1",
				"dir2/small-file1-dup",
				"dir2/small-file2",
				"large-file1",
				"large-file1-dup",
				"large-file2",
				"small-file2",
			},
		},
		{
			name: "FixInvalidFiles_MissingShortContentFileKeep",
			modifyRepoAfterSnapshot: func(env *testenv.CLITest, man *snapshot.Manifest, fileMap map[string]*snapshot.DirEntry) {
				forgetContents(t, env,
					fileMap["small-file1"].ObjectID.String(),
					fileMap["dir1/small-file1"].ObjectID.String())
			},
			initiallyCorrupted: true,
			flags:              []string{"invalid-files", "--invalid-file-handling=keep"},
			wantFailVerify:     true,
		},
		{
			name: "FixInvalidFiles_MissingShortContentDir",
			modifyRepoAfterSnapshot: func(env *testenv.CLITest, man *snapshot.Manifest, fileMap map[string]*snapshot.DirEntry) {
				forgetContents(t, env,
					fileMap["dir1"].ObjectID.String())
			},
			initiallyCorrupted: true,
			flags:              []string{"invalid-files", "--invalid-directory-handling=stub"},
			wantRecoveredFiles: []string{
				".INVALID.dir1",
				"dir2",
				"dir2/large-file1",
				"dir2/large-file1-dup",
				"dir2/large-file2",
				"dir2/small-file1",
				"dir2/small-file1-dup",
				"dir2/small-file2",
				"large-file1",
				"large-file1-dup",
				"large-file2",
				"small-file1",
				"small-file1-dup",
				"small-file2",
			},
		},
		{
			name: "FixInvalidFiles_MissingLargeFileIndex",
			modifyRepoAfterSnapshot: func(env *testenv.CLITest, man *snapshot.Manifest, fileMap map[string]*snapshot.DirEntry) {
				forgetContents(t, env,
					strings.TrimPrefix(fileMap["large-file1"].ObjectID.String(), "I"),
					strings.TrimPrefix(fileMap["dir1/large-file1"].ObjectID.String(), "I"))
			},
			initiallyCorrupted: true,
			flags:              []string{"invalid-files", "--invalid-file-handling=remove"},
			wantRecoveredFiles: []string{
				"dir1",
				"dir1/large-file2",
				"dir1/small-file1",
				"dir1/small-file1-dup",
				"dir1/small-file2",
				"dir2",
				"dir2/large-file1",
				"dir2/large-file1-dup",
				"dir2/large-file2",
				"dir2/small-file1",
				"dir2/small-file1-dup",
				"dir2/small-file2",
				"large-file2",
				"small-file1",
				"small-file1-dup",
				"small-file2",
			},
		},
		{
			name:                    "FixRemoveFiles_ByFileName",
			modifyRepoAfterSnapshot: func(env *testenv.CLITest, man *snapshot.Manifest, fileMap map[string]*snapshot.DirEntry) {},
			flags:                   []string{"remove-files", "--filename=small-file2", "--filename=large-file1-dup"},
			wantRecoveredFiles: []string{
				"dir1",
				"dir1/large-file1",
				"dir1/large-file2",
				"dir1/small-file1",
				"dir1/small-file1-dup",
				"dir2",
				"dir2/large-file1",
				"dir2/large-file2",
				"dir2/small-file1",
				"dir2/small-file1-dup",
				"large-file1",
				"large-file2",
				"small-file1",
				"small-file1-dup",
			},
		},
		{
			name:                    "FixRemoveFiles_ByWildcard",
			modifyRepoAfterSnapshot: func(env *testenv.CLITest, man *snapshot.Manifest, fileMap map[string]*snapshot.DirEntry) {},
			flags:                   []string{"remove-files", "--filename=small-*", "--filename=*-dup"},
			wantRecoveredFiles: []string{
				"dir1",
				"dir1/large-file1",
				"dir1/large-file2",
				"dir2",
				"dir2/large-file1",
				"dir2/large-file2",
				"large-file1",
				"large-file2",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runner := testenv.NewInProcRunner(t)
			env := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

			env.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", env.RepoDir)

			var man1, man2 snapshot.Manifest

			testutil.MustParseJSONLines(t, env.RunAndExpectSuccess(t, "snapshot", "create", srcDir1, "--json"), &man1)
			testutil.MustParseJSONLines(t, env.RunAndExpectSuccess(t, "snapshot", "create", srcDir1, "--json"), &man2)

			fileMap := mustGetFileMap(t, env, man1.RootObjectID())

			tc.modifyRepoAfterSnapshot(env, &man1, fileMap)

			if tc.initiallyCorrupted {
				env.RunAndExpectFailure(t, "snapshot", "verify")
			} else {
				env.RunAndExpectFailure(t, "snapshot", "success")
			}

			if tc.wantFixFail {
				env.RunAndExpectFailure(t, append([]string{"snapshot", "fix"}, tc.flags...)...)
				env.RunAndExpectFailure(t, append(append([]string{"snapshot", "fix"}, tc.flags...), "--commit")...)
				env.RunAndExpectFailure(t, "snapshot", "verify")
				return
			}

			// this does not commit fixes
			env.RunAndExpectSuccess(t, append([]string{"snapshot", "fix"}, tc.flags...)...)

			if tc.initiallyCorrupted {
				// snapshot verify still fails
				env.RunAndExpectFailure(t, "snapshot", "verify")
			} else {
				env.RunAndExpectFailure(t, "snapshot", "success")
			}

			env.RunAndExpectSuccess(t, append(append([]string{"snapshot", "fix"}, tc.flags...), "--commit")...)

			if tc.wantFailVerify {
				env.RunAndExpectFailure(t, "snapshot", "verify")
				return
			}

			env.RunAndExpectSuccess(t, "snapshot", "verify")

			var manifests []cli.SnapshotManifest

			testutil.MustParseJSONLines(t, env.RunAndExpectSuccess(t, "snapshot", "list", "--json"), &manifests)
			require.Len(t, manifests, 2)

			// make sure all root entries have been fixed the same way
			require.Equal(t, manifests[0].RootEntry, manifests[1].RootEntry)

			switch {
			case tc.wantRecoveredFiles != nil:
				var remainingFiles []string

				for f := range mustGetFileMap(t, env, manifests[0].RootObjectID()) {
					remainingFiles = append(remainingFiles, f)
				}

				sort.Strings(remainingFiles)
				require.Equal(t, tc.wantRecoveredFiles, remainingFiles)

			case tc.wantRootStub:
				var stub snapshotfs.UnreadableDirEntryReplacement

				testutil.MustParseJSONLines(t, env.RunAndExpectSuccess(t, "show", manifests[0].RootObjectID().String()), &stub)
			}
		})
	}
}

// forgetContents rewrites contents into a new blob and deletes the blob
// making index entries dangling.
func forgetContents(t *testing.T, env *testenv.CLITest, contentIDs ...string) {
	t.Helper()

	before := mustGetContentMap(t, env)

	env.RunAndExpectSuccess(t, append([]string{"content", "rewrite", "--safety=none"}, contentIDs...)...)

	after := mustGetContentMap(t, env)

	var blobIDs []string

	for _, cidStr := range contentIDs {
		cid, err := content.ParseID(cidStr)
		require.NoError(t, err)
		require.NotEqual(t, before[cid].PackBlobID, after[cid].PackBlobID)
		blobIDs = append(blobIDs, string(after[cid].PackBlobID))
	}

	env.RunAndExpectSuccess(t, append([]string{"blob", "rm"}, blobIDs...)...)
}

func mustGetContentMap(t *testing.T, env *testenv.CLITest) map[content.ID]content.Info {
	t.Helper()

	var contents1 []content.Info

	testutil.MustParseJSONLines(t, env.RunAndExpectSuccess(t, "content", "ls", "--json"), &contents1)

	contentMap := map[content.ID]content.Info{}
	for _, v := range contents1 {
		contentMap[v.ContentID] = v
	}

	return contentMap
}

func mustGetFileMap(t *testing.T, env *testenv.CLITest, root object.ID) map[string]*snapshot.DirEntry {
	t.Helper()

	fileMap := map[string]*snapshot.DirEntry{}
	mustListDirEntries(t, env, fileMap, root, "")

	return fileMap
}

func mustListDirEntries(t *testing.T, env *testenv.CLITest, out map[string]*snapshot.DirEntry, root object.ID, prefix string) {
	t.Helper()

	var dir1 snapshot.DirManifest

	testutil.MustParseJSONLines(t, env.RunAndExpectSuccess(t, "show", root.String()), &dir1)

	for _, v := range dir1.Entries {
		out[prefix+v.Name] = v

		if v.Type == snapshot.EntryTypeDirectory {
			mustListDirEntries(t, env, out, v.ObjectID, prefix+v.Name+"/")
		}
	}
}

func mustWriteFileWithRepeatedData(t *testing.T, fname string, repeat int, data []byte) {
	t.Helper()

	f, err := os.Create(fname)
	require.NoError(t, err)

	defer f.Close()

	for range repeat {
		_, err := f.Write(data)
		require.NoError(t, err)
	}
}
