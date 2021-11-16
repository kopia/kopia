package cli_test

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/repo/blob/sharded"
	"github.com/kopia/kopia/tests/testenv"
)

func TestBlobShardsModify(t *testing.T) {
	env := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewInProcRunner(t))

	env.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", env.RepoDir)

	someQBlob := strings.Split(env.RunAndExpectSuccess(t, "blob", "list", "--prefix=q")[0], " ")[0]

	// verify default sharding is 1,3
	require.FileExists(t, filepath.Join(env.RepoDir, someQBlob[0:1], someQBlob[1:4], someQBlob[4:]+sharded.CompleteBlobSuffix))
	require.FileExists(t, filepath.Join(env.RepoDir, "kopia.repository.f"))

	env.RunAndExpectSuccess(t, "blob", "shards", "modify", "--path", env.RepoDir, "--default-shards=5,5", "--i-am-sure-kopia-is-not-running")

	// verify new sharding is 5,5
	require.FileExists(t, filepath.Join(env.RepoDir, someQBlob[0:5], someQBlob[5:10], someQBlob[10:]+sharded.CompleteBlobSuffix))
	require.NoFileExists(t, filepath.Join(env.RepoDir, someQBlob[0:3], someQBlob[3:6], someQBlob[6:]+sharded.CompleteBlobSuffix))
	require.FileExists(t, filepath.Join(env.RepoDir, "kopia.repository.f"))

	env.RunAndExpectSuccess(t, "blob", "shards", "modify", "--path", env.RepoDir, "--unsharded-length=0", "--i-am-sure-kopia-is-not-running")

	require.FileExists(t, filepath.Join(env.RepoDir, someQBlob[0:5], someQBlob[5:10], someQBlob[10:]+sharded.CompleteBlobSuffix))
	require.FileExists(t, filepath.Join(env.RepoDir, "kopia/.repo/sitory.f"))
	require.NoFileExists(t, filepath.Join(env.RepoDir, "kopia.repository.f"))

	env.RunAndExpectSuccess(t, "blob", "shards", "modify", "--path", env.RepoDir, "--override=kop=2,,,2", "--i-am-sure-kopia-is-not-running")
	require.FileExists(t, filepath.Join(env.RepoDir, "ko/pi/a.repository.f"))
	require.NoFileExists(t, filepath.Join(env.RepoDir, "kopia.repository.f"))

	env.RunAndExpectSuccess(t, "blob", "shards", "modify", "--path", env.RepoDir, "--remove-override=nosuchprefix", "--remove-override=kop", "--i-am-sure-kopia-is-not-running")
	require.FileExists(t, filepath.Join(env.RepoDir, "kopia/.repo/sitory.f"))

	env.RunAndExpectSuccess(t, "blob", "shards", "modify", "--path", env.RepoDir, "--i-am-sure-kopia-is-not-running")

	env.RunAndExpectSuccess(t, "blob", "shards", "modify", "--path", env.RepoDir, "--override=kop=flat", "--i-am-sure-kopia-is-not-running", "--dry-run")
	require.FileExists(t, filepath.Join(env.RepoDir, "kopia/.repo/sitory.f"))

	env.RunAndExpectSuccess(t, "blob", "shards", "modify", "--path", env.RepoDir, "--override=kop=flat", "--i-am-sure-kopia-is-not-running")
	require.FileExists(t, filepath.Join(env.RepoDir, "kopia.repository.f"))

	env.RunAndExpectSuccess(t, "blob", "shards", "modify", "--path", env.RepoDir, "--override=kop=4,4", "--i-am-sure-kopia-is-not-running")
	require.FileExists(t, filepath.Join(env.RepoDir, "kopi/a.re/pository.f"))

	// some invalid cases
	env.RunAndExpectFailure(t, "blob", "shards", "modify", "--path", env.RepoDir, "--default-shards=invalid", "--i-am-sure-kopia-is-not-running")
	env.RunAndExpectFailure(t, "blob", "shards", "modify", "--path", env.RepoDir, "--override=x", "--i-am-sure-kopia-is-not-running")
	env.RunAndExpectFailure(t, "blob", "shards", "modify", "--path", env.RepoDir, "--override=x=aaa", "--i-am-sure-kopia-is-not-running")
	env.RunAndExpectFailure(t, "blob", "shards", "modify", "--path", env.RepoDir, "--override=2,-1", "--i-am-sure-kopia-is-not-running")
}
