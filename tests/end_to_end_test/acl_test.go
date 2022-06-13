package endtoend_test

import (
	"testing"

	"github.com/kopia/kopia/internal/auth"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/tests/clitestutil"
	"github.com/kopia/kopia/tests/testenv"
)

func TestACL(t *testing.T) {
	t.Parallel()

	serverRunner := testenv.NewExeRunner(t)
	serverEnvironment := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, serverRunner)

	defer serverEnvironment.RunAndExpectSuccess(t, "repo", "disconnect")

	serverEnvironment.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", serverEnvironment.RepoDir, "--override-hostname=foo", "--override-username=foo", "--enable-actions")

	if got, want := len(serverEnvironment.RunAndExpectSuccess(t, "server", "acl", "list")), 0; got != want {
		t.Fatalf("unexpected ACLs found")
	}

	// enable ACLs - that should insert all the rules.
	serverEnvironment.RunAndExpectSuccess(t, "server", "acl", "enable")

	if got, want := len(serverEnvironment.RunAndExpectSuccess(t, "server", "acl", "list")), len(auth.DefaultACLs); got != want {
		t.Fatalf("unexpected ACLs found")
	}

	// add read access to all snapshots and policies for user foo@bar
	serverEnvironment.RunAndExpectSuccess(t, "server", "acl", "add", "--user", "foo@bar", "--target", "type=snapshot", "--access=READ")
	serverEnvironment.RunAndExpectSuccess(t, "server", "acl", "add", "--user", "foo@bar", "--target", "type=policy", "--access=READ")

	// add full access to global policy for all users
	serverEnvironment.RunAndExpectSuccess(t, "server", "acl", "add", "--user", "*@*", "--target", "type=policy,policyType=global", "--access=FULL")

	serverEnvironment.RunAndExpectSuccess(t, "server", "users", "add", "foo@bar", "--user-password", "baz")
	serverEnvironment.RunAndExpectSuccess(t, "server", "users", "add", "alice@wonderland", "--user-password", "baz")

	var sp testutil.ServerParameters

	_, kill := serverEnvironment.RunAndProcessStderr(t, sp.ProcessOutput,
		"server", "start",
		"--address=localhost:0",
		"--server-control-username=admin-user",
		"--server-control-password=admin-pwd",
		"--tls-generate-cert",
		"--tls-generate-rsa-key-size=2048", // use shorter key size to speed up generation
	)

	t.Logf("detected server parameters %#v", sp)

	defer kill()

	fooBarRunner := testenv.NewExeRunner(t)
	foobarClientEnvironment := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, fooBarRunner)

	defer foobarClientEnvironment.RunAndExpectSuccess(t, "repo", "disconnect")

	fooBarRunner.RemoveDefaultPassword()

	// connect as foo@bar with password baz
	foobarClientEnvironment.RunAndExpectSuccess(t, "repo", "connect", "server",
		"--url", sp.BaseURL+"/",
		"--server-cert-fingerprint", sp.SHA256Fingerprint,
		"--override-username", "foo",
		"--override-hostname", "bar",
		"--password", "baz",
	)

	aliceInWonderlandRunner := testenv.NewExeRunner(t)
	aliceInWonderlandClientEnvironment := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, aliceInWonderlandRunner)

	defer aliceInWonderlandClientEnvironment.RunAndExpectSuccess(t, "repo", "disconnect")

	aliceInWonderlandRunner.RemoveDefaultPassword()

	// connect as alice@wonderland with password baz
	aliceInWonderlandClientEnvironment.RunAndExpectSuccess(t, "repo", "connect", "server",
		"--url", sp.BaseURL+"/",
		"--server-cert-fingerprint", sp.SHA256Fingerprint,
		"--override-username", "alice",
		"--override-hostname", "wonderland",
		"--password", "baz",
	)

	// both alice and foo@bar can see global policy
	foobarClientEnvironment.RunAndExpectSuccess(t, "policy", "get", "--global")
	aliceInWonderlandClientEnvironment.RunAndExpectSuccess(t, "policy", "get", "--global")

	foobarClientEnvironment.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)

	// foo@bar sees one snapshot
	if snaps := clitestutil.ListSnapshotsAndExpectSuccess(t, foobarClientEnvironment, "-a"); len(snaps) != 1 {
		t.Fatalf("foo@bar expected to see 1 sources (own, got %v", snaps)
	}

	// alice@wonderland sees zero sources
	if snaps := clitestutil.ListSnapshotsAndExpectSuccess(t, aliceInWonderlandClientEnvironment, "-a"); len(snaps) != 0 {
		t.Fatalf("foo@bar expected to see 0 sources (own), got %v", snaps)
	}

	// alice@wonderland takes a snapshot now
	aliceInWonderlandClientEnvironment.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)

	// foo@bar now can see two snapshot sources (own and alice's)
	if snaps := clitestutil.ListSnapshotsAndExpectSuccess(t, foobarClientEnvironment, "-a"); len(snaps) != 2 {
		t.Fatalf("foo@bar expected to see 2 sources (own and alice), got %v", snaps)
	}

	// alice@wonderland can only see her own
	if snaps := clitestutil.ListSnapshotsAndExpectSuccess(t, aliceInWonderlandClientEnvironment, "-a"); len(snaps) != 1 {
		t.Fatalf("foo@bar expected to see 1 source (own), got %v", snaps)
	}

	// alice changes her own password and reconnects
	aliceInWonderlandClientEnvironment.RunAndExpectSuccess(t, "server", "users", "set", "alice@wonderland", "--user-password", "new-password")

	// refresh the auth cache using admin username/password.
	serverEnvironment.RunAndExpectSuccess(t, "server", "refresh",
		"--address", sp.BaseURL,
		"--server-username", "admin-user",
		"--server-password", "admin-pwd",
		"--server-cert-fingerprint", sp.SHA256Fingerprint,
	)

	// attempt to use foo@bar's credentials when refreshing, this will fail.
	serverEnvironment.RunAndExpectFailure(t, "server", "refresh",
		"--address", sp.BaseURL,
		"--server-username", "foo@bar",
		"--server-password", "baz",
		"--server-cert-fingerprint", sp.SHA256Fingerprint,
	)

	aliceInWonderlandClientEnvironment.RunAndExpectSuccess(t, "repo", "disconnect")
	aliceInWonderlandClientEnvironment.RunAndExpectSuccess(t, "repo", "connect", "server",
		"--url", sp.BaseURL+"/",
		"--server-cert-fingerprint", sp.SHA256Fingerprint,
		"--override-username", "alice",
		"--override-hostname", "wonderland",
		"--password", "new-password",
	)
}
