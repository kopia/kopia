package endtoend_test

import (
	"testing"

	"github.com/kopia/kopia/internal/auth"
	"github.com/kopia/kopia/tests/testenv"
)

func TestACL(t *testing.T) {
	t.Parallel()

	serverEnvironment := testenv.NewCLITest(t)
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
	serverEnvironment.RunAndExpectSuccess(t, "server", "acl", "add", "--user", "*@*", "--target", "type=policy,policytype=global", "--access=FULL")

	serverEnvironment.RunAndExpectSuccess(t, "users", "add", "foo@bar", "--user-password", "baz")
	serverEnvironment.RunAndExpectSuccess(t, "users", "add", "alice@wonderland", "--user-password", "baz")

	var sp serverParameters

	srv := serverEnvironment.RunAndProcessStderr(t, sp.ProcessOutput,
		"server", "start",
		"--address=localhost:0",
		"--tls-generate-cert",
		"--tls-generate-rsa-key-size=2048", // use shorter key size to speed up generation
		"--allow-repository-users",
	)

	t.Logf("detected server parameters %#v", sp)

	defer srv.Process.Kill()

	foobarClientEnvironment := testenv.NewCLITest(t)
	defer foobarClientEnvironment.RunAndExpectSuccess(t, "repo", "disconnect")

	foobarClientEnvironment.RemoveDefaultPassword()

	// connect as foo@bar with password baz
	foobarClientEnvironment.RunAndExpectSuccess(t, "repo", "connect", "server",
		"--url", sp.baseURL+"/",
		"--server-cert-fingerprint", sp.sha256Fingerprint,
		"--override-username", "foo",
		"--override-hostname", "bar",
		"--password", "baz",
	)

	aliceInWonderlandClientEnvironment := testenv.NewCLITest(t)
	defer aliceInWonderlandClientEnvironment.RunAndExpectSuccess(t, "repo", "disconnect")

	aliceInWonderlandClientEnvironment.RemoveDefaultPassword()

	// connect as alice@wonderland with password baz
	aliceInWonderlandClientEnvironment.RunAndExpectSuccess(t, "repo", "connect", "server",
		"--url", sp.baseURL+"/",
		"--server-cert-fingerprint", sp.sha256Fingerprint,
		"--override-username", "alice",
		"--override-hostname", "wonderland",
		"--password", "baz",
	)

	// both alice and foo@bar can see global policy
	foobarClientEnvironment.RunAndExpectSuccess(t, "policy", "get", "--global")
	aliceInWonderlandClientEnvironment.RunAndExpectSuccess(t, "policy", "get", "--global")

	foobarClientEnvironment.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)

	// foo@bar sees one snapshot
	if snaps := foobarClientEnvironment.ListSnapshotsAndExpectSuccess(t, "-a"); len(snaps) != 1 {
		t.Fatalf("foo@bar expected to see 1 sources (own, got %v", snaps)
	}

	// alice@wonderland sees zero sources
	if snaps := aliceInWonderlandClientEnvironment.ListSnapshotsAndExpectSuccess(t, "-a"); len(snaps) != 0 {
		t.Fatalf("foo@bar expected to see 0 sources (own), got %v", snaps)
	}

	// alice@wonderland takes a snapshot now
	aliceInWonderlandClientEnvironment.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)

	// foo@bar now can see two snapshot sources (own and alice's)
	if snaps := foobarClientEnvironment.ListSnapshotsAndExpectSuccess(t, "-a"); len(snaps) != 2 {
		t.Fatalf("foo@bar expected to see 2 sources (own and alice), got %v", snaps)
	}

	// alice@wonderland can only see her own
	if snaps := aliceInWonderlandClientEnvironment.ListSnapshotsAndExpectSuccess(t, "-a"); len(snaps) != 1 {
		t.Fatalf("foo@bar expected to see 1 source (own), got %v", snaps)
	}

	// alice changes her own password and reconnects
	aliceInWonderlandClientEnvironment.RunAndExpectSuccess(t, "users", "set", "alice@wonderland", "--user-password", "new-password")
	serverEnvironment.RunAndExpectSuccess(t, "server", "refresh",
		"--address", sp.baseURL,
		"--server-username", "alice@wonderland",
		"--server-password", "baz",
		"--server-cert-fingerprint", sp.sha256Fingerprint,
	)
	aliceInWonderlandClientEnvironment.RunAndExpectSuccess(t, "repo", "disconnect")
	aliceInWonderlandClientEnvironment.RunAndExpectSuccess(t, "repo", "connect", "server",
		"--url", sp.baseURL+"/",
		"--server-cert-fingerprint", sp.sha256Fingerprint,
		"--override-username", "alice",
		"--override-hostname", "wonderland",
		"--password", "new-password",
	)
}
