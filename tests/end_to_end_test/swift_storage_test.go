package endtoend_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack"
	"github.com/gophercloud/gophercloud/v2/openstack/objectstorage/v1/containers"
	"github.com/gophercloud/gophercloud/v2/openstack/objectstorage/v1/objects"
	"github.com/gophercloud/gophercloud/v2/pagination"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/blob/swift"
	"github.com/kopia/kopia/tests/clitestutil"
	"github.com/kopia/kopia/tests/testenv"
)

// TestSwiftStorage exercises the full CLI workflow (create → snapshot → restore → verify)
// against a real OpenStack Swift endpoint.
//
// Gated behind KOPIA_PROVIDER_TEST and KOPIA_SWIFT_CREDS environment variables.
// KOPIA_SWIFT_CREDS must be a JSON object matching swift.Options (container field is optional;
// if omitted, a temporary container is auto-created and deleted after the test).
func TestSwiftStorage(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	opts := mustGetSwiftOptions(t)
	ctx := context.Background()

	containerName := opts.ContainerName
	autoCreated := false

	if containerName == "" {
		containerName = fmt.Sprintf("kopia-e2e-%v", clock.Now().UnixNano())
		opts.ContainerName = containerName
		autoCreated = true
	}

	cli := mustCreateSwiftClient(t, ctx, opts)

	if autoCreated {
		res := containers.Create(ctx, cli, containerName, nil)
		require.NoError(t, res.Err, "failed to create test container")

		t.Cleanup(func() {
			deleteAllObjects(t, ctx, cli, containerName)
			containers.Delete(ctx, cli, containerName) //nolint:errcheck
		})
	}

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	createArgs := []string{
		"repo", "create", "swift",
		"--container", containerName,
		"--auth-url", opts.AuthURL,
		"--prefix", fmt.Sprintf("e2e-%v/", clock.Now().UnixNano()),
	}

	createArgs = appendSwiftAuthFlags(createArgs, opts)

	e.RunAndExpectSuccess(t, createArgs...)

	// snapshot
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)

	// list
	si := clitestutil.ListSnapshotsAndExpectSuccess(t, e, sharedTestDataDir1)
	if got, want := len(si), 1; got != want {
		t.Fatalf("got %v sources, wanted %v", got, want)
	}

	if got, want := len(si[0].Snapshots), 1; got != want {
		t.Fatalf("got %v snapshots, wanted %v", got, want)
	}

	// restore
	restoreDir := testutil.TempDirectory(t)
	rootID := si[0].Snapshots[0].ObjectID

	e.RunAndExpectSuccess(t, "snapshot", "restore", rootID, restoreDir)

	// verify
	e.RunAndExpectSuccess(t, "snapshot", "verify")
}

func mustGetSwiftOptions(t *testing.T) *swift.Options {
	t.Helper()

	v := os.Getenv("KOPIA_SWIFT_CREDS")
	if v == "" {
		t.Skip("KOPIA_SWIFT_CREDS is not set")
	}

	var opt swift.Options

	if err := json.NewDecoder(strings.NewReader(v)).Decode(&opt); err != nil {
		t.Fatalf("failed to parse KOPIA_SWIFT_CREDS: %v", err)
	}

	if opt.AuthURL == "" {
		t.Fatal("KOPIA_SWIFT_CREDS must specify authURL")
	}

	return &opt
}

func mustCreateSwiftClient(t *testing.T, ctx context.Context, opts *swift.Options) *gophercloud.ServiceClient {
	t.Helper()

	provider, err := openstack.NewClient(opts.AuthURL)
	require.NoError(t, err, "failed to create OpenStack client")

	authOpts := gophercloud.AuthOptions{
		IdentityEndpoint:            opts.AuthURL,
		Username:                    opts.Username,
		UserID:                      opts.UserID,
		Password:                    opts.Password,
		DomainName:                  opts.DomainName,
		DomainID:                    opts.DomainID,
		TenantName:                  opts.TenantName,
		TenantID:                    opts.TenantID,
		TokenID:                     opts.Token,
		ApplicationCredentialID:     opts.ApplicationCredentialID,
		ApplicationCredentialName:   opts.ApplicationCredentialName,
		ApplicationCredentialSecret: opts.ApplicationCredentialSecret,
		AllowReauth:                 true,
	}

	require.NoError(t, openstack.Authenticate(ctx, provider, authOpts), "failed to authenticate")

	cli, err := openstack.NewObjectStorageV1(provider, gophercloud.EndpointOpts{
		Region: opts.Region,
	})
	require.NoError(t, err, "failed to create object storage client")

	return cli
}

func deleteAllObjects(t *testing.T, ctx context.Context, cli *gophercloud.ServiceClient, containerName string) {
	t.Helper()

	err := objects.List(cli, containerName, nil).EachPage(ctx, func(_ context.Context, page pagination.Page) (bool, error) {
		names, err := objects.ExtractNames(page)
		if err != nil {
			return false, err
		}

		for _, name := range names {
			if res := objects.Delete(ctx, cli, containerName, name, nil); res.Err != nil {
				t.Logf("warning: failed to delete object %s: %v", name, res.Err)
			}
		}

		return true, nil
	})

	if err != nil {
		t.Logf("warning: failed to list objects for cleanup: %v", err)
	}
}

func appendSwiftAuthFlags(args []string, opts *swift.Options) []string {
	if opts.Username != "" {
		args = append(args, "--username", opts.Username)
	}

	if opts.Password != "" {
		args = append(args, "--os-password", opts.Password)
	}

	if opts.DomainName != "" {
		args = append(args, "--domain-name", opts.DomainName)
	}

	if opts.DomainID != "" {
		args = append(args, "--domain-id", opts.DomainID)
	}

	if opts.TenantName != "" {
		args = append(args, "--tenant-name", opts.TenantName)
	}

	if opts.TenantID != "" {
		args = append(args, "--tenant-id", opts.TenantID)
	}

	if opts.Region != "" {
		args = append(args, "--region", opts.Region)
	}

	if opts.ApplicationCredentialID != "" {
		args = append(args, "--application-credential-id", opts.ApplicationCredentialID)
	}

	if opts.ApplicationCredentialName != "" {
		args = append(args, "--application-credential-name", opts.ApplicationCredentialName)
	}

	if opts.ApplicationCredentialSecret != "" {
		args = append(args, "--application-credential-secret", opts.ApplicationCredentialSecret)
	}

	if opts.Token != "" {
		args = append(args, "--token", opts.Token)
	}

	return args
}
