package cli

import (
	"bytes"
	"encoding/base64"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/alecthomas/kingpin/v2"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testutil"
)

var (
	fakeSwiftCertContent         = []byte("fake swift certificate content")
	fakeSwiftCertContentAsBase64 = base64.StdEncoding.EncodeToString(fakeSwiftCertContent)
)

type fakeSwiftStorageProviderServices struct{}

func (fakeSwiftStorageProviderServices) EnvName(s string) string {
	return "KOPIA_TEST_" + s
}

func (fakeSwiftStorageProviderServices) setPasswordFromToken(string) {}

func (fakeSwiftStorageProviderServices) storageProviders() []StorageProvider {
	return nil
}

func (fakeSwiftStorageProviderServices) stdin() io.Reader {
	return bytes.NewReader(nil)
}

func TestSwiftProviderRegistered(t *testing.T) {
	names := make([]string, 0, len(getRegisteredStorageProviders()))
	for _, p := range getRegisteredStorageProviders() {
		names = append(names, p.Name)
	}

	require.True(t, slices.Contains(names, "swift"), "swift storage provider is not registered")
}

func TestSwiftLoadPEMBase64(t *testing.T) {
	var swiftFlags storageSwiftFlags

	swiftFlags = storageSwiftFlags{rootCaPemBase64: ""}
	require.NoError(t, swiftFlags.preActionLoadPEMBase64(nil))

	swiftFlags = storageSwiftFlags{rootCaPemBase64: "AA=="}
	require.NoError(t, swiftFlags.preActionLoadPEMBase64(nil))

	swiftFlags = storageSwiftFlags{rootCaPemBase64: fakeSwiftCertContentAsBase64}
	require.NoError(t, swiftFlags.preActionLoadPEMBase64(nil))
	require.Equal(t, fakeSwiftCertContent, swiftFlags.options.RootCA)

	swiftFlags = storageSwiftFlags{rootCaPemBase64: "!"}
	err := swiftFlags.preActionLoadPEMBase64(nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "illegal base64 data")
}

func TestSwiftLoadPEMPath(t *testing.T) {
	var swiftFlags storageSwiftFlags

	tempdir := testutil.TempDirectory(t)
	certpath := filepath.Join(tempdir, "certificate-filename")

	require.NoError(t, os.WriteFile(certpath, fakeSwiftCertContent, 0o644))

	swiftFlags = storageSwiftFlags{rootCaPemPath: certpath}
	require.NoError(t, swiftFlags.preActionLoadPEMPath(nil))
	require.Equal(t, fakeSwiftCertContent, swiftFlags.options.RootCA)

	swiftFlags = storageSwiftFlags{rootCaPemPath: "/does-not-exist"}
	err := swiftFlags.preActionLoadPEMPath(nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "error opening root-ca-pem-path")
}

func TestSwiftLoadPEMBoth(t *testing.T) {
	swiftFlags := storageSwiftFlags{rootCaPemBase64: "AA==", rootCaPemPath: "/tmp/blah"}
	require.NoError(t, swiftFlags.preActionLoadPEMBase64(nil))
	err := swiftFlags.preActionLoadPEMPath(nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "mutually exclusive")
}

func TestSwiftFlagAndEnvWiring(t *testing.T) {
	t.Setenv("KOPIA_TEST_OS_AUTH_URL", "https://keystone.example/v3")
	t.Setenv("KOPIA_TEST_OS_USERNAME", "env-user")
	t.Setenv("KOPIA_TEST_OS_PASSWORD", "env-password")
	t.Setenv("KOPIA_TEST_OS_USER_DOMAIN_NAME", "Default")
	t.Setenv("KOPIA_TEST_OS_PROJECT_NAME", "project")
	t.Setenv("KOPIA_TEST_OS_REGION_NAME", "RegionOne")
	t.Setenv("KOPIA_TEST_OS_INTERFACE", "internal")
	t.Setenv("KOPIA_TEST_OS_APPLICATION_CREDENTIAL_SECRET", "env-app-secret")

	var swiftFlags storageSwiftFlags

	app := kingpin.New("test", "test")
	cmd := app.Command("swift", "swift")
	swiftFlags.Setup(fakeSwiftStorageProviderServices{}, cmd)

	_, err := app.Parse([]string{
		"swift",
		"--container", "repo-container",
		"--prefix", "repo-prefix/",
		"--read-only",
		"--disable-tls-verification",
		"--application-credential-id", "app-id",
	})
	require.NoError(t, err)

	require.Equal(t, "repo-container", swiftFlags.options.ContainerName)
	require.Equal(t, "repo-prefix/", swiftFlags.options.Prefix)
	require.Equal(t, "https://keystone.example/v3", swiftFlags.options.AuthURL)
	require.Equal(t, "env-user", swiftFlags.options.Username)
	require.Equal(t, "env-password", swiftFlags.options.Password)
	require.Equal(t, "Default", swiftFlags.options.DomainName)
	require.Equal(t, "project", swiftFlags.options.TenantName)
	require.Equal(t, "RegionOne", swiftFlags.options.Region)
	require.Equal(t, "internal", swiftFlags.options.Availability)
	require.Equal(t, "app-id", swiftFlags.options.ApplicationCredentialID)
	require.Equal(t, "env-app-secret", swiftFlags.options.ApplicationCredentialSecret)
	require.True(t, swiftFlags.options.ReadOnly)
	require.True(t, swiftFlags.options.DoNotVerifyTLS)
}
