package cli

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testutil"
)

func TestR2LoadPEMBase64(t *testing.T) {
	var r2flags storageR2Flags

	r2flags = storageR2Flags{rootCaPemBase64: ""}
	require.NoError(t, r2flags.preActionLoadPEMBase64(nil))

	r2flags = storageR2Flags{rootCaPemBase64: "AA=="}
	require.NoError(t, r2flags.preActionLoadPEMBase64(nil))

	r2flags = storageR2Flags{rootCaPemBase64: fakeCertContentAsBase64}
	require.NoError(t, r2flags.preActionLoadPEMBase64(nil))
	require.Equal(t, fakeCertContent, r2flags.r2options.RootCA, "content of RootCA should be %v", fakeCertContent)

	r2flags = storageR2Flags{rootCaPemBase64: "!"}
	err := r2flags.preActionLoadPEMBase64(nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "illegal base64 data")
}

func TestR2LoadPEMPath(t *testing.T) {
	var r2flags storageR2Flags

	tempdir := testutil.TempDirectory(t)
	certpath := filepath.Join(tempdir, "certificate-filename")

	require.NoError(t, os.WriteFile(certpath, fakeCertContent, 0o644))

	r2flags = storageR2Flags{rootCaPemPath: certpath}
	require.NoError(t, r2flags.preActionLoadPEMPath(nil))
	require.Equal(t, fakeCertContent, r2flags.r2options.RootCA, "content of RootCA should be %v", fakeCertContent)

	r2flags = storageR2Flags{rootCaPemPath: "/does-not-exists"}
	err := r2flags.preActionLoadPEMPath(nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "error opening root-ca-pem-path")
}

func TestR2LoadPEMBoth(t *testing.T) {
	r2flags := storageR2Flags{rootCaPemBase64: "AA==", rootCaPemPath: "/tmp/blah"}
	require.NoError(t, r2flags.preActionLoadPEMBase64(nil))
	err := r2flags.preActionLoadPEMPath(nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "mutually exclusive")
}
