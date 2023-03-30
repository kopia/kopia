package cli

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testutil"
)

var (
	fakeCertContent         = []byte("fake certificate content")
	fakeCertContentAsBase64 = base64.StdEncoding.EncodeToString(fakeCertContent)
)

func TestLoadPEMBase64(t *testing.T) {
	var s3flags storageS3Flags

	s3flags = storageS3Flags{rootCaPemBase64: ""}
	require.NoError(t, s3flags.preActionLoadPEMBase64(nil))

	s3flags = storageS3Flags{rootCaPemBase64: "AA=="}
	require.NoError(t, s3flags.preActionLoadPEMBase64(nil))

	s3flags = storageS3Flags{rootCaPemBase64: fakeCertContentAsBase64}
	require.NoError(t, s3flags.preActionLoadPEMBase64(nil))
	require.Equal(t, fakeCertContent, s3flags.s3options.RootCA, "content of RootCA should be %v", fakeCertContent)

	s3flags = storageS3Flags{rootCaPemBase64: "!"}
	err := s3flags.preActionLoadPEMBase64(nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "illegal base64 data")
}

func TestLoadPEMPath(t *testing.T) {
	var s3flags storageS3Flags

	tempdir := testutil.TempDirectory(t)
	certpath := filepath.Join(tempdir, "certificate-filename")

	require.NoError(t, os.WriteFile(certpath, fakeCertContent, 0o644))

	// Test regular file
	s3flags = storageS3Flags{rootCaPemPath: certpath}
	require.NoError(t, s3flags.preActionLoadPEMPath(nil))
	require.Equal(t, fakeCertContent, s3flags.s3options.RootCA, "content of RootCA should be %v", fakeCertContent)

	// Test inexistent file
	s3flags = storageS3Flags{rootCaPemPath: "/does-not-exists"}
	err := s3flags.preActionLoadPEMPath(nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no such file or directory")
}

func TestLoadPEMBoth(t *testing.T) {
	s3flags := storageS3Flags{rootCaPemBase64: "AA==", rootCaPemPath: "/tmp/blah"}
	require.NoError(t, s3flags.preActionLoadPEMBase64(nil))
	err := s3flags.preActionLoadPEMPath(nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "mutually exclusive")
}
