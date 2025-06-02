package tempfile_test

import (
	"testing"

	"github.com/kopia/kopia/internal/tempfile"
)

func TestTempFile(t *testing.T) {
	tempfile.VerifyTempfile(t, tempfile.CreateAutoDelete)
}
