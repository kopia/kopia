package tempfile_test

import (
	"testing"

	"github.com/kopia/kopia/internal/tempfile"
)

func TestTempFile(t *testing.T) {
	cases := []struct {
		name string
		dir  string
	}{
		{
			name: "empty dir name",
			dir:  "",
		},
		{
			name: "non-empty dir name",
			dir:  t.TempDir(),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tempfile.VerifyTempfile(t, tc.dir, tempfile.Create)
		})
	}
}
