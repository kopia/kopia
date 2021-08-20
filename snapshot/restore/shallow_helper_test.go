package restore

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kopia/kopia/fs/localfs"
)

func TestSafeRemoveAll(t *testing.T) {
	tdir := t.TempDir()
	suffleng := len(localfs.ShallowEntrySuffix)

	for fnl := MaxFilenameLength - suffleng*2 - 1; fnl < MaxFilenameLength+2; fnl++ {
		filename := strings.Repeat("d", fnl)

		path := filepath.Join(tdir, filename)
		filenameext := filename + localfs.ShallowEntrySuffix
		pathext := filepath.Join(tdir, filenameext)

		t.Logf("x*%d + %d", fnl, suffleng)

		// Some of these will fail because their names will be too long. This is
		// not a fatal error.
		err := os.WriteFile(pathext, []byte(filename), 0o600)
		canmakefile := err == nil

		if !canmakefile {
			t.Logf("can't make x*%d + %d: %v", fnl, suffleng, err)
		}

		// SafeRemoveAll should succeed regardless of whether or not the file is
		// too long.
		if err := SafeRemoveAll(path); err != nil {
			t.Errorf("safe remove x*%d + %d (%q) should  have succeeded: %v", fnl, suffleng, path, err)
		}

		// If we actually made a file, it should now be gone.
		if canmakefile {
			if _, err := os.Stat(pathext); err == nil {
				t.Fatalf("x*%d + %d should have been removed", fnl, suffleng)
			}
		}
	}
}
