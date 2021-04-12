// +build !windows

package restore

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/kopia/kopia/fs/localfs"
)

func TestSafeRemoveAll(t *testing.T) {
	tdir := t.TempDir()
	suffleng := len(localfs.ShallowEntrySuffix)

	for fnl := MaxFilenameLength - suffleng - 1; fnl < MaxFilenameLength+2; fnl++ {
		buffy := make([]byte, 0, fnl)
		for i := 0; i < fnl; i++ {
			buffy = append(buffy, 'x')
		}

		filename := string(buffy)
		path := filepath.Join(tdir, filename)
		filenameext := filename + localfs.ShallowEntrySuffix
		pathext := filepath.Join(tdir, filenameext)

		t.Logf("x*%d + %d", fnl, suffleng)

		// Some of these will fail because their names will be too long. This is
		// not a fatal error.
		err := os.WriteFile(pathext, buffy, 0600)
		canmakefile := err == nil

		if !canmakefile {
			t.Logf("can't make x*%d + %d: %v", fnl, suffleng, err)
		}

		// SafeRemoveAll should succeed regardless of whether or not the file is
		// too long.
		if err := SafeRemoveAll(path); err != nil {
			t.Fatalf("safe remove of %q should  have succeeded: %v", path, err)
		}

		// If we actually made a file, it should now be gone.
		if canmakefile {
			if _, err := os.Stat(pathext); err == nil {
				t.Fatalf("x*%d + %d should have been removed", fnl, suffleng)
			}
		}
	}
}
