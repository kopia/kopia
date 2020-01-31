package reporter

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/golang/protobuf/ptypes/timestamp"
	fspb "github.com/google/fswalker/proto/fswalker"

	"github.com/kopia/kopia/tests/testenv"
)

func TestReporterWithFiles(t *testing.T) {
	ctx := context.TODO()

	config := &fspb.ReportConfig{
		Version:    1,
		ExcludePfx: nil,
	}

	fileList := []*fspb.File{
		{
			Version: 0,
			Path:    filepath.Join("some", "path"),
			Info: &fspb.FileInfo{
				Name: "this_is_a.file",
				Size: 11235,
				Mode: 0700,
				Modified: &timestamp.Timestamp{
					Seconds: 12,
					Nanos:   0,
				},
				IsDir: false,
			},
			Stat: &fspb.FileStat{
				Dev:     0,
				Inode:   0,
				Nlink:   0,
				Mode:    0,
				Uid:     0,
				Gid:     0,
				Rdev:    0,
				Size:    0,
				Blksize: 0,
				Blocks:  0,
				Atime: &timestamp.Timestamp{
					Seconds: 0,
					Nanos:   0,
				},
				Mtime: &timestamp.Timestamp{
					Seconds: 0,
					Nanos:   0,
				},
				Ctime: &timestamp.Timestamp{
					Seconds: 0,
					Nanos:   0,
				},
			},
			Fingerprint: nil,
		},
	}

	beforeWalk := &fspb.Walk{
		Id:      "first-walk-ID",
		Version: 1,
		Policy: &fspb.Policy{
			Version:              0,
			Include:              nil,
			ExcludePfx:           nil,
			HashPfx:              nil,
			MaxHashFileSize:      0,
			WalkCrossDevice:      false,
			IgnoreIrregularFiles: false,
			MaxDirectoryDepth:    0,
		},
		File:         fileList,
		Notification: nil,
		Hostname:     "a-hostname",
		StartWalk: &timestamp.Timestamp{
			Seconds: 0,
			Nanos:   0,
		},
		StopWalk: &timestamp.Timestamp{
			Seconds: 0,
			Nanos:   0,
		},
	}

	afterWalk := &fspb.Walk{
		Id:      "second-walk-ID",
		Version: 1,
		Policy: &fspb.Policy{
			Version:              0,
			Include:              nil,
			ExcludePfx:           nil,
			HashPfx:              nil,
			MaxHashFileSize:      0,
			WalkCrossDevice:      false,
			IgnoreIrregularFiles: false,
			MaxDirectoryDepth:    0,
		},
		File:         fileList,
		Notification: nil,
		Hostname:     "a-hostname",
		StartWalk: &timestamp.Timestamp{
			Seconds: 100,
			Nanos:   0,
		},
		StopWalk: &timestamp.Timestamp{
			Seconds: 101,
			Nanos:   0,
		},
	}

	report, err := Report(ctx, config, beforeWalk, afterWalk)
	testenv.AssertNoError(t, err)

	if got, want := len(report.Deleted), 0; got != want {
		t.Errorf("Expected %d deleted files, but got %d", want, got)
	}

	if got, want := len(report.Added), 0; got != want {
		t.Errorf("Expected %d added files, but got %d", want, got)
	}

	if got, want := len(report.Modified), 0; got != want {
		t.Errorf("Expected %d modified files, but got %d", want, got)
	}

	if got, want := len(report.Errors), 0; got != want {
		t.Errorf("Expected %d modified files, but got %d", want, got)
	}
}
