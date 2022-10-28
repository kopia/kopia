//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

// Package reporter wraps calls to the fswalker Reporter
package reporter

import (
	"context"
	"os"

	"github.com/google/fswalker"
	fspb "github.com/google/fswalker/proto/fswalker"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/tests/tools/fswalker/protofile"
)

// Report performs a report governed by the contents of the provided
// ReportConfig of the comparison of the two Walks provided.
func Report(ctx context.Context, config *fspb.ReportConfig, beforeWalk, afterWalk *fspb.Walk) (*fswalker.Report, error) {
	tmpCfgFile, err := writeTempConfigFile(config)
	if err != nil {
		return nil, err
	}

	defer os.RemoveAll(tmpCfgFile) //nolint:errcheck

	verbose := false

	reporter, err := fswalker.ReporterFromConfigFile(ctx, tmpCfgFile, verbose)
	if err != nil {
		return nil, err
	}

	return reporter.Compare(beforeWalk, afterWalk)
}

// ReportFiles performs a report governed by the contents of the provided
// ReportConfig of the two Walks at the provided file paths.
func ReportFiles(ctx context.Context, config *fspb.ReportConfig, beforeFile, afterFile string) (*fswalker.Report, error) {
	tmpCfgFile, err := writeTempConfigFile(config)
	if err != nil {
		return nil, err
	}

	defer os.RemoveAll(tmpCfgFile) //nolint:errcheck

	verbose := false

	reporter, err := fswalker.ReporterFromConfigFile(ctx, tmpCfgFile, verbose)
	if err != nil {
		return nil, err
	}

	var before, after *fswalker.WalkFile

	after, err = reporter.ReadWalk(ctx, afterFile)
	if err != nil {
		return nil, errors.Errorf("file cannot be read: %s", afterFile)
	}

	if beforeFile != "" {
		before, err = reporter.ReadWalk(ctx, beforeFile)
		if err != nil {
			return nil, errors.Errorf("file cannot be read: %s", beforeFile)
		}
	}

	return reporter.Compare(before.Walk, after.Walk)
}

func writeTempConfigFile(config *fspb.ReportConfig) (string, error) {
	f, err := os.CreateTemp("", "fswalker-report-config-")
	if err != nil {
		return "", err
	}

	f.Close() //nolint:errcheck

	configFileName := f.Name()
	err = protofile.WriteTextProto(configFileName, config)

	return configFileName, err
}
