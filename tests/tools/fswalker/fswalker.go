//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

// Package fswalker provides the checker.Comparer interface using FSWalker
// walker and reporter.
package fswalker

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"path/filepath"
	"strings"

	//nolint:staticcheck

	"github.com/google/fswalker"
	fspb "github.com/google/fswalker/proto/fswalker"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"

	"github.com/kopia/kopia/tests/tools/fswalker/reporter"
	"github.com/kopia/kopia/tests/tools/fswalker/walker"
)

// WalkCompare is a checker.Comparer that utilizes the fswalker
// libraries to perform the data consistency check.
type WalkCompare struct {
	GlobalFilterFuncs []func(string, fswalker.ActionData) bool
}

// NewWalkCompare instantiates a new WalkCompare and returns its pointer.
func NewWalkCompare() *WalkCompare {
	return &WalkCompare{
		GlobalFilterFuncs: []func(string, fswalker.ActionData) bool{
			filterFileTimeDiffs,
			isRootDirectoryRename,
			filterDirSizeCheck,
			ignoreGIDIfZero,
			ignoreUIDIfZero,
		},
	}
}

// Gather meets the checker.Comparer interface. It performs a fswalker Walk
// and returns the resulting Walk as a protobuf Marshaled buffer.
func (chk *WalkCompare) Gather(ctx context.Context, path string, opts map[string]string) ([]byte, error) {
	walkData, err := performWalk(ctx, path)
	if err != nil {
		return nil, errors.Wrap(err, "walk with hashing error during gather phase")
	}

	// Store the walk data along with the snapshot ID
	b, err := proto.Marshal(walkData)

	return b, errors.Wrap(err, "walk data proto marshal error")
}

func performWalk(ctx context.Context, path string) (*fspb.Walk, error) {
	walkData, err := walker.WalkPathHash(ctx, path)
	if err != nil {
		return nil, errors.Wrap(err, "walk with hashing error during gather phase")
	}

	err = processWalk(walkData, path)

	return walkData, errors.Wrap(err, "error during gather phase")
}

func processWalk(walk *fspb.Walk, path string) error {
	// Hostname is cleared to allow reporting on walks done across
	// different hosts.
	clearHostname(walk)

	// Walk paths are rerooted to be relative to the root directory
	// so child entries of /source/rootDir/... won't be different than
	// child entries of /target/rootDir/...
	err := rerootWalkDataPaths(walk, path)

	return errors.Wrap(err, "reroot walk paths error")
}

func clearHostname(walk *fspb.Walk) {
	walk.Hostname = ""
}

// Compare meets the checker.Comparer interface. It performs a fswalker Walk
// on the provided file path, unmarshals the comparison data as a fswalker Walk,
// and generates a fswalker report comparing the two Walks. If there are any differences
// an error is returned, and the full report will be written to the provided writer
// as JSON.
func (chk *WalkCompare) Compare(ctx context.Context, path string, data []byte, reportOut io.Writer, opts map[string]string) error {
	beforeWalk := &fspb.Walk{}

	err := proto.Unmarshal(data, beforeWalk)
	if err != nil {
		return errors.Wrap(err, "walk data unmarshal error")
	}

	clearHostname(beforeWalk)

	afterWalk, err := performWalk(ctx, path)
	if err != nil {
		return errors.Wrap(err, "walk with hashing error during compare phase")
	}

	report, err := reporter.Report(ctx, &fspb.ReportConfig{}, beforeWalk, afterWalk)
	if err != nil {
		return errors.Wrap(err, "report error")
	}

	chk.filterReportDiffs(report)

	err = validateReport(report)
	if err != nil && reportOut != nil {
		printReportSummary(report, reportOut)

		// Don't spew these fields. They can be enormous and
		// are probably redundant
		report.WalkBefore = nil
		report.WalkAfter = nil

		b, marshalErr := json.MarshalIndent(report, "", "   ")
		if marshalErr != nil {
			_, reportErr := reportOut.Write([]byte(marshalErr.Error()))
			if reportErr != nil {
				return errors.Wrapf(reportErr, "error while writing out marshal error (%v)", marshalErr.Error())
			}

			return errors.Wrap(marshalErr, "error JSON marshaling report")
		}

		if _, wrErr := reportOut.Write(b); wrErr != nil {
			return errors.Wrap(err, "error writing report to output")
		}

		return errors.Wrap(err, "validation error")
	}

	return err
}

func printReportSummary(report *fswalker.Report, reportOut io.Writer) {
	rptr := &fswalker.Reporter{}
	rptr.PrintDiffSummary(reportOut, report)
	rptr.PrintReportSummary(reportOut, report)
	rptr.PrintRuleSummary(reportOut, report)
}

func (chk *WalkCompare) filterReportDiffs(report *fswalker.Report) {
	var newModList []fswalker.ActionData

	for _, mod := range report.Modified {
		var newDiffItemList []string

		diffItems := strings.Split(mod.Diff, "\n")

	DiffItemLoop:
		for _, diffItem := range diffItems {
			for _, filterFunc := range chk.GlobalFilterFuncs {
				if filterFunc(diffItem, mod) {
					continue DiffItemLoop
				}
			}

			newDiffItemList = append(newDiffItemList, diffItem)
		}

		if len(newDiffItemList) > 0 {
			log.Println("Not Filtering", newDiffItemList)
			mod.Diff = strings.Join(newDiffItemList, "\n")
			newModList = append(newModList, mod)
		}
	}

	report.Modified = newModList
}

func isRootDirectoryRename(diffItem string, mod fswalker.ActionData) bool {
	if !strings.HasPrefix(diffItem, "name: ") {
		return false
	}

	// The mod.Before.Path may be given from fswalker Report as "./", so
	// clean it before compare
	return mod.Before.GetInfo().GetIsDir() && filepath.Clean(mod.Before.GetPath()) == "."
}

// Directory size changes with underlying file system setups. Ignote the dir size during data consistency check to make it robust.
// Remove this filter from GlobalFilterFuncs to detect the size difference in a directory.
func filterDirSizeCheck(str string, mod fswalker.ActionData) bool {
	return mod.Before.GetInfo().GetIsDir() && strings.Contains(str, "size: ")
}

func filterFileTimeDiffs(str string, mod fswalker.ActionData) bool {
	return strings.Contains(str, "ctime:") || strings.Contains(str, "atime:") || strings.Contains(str, "mtime:")
}

func ignoreGIDIfZero(str string, mod fswalker.ActionData) bool {
	if !strings.Contains(str, "gid:") {
		return false
	}

	beforeGID := mod.Before.GetStat().GetGid()
	afterGID := mod.After.GetStat().GetGid()

	return beforeGID != afterGID && beforeGID == 0
}

func ignoreUIDIfZero(str string, mod fswalker.ActionData) bool {
	if !strings.Contains(str, "uid:") {
		return false
	}

	beforeUID := mod.Before.GetStat().GetUid()
	afterUID := mod.After.GetStat().GetUid()

	return beforeUID != afterUID && beforeUID == 0
}

func validateReport(report *fswalker.Report) error {
	if len(report.Modified) > 0 {
		return errors.New("files were modified")
	}

	if len(report.Added) > 0 {
		return errors.New("files were added")
	}

	if len(report.Deleted) > 0 {
		return errors.New("files were deleted")
	}

	if len(report.Errors) > 0 {
		return errors.New("errors were thrown in the walk")
	}

	return nil
}

func rerootWalkDataPaths(walk *fspb.Walk, newRoot string) error {
	for _, f := range walk.GetFile() {
		var err error

		f.Path, err = filepath.Rel(newRoot, f.GetPath())
		if err != nil {
			return err
		}
	}

	return nil
}
