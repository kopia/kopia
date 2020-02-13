// +build linux

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

	"github.com/golang/protobuf/proto"
	"github.com/google/fswalker"
	fspb "github.com/google/fswalker/proto/fswalker"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/tests/robustness/checker"
	"github.com/kopia/kopia/tests/tools/fswalker/reporter"
	"github.com/kopia/kopia/tests/tools/fswalker/walker"
)

var _ checker.Comparer = &WalkCompare{}

// WalkCompare is a checker.Comparer that utilizes the fswalker
// libraries to perform the data consistency check.
type WalkCompare struct {
	GlobalFilterMatchers []string
}

// NewWalkCompare instantiates a new WalkCompare and returns its pointer
func NewWalkCompare() *WalkCompare {
	return &WalkCompare{
		GlobalFilterMatchers: []string{
			"ctime:",
			"atime:",
			"mtime:",
		},
	}
}

// Gather meets the checker.Comparer interface. It performs a fswalker Walk
// and returns the resulting Walk as a protobuf Marshaled buffer.
func (chk *WalkCompare) Gather(ctx context.Context, path string) ([]byte, error) {
	walkData, err := walker.WalkPathHash(ctx, path)
	if err != nil {
		return nil, errors.Wrap(err, "walk with hashing error during gather phase")
	}

	err = rerootWalkDataPaths(walkData, path)
	if err != nil {
		return nil, errors.Wrap(err, "reroot walk paths error during gather phase")
	}

	// Store the walk data along with the snapshot ID
	b, err := proto.Marshal(walkData)
	if err != nil {
		return nil, errors.Wrap(err, "walk data proto marshal error")
	}

	return b, nil
}

// Compare meets the checker.Comparer interface. It performs a fswalker Walk
// on the provided file path, unmarshals the comparison data as a fswalker Walk,
// and generates a fswalker report comparing the two Walks. If there are any differences
// an error is returned, and the full report will be written to the provided writer
// as JSON.
func (chk *WalkCompare) Compare(ctx context.Context, path string, data []byte, reportOut io.Writer) error {
	beforeWalk := &fspb.Walk{}

	err := proto.Unmarshal(data, beforeWalk)
	if err != nil {
		return errors.Wrap(err, "walk data unmarshal error")
	}

	afterWalk, err := walker.WalkPathHash(ctx, path)
	if err != nil {
		return errors.Wrap(err, "walk with hashing error during compare phase")
	}

	err = rerootWalkDataPaths(afterWalk, path)
	if err != nil {
		return errors.Wrap(err, "reroot walk paths error during compare phase")
	}

	report, err := reporter.Report(ctx, &fspb.ReportConfig{}, beforeWalk, afterWalk)
	if err != nil {
		return errors.Wrap(err, "report error")
	}

	chk.filterReportDiffs(report)

	err = validateReport(report)
	if err != nil {
		printReportSummary(report, reportOut)

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

	return nil
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
			for _, filterStr := range chk.GlobalFilterMatchers {
				if strings.Contains(diffItem, filterStr) {
					log.Printf("Filtering %s due to filtered prefix %q\n", diffItem, filterStr)
					continue DiffItemLoop
				}
			}

			// Filter the rename of the root directory
			if isRootDirectoryRename(diffItem, mod) {
				log.Println("Filtering", diffItem, "due to root directory rename")
				continue DiffItemLoop
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
	return mod.Before.Info.IsDir && filepath.Clean(mod.Before.Path) == "."
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
	for _, f := range walk.File {
		var err error

		f.Path, err = filepath.Rel(newRoot, f.Path)
		if err != nil {
			return err
		}
	}

	return nil
}
