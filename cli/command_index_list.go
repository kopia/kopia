package cli

import (
	"context"
	"sort"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
)

type commandIndexList struct {
	blockIndexListSummary           bool
	blockIndexListIncludeSuperseded bool
	blockIndexListSort              string

	jo  jsonOutput
	out textOutput
}

func (c *commandIndexList) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("list", "List content indexes").Alias("ls")
	cmd.Flag("summary", "Display index blob summary").BoolVar(&c.blockIndexListSummary)
	cmd.Flag("superseded", "Include inactive index files superseded by compaction").BoolVar(&c.blockIndexListIncludeSuperseded)
	cmd.Flag("sort", "Index blob sort order").Default("time").EnumVar(&c.blockIndexListSort, "time", "size", "name")
	c.jo.setup(svc, cmd)
	c.out.setup(svc)
	cmd.Action(svc.directRepositoryReadAction(c.run))
}

func (c *commandIndexList) run(ctx context.Context, rep repo.DirectRepository) error {
	var jl jsonList

	jl.begin(&c.jo)
	defer jl.end()

	blks, err := rep.IndexBlobs(ctx, c.blockIndexListIncludeSuperseded)
	if err != nil {
		return errors.Wrap(err, "error listing index blobs")
	}

	switch c.blockIndexListSort {
	case "time":
		sort.Slice(blks, func(i, j int) bool {
			return blks[i].Timestamp.Before(blks[j].Timestamp)
		})
	case "size":
		sort.Slice(blks, func(i, j int) bool {
			return blks[i].Length < blks[j].Length
		})
	case "name":
		sort.Slice(blks, func(i, j int) bool {
			return blks[i].BlobID < blks[j].BlobID
		})
	}

	for _, b := range blks {
		if c.jo.jsonOutput {
			jl.emit(b)
		} else {
			c.out.printStdout("%-60v %10v %v %v\n", b.BlobID, b.Length, formatTimestampPrecise(b.Timestamp), b.Superseded)
		}
	}

	if c.blockIndexListSummary && !c.jo.jsonOutput {
		c.out.printStdout("total %v indexes\n", len(blks))
	}

	return nil
}
