package cli

import (
	"context"

	"github.com/kopia/kopia/repo"
)

type fileHistogram struct {
	totalFiles       uint
	size0Byte        uint
	size0bTo100Kb    uint
	size100KbTo100Mb uint
	size100MbTo1Gb   uint
	sizeOver1Gb      uint
}

type dirHistogram struct {
	totalDirs             uint
	numEntries0           uint
	numEntries0to100      uint
	numEntries100to1000   uint
	numEntries1000to10000 uint
	numEntries10000to1mil uint
	numEntriesOver1mil    uint
}

type sourceHistogram struct {
	totalSize uint64
	files     fileHistogram
	dirs      dirHistogram
}

type commandDiscover struct {
	directoryPath string

	out textOutput
}

func (c *commandDiscover) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("discovery", " This tool introduces an all-inclusive scanning solution, efficiently scrutinizing directories and delivering comprehensive reports on file system sizes and file counts within the backup’s sources.").Alias("directory")
	cmd.Arg("path", "directory path").Required().StringVar(&c.directoryPath)
	cmd.Action(svc.repositoryReaderAction(c.run))

	c.out.setup(svc)
}

func (c *commandDiscover) run(ctx context.Context, rep repo.Repository) error {
	// todo:
	return nil
}
