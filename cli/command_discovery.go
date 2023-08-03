package cli

import (
	"context"

	"github.com/kopia/kopia/repo"
)

type commandDiscovery struct {
	directoryPath string

	out textOutput
}

func (c *commandDiscovery) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("discovery", " This tool introduces an all-inclusive scanning solution, efficiently scrutinizing directories and delivering comprehensive reports on file system sizes and file counts within the backup’s sources.").Alias("directory")
	cmd.Arg("path", "directory path").Required().StringVar(&c.directoryPath)
	cmd.Action(svc.repositoryReaderAction(c.run))

	c.out.setup(svc)
}

func (c *commandDiscovery) run(ctx context.Context, rep repo.Repository) error {
	// todo:
	return nil
}
