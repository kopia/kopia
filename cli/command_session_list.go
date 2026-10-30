package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
)

type commandSessionList struct {
	out textOutput
}

func (c *commandSessionList) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("list", "List sessions").Alias("ls")
	cmd.Action(svc.directRepositoryReadAction(c.run))
	c.out.setup(svc)
}

func (c *commandSessionList) run(ctx context.Context, rep repo.DirectRepository) error {
	sessions, err := rep.ContentReader().ListActiveSessions(ctx)
	if err != nil {
		return errors.Wrap(err, "error listing sessions")
	}

	for _, s := range sessions {
		c.out.printStdout("%v %v@%v %v %v\n", s.ID, s.User, s.Host, formatTimestamp(s.StartTime), formatTimestamp(s.CheckpointTime))
	}

	return nil
}
