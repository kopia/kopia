package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
)

type commandLogsList struct {
	out textOutput

	crit logSelectionCriteria
}

func (c *commandLogsList) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("list", "List logs.").Alias("ls")

	cmd.Action(svc.directRepositoryReadAction(c.run))

	c.out.setup(svc)
	c.crit.setup(cmd)
}

func (c *commandLogsList) run(ctx context.Context, rep repo.DirectRepository) error {
	allSessions0, err := getLogSessions(ctx, rep.BlobReader())
	if err != nil {
		return errors.Wrap(err, "error getting log sessions")
	}

	allSessions := c.crit.filterLogSessions(allSessions0)

	if len(allSessions) < len(allSessions0) {
		defer log(ctx).Infof("NOTE: Listed %v/%v log sessions, pass --all to show all.", len(allSessions), len(allSessions0))
	}

	// output sessions
	for _, s := range allSessions {
		c.out.printStdout(
			"%v %v %v %v %v\n", s.id,
			formatTimestamp(s.startTime),
			s.endTime.Sub(s.startTime),
			units.BytesStringBaseEnv(s.totalSize),
			len(s.segments),
		)
	}

	return nil
}
