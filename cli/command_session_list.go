package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
)

var sessionListCommand = sessionCommands.Command("list", "List sessions").Alias("ls")

func runSessionList(ctx context.Context, rep *repo.DirectRepository) error {
	sessions, err := rep.ListActiveSessions(ctx)
	if err != nil {
		return errors.Wrap(err, "error listing sessions")
	}

	for _, s := range sessions {
		printStdout("%v %v@%v %v %v\n", s.ID, s.User, s.Host, formatTimestamp(s.StartTime), formatTimestamp(s.CheckpointTime))
	}

	return nil
}

func init() {
	sessionListCommand.Action(directRepositoryAction(runSessionList))
}
