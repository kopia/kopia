package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/user"
	"github.com/kopia/kopia/repo"
)

type commandServerUserList struct {
	jo  jsonOutput
	out textOutput
}

func (c *commandServerUserList) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("list", "List users").Alias("ls")
	c.jo.setup(svc, cmd)
	c.out.setup(svc)
	cmd.Action(svc.repositoryReaderAction(c.runUserList))
}

func (c *commandServerUserList) runUserList(ctx context.Context, rep repo.Repository) error {
	var jl jsonList

	jl.begin(&c.jo)
	defer jl.end()

	profiles, err := user.ListUserProfiles(ctx, rep)
	if err != nil {
		return errors.Wrap(err, "error listing user profiles")
	}

	for _, p := range profiles {
		if c.jo.jsonOutput {
			jl.emit(p)
		} else {
			c.out.printStdout("%v\n", p.Username)
		}
	}

	return nil
}
