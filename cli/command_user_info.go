package cli

import (
	"context"
	"encoding/json"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/user"
	"github.com/kopia/kopia/repo"
)

type commandServerUserInfo struct {
	name string
}

func (c *commandServerUserInfo) setup(app appServices, parent commandParent) {
	cmd := parent.Command("info", "Info about particular user")
	cmd.Arg("username", "The username to look up.").Required().StringVar(&c.name)
	cmd.Action(app.repositoryReaderAction(c.run))
}

func (c *commandServerUserInfo) run(ctx context.Context, rep repo.Repository) error {
	up, err := user.GetUserProfile(ctx, rep, c.name)
	if err != nil {
		return errors.Wrap(err, "error getting user profile")
	}

	j, err := json.MarshalIndent(up, "", "  ")
	if err != nil {
		return errors.Wrap(err, "error marshaling JSON")
	}

	printStdout("%s", j)

	return nil
}
