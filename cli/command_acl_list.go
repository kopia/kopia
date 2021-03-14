package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/acl"
	"github.com/kopia/kopia/repo"
)

var aclListCommand = aclCommands.Command("list", "List ACL entries").Alias("ls")

func runACLList(ctx context.Context, rep repo.Repository) error {
	entries, err := acl.LoadEntries(ctx, rep, nil)
	if err != nil {
		return errors.Wrap(err, "error loading ACL entries")
	}

	for _, e := range entries {
		printStdout("id:%v user:%v access:%v target:%v priority:%v \n", e.ManifestID, e.User, e.Access, e.Target, e.Priority)
	}

	return nil
}

func init() {
	aclListCommand.Action(repositoryReaderAction(runACLList))
}
