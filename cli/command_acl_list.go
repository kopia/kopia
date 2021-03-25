package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/acl"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
)

var aclListCommand = aclCommands.Command("list", "List ACL entries").Alias("ls")

func runACLList(ctx context.Context, rep repo.Repository) error {
	var jl jsonList

	jl.begin()
	defer jl.end()

	entries, err := acl.LoadEntries(ctx, rep, nil)
	if err != nil {
		return errors.Wrap(err, "error loading ACL entries")
	}

	for _, e := range entries {
		if jsonOutput {
			jl.emit(aclListItem{e.ManifestID, e})
		} else {
			printStdout("id:%v user:%v access:%v target:%v\n", e.ManifestID, e.User, e.Access, e.Target)
		}
	}

	return nil
}

type aclListItem struct {
	ID manifest.ID `json:"id"`
	*acl.Entry
}

func init() {
	registerJSONOutputFlags(aclListCommand)
	aclListCommand.Action(repositoryReaderAction(runACLList))
}
