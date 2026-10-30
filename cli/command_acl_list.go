package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/acl"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
)

type commandACLList struct {
	jo  jsonOutput
	out textOutput
}

func (c *commandACLList) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("list", "List ACL entries").Alias("ls")

	c.jo.setup(svc, cmd)
	c.out.setup(svc)
	cmd.Action(svc.repositoryReaderAction(c.run))
}

func (c *commandACLList) run(ctx context.Context, rep repo.Repository) error {
	var jl jsonList

	jl.begin(&c.jo)
	defer jl.end()

	entries, err := acl.LoadEntries(ctx, rep, nil)
	if err != nil {
		return errors.Wrap(err, "error loading ACL entries")
	}

	for _, e := range entries {
		if c.jo.jsonOutput {
			jl.emit(aclListItem{e.ManifestID, e})
		} else {
			c.out.printStdout("id:%v user:%v access:%v target:%v\n", e.ManifestID, e.User, e.Access, e.Target)
		}
	}

	return nil
}

type aclListItem struct {
	ID manifest.ID `json:"id"`
	*acl.Entry
}
