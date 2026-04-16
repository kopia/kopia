package cli

import (
	"context"
	"sort"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/notification/notifytemplate"
	"github.com/kopia/kopia/repo"
)

type commandNotificationTemplateList struct {
	out textOutput
	jo  jsonOutput
}

func (c *commandNotificationTemplateList) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("list", "List templates")
	cmd.Action(svc.repositoryReaderAction(c.run))

	c.out.setup(svc)
	c.jo.setup(svc, cmd)
}

func (c *commandNotificationTemplateList) run(ctx context.Context, rep repo.Repository) error {
	infos, err := notifytemplate.ListTemplates(ctx, rep, "")
	if err != nil {
		return errors.Wrap(err, "error listing templates")
	}

	sort.Slice(infos, func(i, j int) bool {
		return infos[i].Name < infos[j].Name
	})

	var jl jsonList

	if c.jo.jsonOutput {
		jl.begin(&c.jo)
		defer jl.end()
	}

	c.out.printStdout("%-30v %-15v %v\n", "NAME", "TYPE", "MODIFIED")

	for _, i := range infos {
		if c.jo.jsonOutput {
			jl.emit(i)
			continue
		}

		var typeString, lastModString string

		if i.LastModified == nil {
			typeString = "<built-in>"
			lastModString = ""
		} else {
			typeString = "<customized>"
			lastModString = formatTimestamp(*i.LastModified)
		}

		c.out.printStdout("%-30v %-15v %v\n", i.Name, typeString, lastModString)
	}

	return nil
}
