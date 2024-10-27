package cli

import (
	"context"

	"github.com/alecthomas/kingpin/v2"

	"github.com/kopia/kopia/notification/notifytemplate"
	"github.com/kopia/kopia/repo"
)

type commandNotificationTemplate struct {
	list   commandNotificationTemplateList
	show   commandNotificationTemplateShow
	set    commandNotificationTemplateSet
	remove commandNotificationTemplateRemove
}

type notificationTemplateNameArg struct {
	templateName string
}

func (c *notificationTemplateNameArg) setup(svc appServices, cmd *kingpin.CmdClause) {
	cmd.Arg("template", "Template name").Required().HintAction(svc.repositoryHintAction(c.listNotificationTemplates)).StringVar(&c.templateName)
}

func (c *notificationTemplateNameArg) listNotificationTemplates(ctx context.Context, rep repo.Repository) []string {
	infos, _ := notifytemplate.ListTemplates(ctx, rep, c.templateName)

	var hints []string

	for _, ti := range infos {
		hints = append(hints, ti.Name)
	}

	return hints
}

func (c *commandNotificationTemplate) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("template", "Manage templates")
	c.list.setup(svc, cmd)
	c.set.setup(svc, cmd)
	c.show.setup(svc, cmd)
	c.remove.setup(svc, cmd)
}
