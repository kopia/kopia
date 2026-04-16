package cli

import (
	"context"
	"io"
	"os"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/editor"
	"github.com/kopia/kopia/notification/notifytemplate"
	"github.com/kopia/kopia/repo"
)

type commandNotificationTemplateSet struct {
	notificationTemplateNameArg
	fromStdin    bool
	fromFileName string
	editor       bool

	out textOutput
	svc appServices
}

func (c *commandNotificationTemplateSet) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("set", "Set the notification template")

	c.notificationTemplateNameArg.setup(svc, cmd)

	cmd.Flag("from-stdin", "Read new template from stdin").BoolVar(&c.fromStdin)
	cmd.Flag("from-file", "Read new template from file").ExistingFileVar(&c.fromFileName)
	cmd.Flag("editor", "Edit template using default editor").BoolVar(&c.editor)
	cmd.Action(svc.repositoryWriterAction(c.run))

	c.svc = svc
	c.out.setup(svc)
}

func (c *commandNotificationTemplateSet) run(ctx context.Context, rep repo.RepositoryWriter) error {
	var (
		data []byte
		err  error
	)

	switch {
	case c.fromStdin:
		data, err = io.ReadAll(c.svc.stdin())
	case c.fromFileName != "":
		data, err = os.ReadFile(c.fromFileName)
	case c.editor:
		return c.launchEditor(ctx, rep)
	default:
		return errors.Errorf("must specify either --from-file, --from-stdin or --editor")
	}

	if err != nil {
		return errors.Wrap(err, "error reading template")
	}

	//nolint:wrapcheck
	return notifytemplate.SetTemplate(ctx, rep, c.templateName, string(data))
}

func (c *commandNotificationTemplateSet) launchEditor(ctx context.Context, rep repo.RepositoryWriter) error {
	s, found, err := notifytemplate.GetTemplate(ctx, rep, c.templateName)
	if err != nil {
		return errors.Wrap(err, "unable to get template")
	}

	if !found {
		s, err = notifytemplate.GetEmbeddedTemplate(c.templateName)
		if err != nil {
			return errors.Wrap(err, "unable to get template")
		}
	}

	var lastUpdated string

	if err := editor.EditLoop(ctx, "template.md", s, false, func(updated string) error {
		_, err := notifytemplate.ParseTemplate(updated, notifytemplate.DefaultOptions)
		if err == nil {
			lastUpdated = updated
			return nil
		}

		return errors.Wrap(err, "invalid template")
	}); err != nil {
		return errors.Wrap(err, "unable to edit template")
	}

	//nolint:wrapcheck
	return notifytemplate.SetTemplate(ctx, rep, c.templateName, lastUpdated)
}
