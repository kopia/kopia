package cli

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	"github.com/skratchdot/open-golang/open"

	"github.com/kopia/kopia/notification/notifytemplate"
	"github.com/kopia/kopia/repo"
)

type commandNotificationTemplateShow struct {
	notificationTemplateNameArg

	templateFormat string
	original       bool
	htmlOutput     bool

	out textOutput
}

func (c *commandNotificationTemplateShow) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("show", "Show template")
	c.notificationTemplateNameArg.setup(svc, cmd)

	cmd.Flag("format", "Template format").EnumVar(&c.templateFormat, "html", "md")
	cmd.Flag("original", "Show original template").BoolVar(&c.original)
	cmd.Flag("html", "Convert the output to HTML").BoolVar(&c.htmlOutput)
	cmd.Action(svc.repositoryReaderAction(c.run))

	c.out.setup(svc)
}

func (c *commandNotificationTemplateShow) run(ctx context.Context, rep repo.Repository) error {
	var (
		text string
		err  error
	)

	if c.original {
		text, err = notifytemplate.GetEmbeddedTemplate(c.templateName)
	} else {
		var found bool

		text, found, err = notifytemplate.GetTemplate(ctx, rep, c.templateName)
		if !found {
			text, err = notifytemplate.GetEmbeddedTemplate(c.templateName)
		}
	}

	if err != nil {
		return errors.Wrap(err, "error listing templates")
	}

	if c.htmlOutput {
		tf := filepath.Join(os.TempDir(), "kopia-template-preview.html")

		//nolint:gosec,mnd
		if err := os.WriteFile(tf, []byte(text), 0o644); err != nil {
			return errors.Wrap(err, "error writing template to file")
		}

		open.Run(tf) //nolint:errcheck
	}

	c.out.printStdout("%v\n", strings.TrimRight(text, "\n"))

	return nil
}
