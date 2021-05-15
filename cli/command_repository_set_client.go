package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
)

type commandRepositorySetClient struct {
	repoClientOptionsReadOnly    bool
	repoClientOptionsReadWrite   bool
	repoClientOptionsDescription []string
	repoClientOptionsUsername    []string
	repoClientOptionsHostname    []string

	svc appServices
}

func (c *commandRepositorySetClient) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("set-client", "Set repository client options.")

	cmd.Flag("read-only", "Set repository to read-only").BoolVar(&c.repoClientOptionsReadOnly)
	cmd.Flag("read-write", "Set repository to read-write").BoolVar(&c.repoClientOptionsReadWrite)
	cmd.Flag("description", "Change description").StringsVar(&c.repoClientOptionsDescription)
	cmd.Flag("username", "Change username").StringsVar(&c.repoClientOptionsUsername)
	cmd.Flag("hostname", "Change hostname").StringsVar(&c.repoClientOptionsHostname)
	cmd.Action(svc.repositoryReaderAction(c.run))

	c.svc = svc
}

func (c *commandRepositorySetClient) run(ctx context.Context, rep repo.Repository) error {
	var anyChange bool

	opt := rep.ClientOptions()

	if c.repoClientOptionsReadOnly {
		if opt.ReadOnly {
			log(ctx).Infof("Repository is already in read-only mode.")
		} else {
			opt.ReadOnly = true
			anyChange = true

			log(ctx).Infof("Setting repository to read-only mode.")
		}
	}

	if c.repoClientOptionsReadWrite {
		if !opt.ReadOnly {
			log(ctx).Infof("Repository is already in read-write mode.")
		} else {
			opt.ReadOnly = false
			anyChange = true

			log(ctx).Infof("Setting repository to read-write mode.")
		}
	}

	if v := c.repoClientOptionsDescription; len(v) > 0 {
		opt.Description = v[0]
		anyChange = true

		log(ctx).Infof("Setting description to %v", opt.Description)
	}

	if v := c.repoClientOptionsUsername; len(v) > 0 {
		opt.Username = v[0]
		anyChange = true

		log(ctx).Infof("Setting local username to %v", opt.Username)
	}

	if v := c.repoClientOptionsHostname; len(v) > 0 {
		opt.Hostname = v[0]
		anyChange = true

		log(ctx).Infof("Setting local hostname to %v", opt.Hostname)
	}

	if !anyChange {
		return errors.Errorf("no changes")
	}

	// nolint:wrapcheck
	return repo.SetClientOptions(ctx, c.svc.repositoryConfigFileName(), opt)
}
