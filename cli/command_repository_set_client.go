package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
)

var (
	repoClientOptionsCommand = repositoryCommands.Command("set-client", "Set repository client options.")

	repoClientOptionsReadOnly    = repoClientOptionsCommand.Flag("read-only", "Set repository to read-only").Bool()
	repoClientOptionsReadWrite   = repoClientOptionsCommand.Flag("read-write", "Set repository to read-write").Bool()
	repoClientOptionsDescription = repoClientOptionsCommand.Flag("description", "Change description").Strings()
	repoClientOptionsUsername    = repoClientOptionsCommand.Flag("username", "Change username").Strings()
	repoClientOptionsHostname    = repoClientOptionsCommand.Flag("hostname", "Change hostname").Strings()
)

func runRepoClientOptionsCommand(ctx context.Context, rep repo.Repository) error {
	var anyChange bool

	opt := rep.ClientOptions()

	if *repoClientOptionsReadOnly {
		if opt.ReadOnly {
			log(ctx).Infof("Repository is already in read-only mode.")
		} else {
			opt.ReadOnly = true
			anyChange = true

			log(ctx).Infof("Setting repository to read-only mode.")
		}
	}

	if *repoClientOptionsReadWrite {
		if !opt.ReadOnly {
			log(ctx).Infof("Repository is already in read-write mode.")
		} else {
			opt.ReadOnly = false
			anyChange = true

			log(ctx).Infof("Setting repository to read-write mode.")
		}
	}

	if v := *repoClientOptionsDescription; len(v) > 0 {
		opt.Description = v[0]
		anyChange = true

		log(ctx).Infof("Setting description to %v", opt.Description)
	}

	if v := *repoClientOptionsUsername; len(v) > 0 {
		opt.Username = v[0]
		anyChange = true

		log(ctx).Infof("Setting local username to %v", opt.Username)
	}

	if v := *repoClientOptionsHostname; len(v) > 0 {
		opt.Hostname = v[0]
		anyChange = true

		log(ctx).Infof("Setting local hostname to %v", opt.Hostname)
	}

	if !anyChange {
		return errors.Errorf("no changes")
	}

	return repo.SetClientOptions(ctx, repositoryConfigFileName(), opt)
}

func init() {
	repoClientOptionsCommand.Action(repositoryAction(runRepoClientOptionsCommand))
}
