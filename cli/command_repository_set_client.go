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
			printStderr("Repository is already in read-only mode.\n")
		} else {
			opt.ReadOnly = true
			anyChange = true

			printStderr("Setting repository to read-only mode.\n")
		}
	}

	if *repoClientOptionsReadWrite {
		if !opt.ReadOnly {
			printStderr("Repository is already in read-write mode.\n")
		} else {
			opt.ReadOnly = false
			anyChange = true

			printStderr("Setting repository to read-write mode.\n")
		}
	}

	if v := *repoClientOptionsDescription; len(v) > 0 {
		opt.Description = v[0]
		anyChange = true

		printStderr("Setting description to %v\n", opt.Description)
	}

	if v := *repoClientOptionsUsername; len(v) > 0 {
		opt.Username = v[0]
		anyChange = true

		printStderr("Setting local username to %v\n", opt.Username)
	}

	if v := *repoClientOptionsHostname; len(v) > 0 {
		opt.Hostname = v[0]
		anyChange = true

		printStderr("Setting local hostname to %v\n", opt.Hostname)
	}

	if !anyChange {
		return errors.Errorf("no changes")
	}

	return repo.SetClientOptions(ctx, repositoryConfigFileName(), opt)
}

func init() {
	repoClientOptionsCommand.Action(repositoryAction(runRepoClientOptionsCommand))
}
