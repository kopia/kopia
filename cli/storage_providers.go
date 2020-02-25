package cli

import (
	"context"

	"github.com/pkg/errors"
	kingpin "gopkg.in/alecthomas/kingpin.v2"

	"github.com/kopia/kopia/repo/blob"
)

// RegisterStorageConnectFlags registers repository subcommand to connect to a storage
// or create new repository in a given storage.
func RegisterStorageConnectFlags(
	name, description string,
	flags func(*kingpin.CmdClause),
	connect func(ctx context.Context, isNew bool) (blob.Storage, error),
) {
	if name != "from-config" {
		// Set up 'create' subcommand
		cc := createCommand.Command(name, "Create repository in "+description)
		flags(cc)
		cc.Action(func(_ *kingpin.ParseContext) error {
			ctx := rootContext()
			st, err := connect(ctx, true)
			if err != nil {
				return errors.Wrap(err, "can't connect to storage")
			}

			return runCreateCommandWithStorage(ctx, st)
		})
	}

	// Set up 'connect' subcommand
	cc := connectCommand.Command(name, "Connect to repository in "+description)
	flags(cc)
	cc.Action(func(_ *kingpin.ParseContext) error {
		ctx := rootContext()
		st, err := connect(ctx, false)
		if err != nil {
			return errors.Wrap(err, "can't connect to storage")
		}

		return runConnectCommandWithStorage(ctx, st)
	})

	// Set up 'repair' subcommand
	cc = repairCommand.Command(name, "Repair repository in "+description)
	flags(cc)
	cc.Action(func(_ *kingpin.ParseContext) error {
		ctx := rootContext()
		st, err := connect(ctx, false)
		if err != nil {
			return errors.Wrap(err, "can't connect to storage")
		}

		return runRepairCommandWithStorage(ctx, st)
	})
}
