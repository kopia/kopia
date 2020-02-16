// Package cli implements command-line commands for the Kopia.
package cli

import (
	"context"
	"os"

	"github.com/pkg/errors"
	kingpin "gopkg.in/alecthomas/kingpin.v2"

	"github.com/kopia/kopia/internal/kopialogging"
	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
)

var log = kopialogging.Logger("kopia/cli")

var (
	app = kingpin.New("kopia", "Kopia - Online Backup").Author("http://kopia.github.io/")

	_ = app.Flag("help-full", "Show help for all commands, including hidden").Action(helpFullAction).Bool()

	repositoryCommands = app.Command("repository", "Commands to manipulate repository.").Alias("repo")
	cacheCommands      = app.Command("cache", "Commands to manipulate local cache").Hidden()
	snapshotCommands   = app.Command("snapshot", "Commands to manipulate snapshots.").Alias("snap")
	policyCommands     = app.Command("policy", "Commands to manipulate snapshotting policies.").Alias("policies")
	serverCommands     = app.Command("server", "Commands to control HTTP API server.")
	manifestCommands   = app.Command("manifest", "Low-level commands to manipulate manifest items.").Hidden()
	contentCommands    = app.Command("content", "Commands to manipulate content in repository.").Alias("contents").Hidden()
	blobCommands       = app.Command("blob", "Commands to manipulate BLOBs.").Hidden()
	indexCommands      = app.Command("index", "Commands to manipulate content index.").Hidden()
	benchmarkCommands  = app.Command("benchmark", "Commands to test performance of algorithms.").Hidden()
)

func helpFullAction(ctx *kingpin.ParseContext) error {
	_ = app.UsageForContextWithTemplate(ctx, 0, kingpin.DefaultUsageTemplate)

	os.Exit(0)

	return nil
}

func noRepositoryAction(act func(ctx context.Context) error) func(ctx *kingpin.ParseContext) error {
	return func(_ *kingpin.ParseContext) error {
		return act(context.Background())
	}
}

func serverAction(act func(ctx context.Context, cli *serverapi.Client) error) func(ctx *kingpin.ParseContext) error {
	return func(_ *kingpin.ParseContext) error {
		opts, err := serverAPIClientOptions()
		if err != nil {
			return errors.Wrap(err, "unable to create API client options")
		}

		apiClient, err := serverapi.NewClient(opts)
		if err != nil {
			return errors.Wrap(err, "unable to create API client")
		}

		return act(context.Background(), apiClient)
	}
}

func repositoryAction(act func(ctx context.Context, rep *repo.Repository) error) func(ctx *kingpin.ParseContext) error {
	return maybeRepositoryAction(act, true)
}

func optionalRepositoryAction(act func(ctx context.Context, rep *repo.Repository) error) func(ctx *kingpin.ParseContext) error {
	return maybeRepositoryAction(act, false)
}

func maybeRepositoryAction(act func(ctx context.Context, rep *repo.Repository) error, required bool) func(ctx *kingpin.ParseContext) error {
	return func(kpc *kingpin.ParseContext) error {
		return withProfiling(func() error {
			startMemoryTracking()
			defer finishMemoryTracking()

			ctx := context.Background()
			ctx = content.UsingContentCache(ctx, *enableCaching)
			ctx = content.UsingListCache(ctx, *enableListCaching)
			ctx = blob.WithUploadProgressCallback(ctx, func(desc string, bytesSent, totalBytes int64) {
				if bytesSent >= totalBytes {
					log.Debugf("Uploaded %v %v %v", desc, bytesSent, totalBytes)
					progress.UploadedBytes(totalBytes)
				}
			})

			rep, err := openRepository(ctx, nil, required)
			if err != nil && required {
				return errors.Wrap(err, "open repository")
			}

			err = act(ctx, rep)
			if rep != nil && required {
				if cerr := rep.Close(ctx); cerr != nil {
					return errors.Wrap(cerr, "unable to close repository")
				}
			}
			return err
		})
	}
}

// App returns an instance of command-line application object.
func App() *kingpin.Application {
	return app
}
