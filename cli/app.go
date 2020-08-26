// Package cli implements command-line commands for the Kopia.
package cli

import (
	"context"
	"net/http"
	"os"

	"github.com/fatih/color"
	"github.com/pkg/errors"
	kingpin "gopkg.in/alecthomas/kingpin.v2"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/repo/maintenance"
	"github.com/kopia/kopia/snapshot/snapshotmaintenance"
)

var log = logging.GetContextLoggerFunc("kopia/cli")

var (
	defaultColor = color.New()
	warningColor = color.New(color.FgYellow)
	errorColor   = color.New(color.FgHiRed)
)

var (
	app = kingpin.New("kopia", "Kopia - Online Backup").Author("http://kopia.github.io/")

	enableAutomaticMaintenance = app.Flag("auto-maintenance", "Automatic maintenance").Default("true").Hidden().Bool()

	_ = app.Flag("help-full", "Show help for all commands, including hidden").Action(helpFullAction).Bool()

	repositoryCommands  = app.Command("repository", "Commands to manipulate repository.").Alias("repo")
	cacheCommands       = app.Command("cache", "Commands to manipulate local cache").Hidden()
	snapshotCommands    = app.Command("snapshot", "Commands to manipulate snapshots.").Alias("snap")
	policyCommands      = app.Command("policy", "Commands to manipulate snapshotting policies.").Alias("policies")
	serverCommands      = app.Command("server", "Commands to control HTTP API server.")
	manifestCommands    = app.Command("manifest", "Low-level commands to manipulate manifest items.").Hidden()
	contentCommands     = app.Command("content", "Commands to manipulate content in repository.").Alias("contents").Hidden()
	blobCommands        = app.Command("blob", "Commands to manipulate BLOBs.").Hidden()
	indexCommands       = app.Command("index", "Commands to manipulate content index.").Hidden()
	benchmarkCommands   = app.Command("benchmark", "Commands to test performance of algorithms.").Hidden()
	maintenanceCommands = app.Command("maintenance", "Maintenance commands.").Hidden().Alias("gc")
)

func helpFullAction(ctx *kingpin.ParseContext) error {
	_ = app.UsageForContextWithTemplate(ctx, 0, kingpin.DefaultUsageTemplate)

	os.Exit(0)

	return nil
}

func noRepositoryAction(act func(ctx context.Context) error) func(ctx *kingpin.ParseContext) error {
	return func(_ *kingpin.ParseContext) error {
		return act(rootContext())
	}
}

func serverAction(act func(ctx context.Context, cli *apiclient.KopiaAPIClient) error) func(ctx *kingpin.ParseContext) error {
	return func(_ *kingpin.ParseContext) error {
		opts, err := serverAPIClientOptions()
		if err != nil {
			return errors.Wrap(err, "unable to create API client options")
		}

		apiClient, err := apiclient.NewKopiaAPIClient(opts)
		if err != nil {
			return errors.Wrap(err, "unable to create API client")
		}

		return act(rootContext(), apiClient)
	}
}

func assertDirectRepository(act func(ctx context.Context, rep *repo.DirectRepository) error) func(ctx context.Context, rep repo.Repository) error {
	return func(ctx context.Context, rep repo.Repository) error {
		if rep == nil {
			return act(ctx, nil)
		}

		// right now this assertion never fails,
		// but will fail in the future when we have remote repository implementation
		lr, ok := rep.(*repo.DirectRepository)
		if !ok {
			return errors.Errorf("operation supported only on direct repository")
		}

		return act(ctx, lr)
	}
}

func directRepositoryAction(act func(ctx context.Context, rep *repo.DirectRepository) error) func(ctx *kingpin.ParseContext) error {
	return maybeRepositoryAction(assertDirectRepository(act), true)
}

func optionalRepositoryAction(act func(ctx context.Context, rep repo.Repository) error) func(ctx *kingpin.ParseContext) error {
	return maybeRepositoryAction(act, false)
}

func repositoryAction(act func(ctx context.Context, rep repo.Repository) error) func(ctx *kingpin.ParseContext) error {
	return maybeRepositoryAction(act, true)
}

func rootContext() context.Context {
	ctx := context.Background()
	ctx = content.UsingContentCache(ctx, *enableCaching)
	ctx = content.UsingListCache(ctx, *enableListCaching)
	ctx = blob.WithUploadProgressCallback(ctx, func(desc string, bytesSent, totalBytes int64) {
		if bytesSent >= totalBytes {
			log(ctx).Debugf("Uploaded %v %v %v", desc, bytesSent, totalBytes)
			progress.UploadedBytes(totalBytes)
		}
	})

	return ctx
}

func maybeRepositoryAction(act func(ctx context.Context, rep repo.Repository) error, required bool) func(ctx *kingpin.ParseContext) error {
	return func(kpc *kingpin.ParseContext) error {
		return withProfiling(func() error {
			ctx := rootContext()

			startMemoryTracking(ctx)
			defer finishMemoryTracking(ctx)

			if *metricsListenAddr != "" {
				mux := http.NewServeMux()
				if err := initPrometheus(mux); err != nil {
					return errors.Wrap(err, "unable to initialize prometheus.")
				}

				log(ctx).Infof("starting prometheus metrics on %v", *metricsListenAddr)
				go http.ListenAndServe(*metricsListenAddr, mux) // nolint:errcheck
			}

			rep, err := openRepository(ctx, nil, required)
			if err != nil && required {
				return errors.Wrap(err, "open repository")
			}

			err = act(ctx, rep)

			if rep != nil {
				if merr := maybeRunMaintenance(ctx, rep); merr != nil {
					log(ctx).Warningf("error running maintenance: %v", merr)
				}
			}

			if rep != nil && required {
				if cerr := rep.Close(ctx); cerr != nil {
					return errors.Wrap(cerr, "unable to close repository")
				}
			}

			return err
		})
	}
}

func maybeRunMaintenance(ctx context.Context, rep repo.Repository) error {
	if !*enableAutomaticMaintenance {
		return nil
	}

	if rep.IsReadOnly() {
		return nil
	}

	err := snapshotmaintenance.Run(ctx, rep, maintenance.ModeAuto, false)
	if err == nil {
		return nil
	}

	if _, ok := err.(maintenance.NotOwnedError); ok {
		// do not report the NotOwnedError to the user since this is automatic maintenance.
		return nil
	}

	return err
}

// App returns an instance of command-line application object.
func App() *kingpin.Application {
	return app
}
