package cli

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
)

type commandRepositorySetClient struct {
	repoClientOptionsReadOnly               bool
	repoClientOptionsReadWrite              bool
	repoClientOptionsPermissiveCacheLoading bool
	repoClientOptionsDescription            []string
	repoClientOptionsUsername               []string
	repoClientOptionsHostname               []string

	formatBlobCacheDuration time.Duration
	disableFormatBlobCache  bool

	svc appServices
}

func (c *commandRepositorySetClient) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("set-client", "Set repository client options.")

	cmd.Flag("read-only", "Set repository to read-only").BoolVar(&c.repoClientOptionsReadOnly)
	cmd.Flag("read-write", "Set repository to read-write").BoolVar(&c.repoClientOptionsReadWrite)
	cmd.Flag("permissive-cache-loading", "Do not fail when loading bad cache index entries.  Repository must be opened in read-only mode").Hidden().BoolVar(&c.repoClientOptionsPermissiveCacheLoading)
	cmd.Flag("description", "Change description").StringsVar(&c.repoClientOptionsDescription)
	cmd.Flag("username", "Change username").StringsVar(&c.repoClientOptionsUsername)
	cmd.Flag("hostname", "Change hostname").StringsVar(&c.repoClientOptionsHostname)
	cmd.Flag("repository-format-cache-duration", "Duration of kopia.repository format blob cache").DurationVar(&c.formatBlobCacheDuration)
	cmd.Flag("disable-repository-format-cache", "Disable caching of kopia.repository format blob").BoolVar(&c.disableFormatBlobCache)
	cmd.Action(svc.repositoryReaderAction(c.run))

	c.svc = svc
}

func (c *commandRepositorySetClient) run(ctx context.Context, rep repo.Repository) error {
	var anyChange bool

	opt := rep.ClientOptions()

	if c.repoClientOptionsReadOnly {
		if opt.ReadOnly {
			log(ctx).Info("Repository is already in read-only mode.")
		} else {
			opt.ReadOnly = true
			anyChange = true

			log(ctx).Info("Setting repository to read-only mode.")
		}
	}

	if c.repoClientOptionsReadWrite {
		if !opt.ReadOnly {
			log(ctx).Info("Repository is already in read-write mode.")
		} else {
			opt.ReadOnly = false
			anyChange = true

			log(ctx).Info("Setting repository to read-write mode.")
		}
	}

	if c.repoClientOptionsPermissiveCacheLoading {
		if !opt.PermissiveCacheLoading {
			log(ctx).Info("Repository fails on read of bad index blobs.")
		} else {
			opt.PermissiveCacheLoading = true
			anyChange = true

			log(ctx).Info("Setting to load indicies into cache permissively.")
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

	if v := c.formatBlobCacheDuration; v != 0 {
		opt.FormatBlobCacheDuration = v
		anyChange = true

		log(ctx).Infof("Setting format blob cache duration to %v", v)
	}

	if c.disableFormatBlobCache {
		opt.FormatBlobCacheDuration = -1
		anyChange = true

		log(ctx).Info("Disabling format blob cache")
	}

	if !anyChange {
		return errors.New("no changes")
	}

	//nolint:wrapcheck
	return repo.SetClientOptions(ctx, c.svc.repositoryConfigFileName(), opt)
}
