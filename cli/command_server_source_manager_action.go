package cli

import (
	"context"
	"net/url"
	"path/filepath"

	"github.com/alecthomas/kingpin/v2"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/internal/serverapi"
)

// commandServerSourceManagerAction encapsulates common logic for all commands
// that operate on snapshot sources managed by the server and can be used to
// act upon them one-by-one or all at the same time.
type commandServerSourceManagerAction struct {
	sf serverClientFlags

	source string
	all    bool

	out textOutput
}

func (c *commandServerSourceManagerAction) setup(svc appServices, cmd *kingpin.CmdClause) {
	cmd.Flag("all", "All paths managed by server").BoolVar(&c.all)
	cmd.Arg("source", "Source path managed by server").StringVar(&c.source)

	c.sf.setup(svc, cmd)
	c.out.setup(svc)
}

func (c *commandServerSourceManagerAction) triggerActionOnMatchingSources(ctx context.Context, cli *apiclient.KopiaAPIClient, path string) error {
	var resp serverapi.MultipleSourceActionResponse

	uv := url.Values{}

	if !c.all {
		if c.source == "" {
			return errors.New("must specify source or --all")
		}

		absPath, err := filepath.Abs(c.source)
		if err != nil {
			return errors.Wrap(err, "unable to determine absolute path")
		}

		uv.Set("path", absPath)
	}

	if err := cli.Post(ctx, path+"?"+uv.Encode(), &serverapi.Empty{}, &resp); err != nil {
		return errors.Wrapf(err, "server returned error")
	}

	for src, resp := range resp.Sources {
		if resp.Success {
			log(ctx).Infof("Success %v", src)
		} else {
			log(ctx).Warnf("Failed %v", src)
		}
	}

	return nil
}
