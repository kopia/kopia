package cli

import (
	"context"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/acl"
	"github.com/kopia/kopia/repo"
)

type commandACLAdd struct {
	user      string
	target    string
	level     string
	overwrite bool
}

func (c *commandACLAdd) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("add", "Add ACL entry")
	cmd.Flag("user", "User the ACL targets").Required().StringVar(&c.user)
	cmd.Flag("target", "Manifests targeted by the rule (type:T,key1:value1,...,keyN:valueN)").Required().StringVar(&c.target)
	cmd.Flag("access", "Access the user gets to subject").Required().EnumVar(&c.level, acl.SupportedAccessLevels()...)
	cmd.Flag("overwrite", "Overwrite existing rule with the same user and target").BoolVar(&c.overwrite)
	cmd.Action(svc.repositoryWriterAction(c.run))
}

func (c *commandACLAdd) run(ctx context.Context, rep repo.RepositoryWriter) error {
	r := acl.TargetRule{}

	for _, v := range strings.Split(c.target, ",") {
		parts := strings.SplitN(v, "=", 2) //nolint:mnd
		if len(parts) != 2 {               //nolint:mnd
			return errors.Errorf("invalid target labels %q, must be key=value", v)
		}

		r[parts[0]] = parts[1]
	}

	al, err := acl.ParseAccessLevel(c.level)
	if err != nil {
		return errors.Wrap(err, "invalid access level")
	}

	e := &acl.Entry{
		User:   c.user,
		Target: r,
		Access: al,
	}

	return errors.Wrap(acl.AddACL(ctx, rep, e, c.overwrite), "error adding ACL entry")
}
