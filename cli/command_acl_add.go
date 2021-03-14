package cli

import (
	"context"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/acl"
	"github.com/kopia/kopia/repo"
)

var (
	aclAddCommand            = aclCommands.Command("add", "Add ACL entry")
	aclAddCommandUser        = aclAddCommand.Flag("user", "User the ACL targets").Required().String()
	aclAddCommandTarget      = aclAddCommand.Flag("target", "Manifests targeted by the rule (type:T,key1:value1,...,keyN:valueN)").Required().String()
	aclAddCommandAccessLevel = aclAddCommand.Flag("access", "Access the user gets to subject").Required().Enum(acl.SupportedAccessLevels()...)
	aclAddCommandPriority    = aclAddCommand.Flag("priority", "Priority the ACL").Default("50").Int()
)

func runACLAdd(ctx context.Context, rep repo.RepositoryWriter) error {
	r := acl.TargetRule{}

	for _, v := range strings.Split(*aclAddCommandTarget, ",") {
		parts := strings.SplitN(v, "=", 2)
		if len(parts) != 2 { //nolint:gomnd
			return errors.Errorf("invalid target labels %q, must be key=value", v)
		}

		r[parts[0]] = parts[1]
	}

	e := &acl.Entry{
		User:     *aclAddCommandUser,
		Target:   r,
		Access:   acl.StringToAccessLevel[*aclAddCommandAccessLevel],
		Priority: *aclAddCommandPriority,
	}

	return acl.AddACL(ctx, rep, e)
}

func init() {
	aclAddCommand.Action(repositoryWriterAction(runACLAdd))
}
