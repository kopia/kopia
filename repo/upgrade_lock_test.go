package repo_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/repo/content"
)

func TestFormatUpgradeSetLock(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, content.FormatVersion1)
	formatBlockCacheDuration := env.Repository.ClientOptions().FormatBlobCacheDuration

	l := &content.UpgradeLock{
		OwnerID:                "upgrade-owner",
		CreationTime:           env.Repository.Time(),
		AdvanceNoticeDuration:  0,
		IODrainTimeout:         formatBlockCacheDuration * 2,
		StatusPollInterval:     formatBlockCacheDuration,
		Message:                "upgrading from format version 2 -> 3",
		MaxPermittedClockDrift: formatBlockCacheDuration / 3,
		OldFormatVersion:       env.RepositoryWriter.ContentManager().ContentFormat().MutableParameters.Version,
	}

	l, err := env.RepositoryWriter.SetUpgradeLockIntent(ctx, *l)
	require.NoError(t, err)

	l.OwnerID = "new-upgrade-owner"

	// verify that second owner cannot set / update the lock
	_, err = env.RepositoryWriter.SetUpgradeLockIntent(ctx, *l)
	require.EqualError(t, err,
		"failed to update the existing lock: upgrade owner-id mismatch \"new-upgrade-owner\" != \"upgrade-owner\", you are not the owner of the upgrade lock")

	l.OwnerID = "upgrade-owner"

	// push the advance notice
	l.AdvanceNoticeDuration *= 2

	// update the lock
	_, err = env.RepositoryWriter.SetUpgradeLockIntent(ctx, *l)
	require.NoError(t, err)

	require.NoError(t, env.RepositoryWriter.CommitUpgrade(ctx))
}

func TestFormatUpgradeAlreadyUpgraded(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, content.MaxFormatVersion)
	formatBlockCacheDuration := env.Repository.ClientOptions().FormatBlobCacheDuration

	l := &content.UpgradeLock{
		OwnerID:                "new-upgrade-owner",
		CreationTime:           env.Repository.Time(),
		AdvanceNoticeDuration:  0,
		IODrainTimeout:         formatBlockCacheDuration * 2,
		StatusPollInterval:     formatBlockCacheDuration,
		Message:                "upgrading from format version 2 -> 3",
		MaxPermittedClockDrift: formatBlockCacheDuration / 3,
		OldFormatVersion:       env.RepositoryWriter.ContentManager().ContentFormat().MutableParameters.Version,
	}

	_, err := env.RepositoryWriter.SetUpgradeLockIntent(ctx, *l)
	require.EqualError(t, err, fmt.Sprintf("repository is using version %d, and version %d is the maximum",
		content.MaxFormatVersion, content.MaxFormatVersion))
}

func TestFormatUpgradeCommit(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, content.FormatVersion1)
	formatBlockCacheDuration := env.Repository.ClientOptions().FormatBlobCacheDuration

	l := &content.UpgradeLock{
		OwnerID:                "upgrade-owner",
		CreationTime:           env.Repository.Time(),
		AdvanceNoticeDuration:  0,
		IODrainTimeout:         formatBlockCacheDuration * 2,
		StatusPollInterval:     formatBlockCacheDuration,
		Message:                "upgrading from format version 2 -> 3",
		MaxPermittedClockDrift: formatBlockCacheDuration / 3,
		OldFormatVersion:       env.RepositoryWriter.ContentManager().ContentFormat().MutableParameters.Version,
	}

	require.EqualError(t, env.RepositoryWriter.CommitUpgrade(ctx), "no upgrade in progress")

	_, err := env.RepositoryWriter.SetUpgradeLockIntent(ctx, *l)
	require.NoError(t, err)

	require.NoError(t, env.RepositoryWriter.CommitUpgrade(ctx))

	// verify that rollback after commit fails
	require.EqualError(t, env.RepositoryWriter.RollbackUpgrade(ctx), "no upgrade in progress")
}

func TestFormatUpgradeRollback(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, content.FormatVersion1)
	formatBlockCacheDuration := env.Repository.ClientOptions().FormatBlobCacheDuration

	l := &content.UpgradeLock{
		OwnerID:                "upgrade-owner",
		CreationTime:           env.Repository.Time(),
		AdvanceNoticeDuration:  0,
		IODrainTimeout:         formatBlockCacheDuration * 2,
		StatusPollInterval:     formatBlockCacheDuration,
		Message:                "upgrading from format version 2 -> 3",
		MaxPermittedClockDrift: formatBlockCacheDuration / 3,
		OldFormatVersion:       env.RepositoryWriter.ContentManager().ContentFormat().MutableParameters.Version,
	}

	_, err := env.RepositoryWriter.SetUpgradeLockIntent(ctx, *l)
	require.NoError(t, err)

	require.NoError(t, env.RepositoryWriter.RollbackUpgrade(ctx))

	// reopen the repo because we still have the lock in-memory
	env.MustReopen(t)

	// verify that commit after rollback fails
	require.EqualError(t, env.RepositoryWriter.CommitUpgrade(ctx), "no upgrade in progress")
}
