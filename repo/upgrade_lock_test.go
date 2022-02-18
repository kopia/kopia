package repo_test

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/beforeop"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/kopia/kopia/repo/object"
)

func TestFormatUpgradeSetLock(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, content.FormatVersion1)
	formatBlockCacheDuration := env.Repository.ClientOptions().FormatBlobCacheDuration

	l := &content.UpgradeLock{
		CreationTime:           env.Repository.Time(),
		AdvanceNoticeDuration:  0,
		IODrainTimeout:         formatBlockCacheDuration * 2,
		StatusPollInterval:     formatBlockCacheDuration,
		Message:                "upgrading from format version 2 -> 3",
		MaxPermittedClockDrift: formatBlockCacheDuration / 3,
		OldFormatVersion:       env.RepositoryWriter.ContentManager().ContentFormat().MutableParameters.Version,
	}

	// set invalid lock
	_, err := env.RepositoryWriter.SetUpgradeLockIntent(ctx, *l)
	require.EqualError(t, err, "invalid upgrade lock intent: no owner-id set, it is required to set a unique owner-id")

	l.OwnerID = "upgrade-owner"
	l, err = env.RepositoryWriter.SetUpgradeLockIntent(ctx, *l)
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

func TestFormatMultipleLocksRollback(t *testing.T) {
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

	secondWriter := env.MustOpenAnother(t)

	// first lock by primary creator
	_, err := env.RepositoryWriter.SetUpgradeLockIntent(ctx, *l)
	require.NoError(t, err)

	// second lock from a random owner
	secondL := l.Clone()
	secondL.OwnerID = "another-upgrade-owner"
	_, err = secondWriter.(repo.DirectRepositoryWriter).SetUpgradeLockIntent(ctx, *secondL)
	require.NoError(t, err)

	// verify that we have two repository backups, the second one will contain
	// the first owner's lock
	{
		var backups []string
		require.NoError(t, env.RootStorage().ListBlobs(ctx, repo.FormatBlobBackupIDPrefix, func(bm blob.Metadata) error {
			backups = append(backups, string(bm.BlobID))
			return nil
		}))
		sort.Strings(backups)
		require.Equal(t, []string{string(repo.FormatBlobBackupID(*secondL)), string(repo.FormatBlobBackupID(*l))},
			backups, "invalid backups list")
	}

	// verify that we have upgraded our format version
	env.MustReopen(t)
	require.Equal(t, content.FormatVersion2,
		env.RepositoryWriter.ContentManager().ContentFormat().MutableParameters.Version)

	require.NoError(t, env.RepositoryWriter.RollbackUpgrade(ctx))

	// verify that we have no repository backups pending
	require.NoError(t, env.RootStorage().ListBlobs(ctx, repo.FormatBlobBackupIDPrefix, func(bm blob.Metadata) error {
		t.Fatalf("found unexpected backup: %s", bm.BlobID)
		return nil
	}))

	// reopen the repo because we still have the lock in-memory
	env.MustReopen(t)

	// verify that commit after rollback fails, this ensures that the correct
	// backup got restored because if the second backup was restored then we'd
	// still get a lock to be committed without any error
	require.EqualError(t, env.RepositoryWriter.CommitUpgrade(ctx), "no upgrade in progress")

	// verify that we are back to the original version where we started from
	require.Equal(t, content.FormatVersion1,
		env.RepositoryWriter.ContentManager().ContentFormat().MutableParameters.Version)
}

func TestFormatUpgradeFailureToBackupFormatBlobOnLock(t *testing.T) {
	// this lock will be allowed by the backend to create backups
	allowedLock := content.UpgradeLock{
		OwnerID:                "allowed-upgrade-owner",
		CreationTime:           clock.Now(),
		AdvanceNoticeDuration:  0,
		IODrainTimeout:         30,
		StatusPollInterval:     15,
		Message:                "upgrading from format version 2 -> 3",
		MaxPermittedClockDrift: 5,
		OldFormatVersion:       content.FormatVersion1,
	}
	// this lock will not be allowed by the backend to create backups
	faultyLock := allowedLock.Clone()
	faultyLock.OwnerID = "faulty-upgrade-owner"

	allowDeletes, allowGets, allowPuts := false, true, true
	st := repotesting.NewReconnectableStorage(t, beforeop.NewWrapper(
		blobtesting.NewVersionedMapStorage(nil),
		// GetBlob filter
		func(id blob.ID) error {
			if !allowGets && id == repo.FormatBlobBackupID(allowedLock) {
				return errors.New("unexpected error on get")
			}
			return nil
		}, nil,
		func() error {
			if allowDeletes {
				return nil
			}
			// all deletes are disallowed unless requested
			return errors.New("unexpected error")
		},
		// PutBlob callback
		func(id blob.ID, _ *blob.PutOptions) error {
			if !allowPuts || (strings.HasPrefix(string(id), repo.FormatBlobBackupIDPrefix) && id != repo.FormatBlobBackupID(allowedLock)) {
				return errors.New("unexpected error")
			}
			return nil
		},
	))

	opt := &repo.NewRepositoryOptions{
		BlockFormat: content.FormattingOptions{
			MutableParameters: content.MutableParameters{
				Version: content.FormatVersion1,
			},
			HMACSecret:           []byte{},
			Hash:                 "HMAC-SHA256",
			Encryption:           encryption.DefaultAlgorithm,
			EnablePasswordChange: true,
		},
		ObjectFormat: object.Format{
			Splitter: "FIXED-1M",
		},
	}
	require.NoError(t, repo.Initialize(testlogging.Context(t), st, opt, "password"))

	configFile := filepath.Join(testutil.TempDirectory(t), ".kopia.config")
	defer os.Remove(configFile)

	connectOpts := repo.ConnectOptions{CachingOptions: content.CachingOptions{CacheDirectory: testutil.TempDirectory(t)}}
	defer os.RemoveAll(connectOpts.CacheDirectory)

	require.NoError(t, repo.Connect(testlogging.Context(t), configFile, st, "password", &connectOpts))

	r, err := repo.Open(testlogging.Context(t), configFile, "password", nil)
	require.NoError(t, err)

	_, err = r.(repo.DirectRepositoryWriter).SetUpgradeLockIntent(testlogging.Context(t), *faultyLock)
	require.EqualError(t, err, "failed to backup the repo format blob: unable to write format blob \"kopia.repository.backup.faulty-upgrade-owner\": unexpected error")

	_, err = r.(repo.DirectRepositoryWriter).SetUpgradeLockIntent(testlogging.Context(t), allowedLock)
	require.NoError(t, err)

	require.EqualError(t, r.(repo.DirectRepositoryWriter).RollbackUpgrade(testlogging.Context(t)),
		"failed to delete the format blob backup \"kopia.repository.backup.allowed-upgrade-owner\": unexpected error")

	require.EqualError(t, r.(repo.DirectRepositoryWriter).RollbackUpgrade(testlogging.Context(t)),
		"failed to delete the format blob backup \"kopia.repository.backup.allowed-upgrade-owner\": unexpected error")

	allowPuts = false

	require.EqualError(t, r.(repo.DirectRepositoryWriter).RollbackUpgrade(testlogging.Context(t)),
		"failed to restore format blob from backup \"kopia.repository.backup.allowed-upgrade-owner\": unexpected error")

	allowGets = false

	require.EqualError(t, r.(repo.DirectRepositoryWriter).RollbackUpgrade(testlogging.Context(t)),
		"failed to read from backup \"kopia.repository.backup.allowed-upgrade-owner\": unexpected error on get")

	allowPuts, allowGets, allowDeletes = true, true, true

	require.NoError(t, r.(repo.DirectRepositoryWriter).RollbackUpgrade(testlogging.Context(t)))
}
