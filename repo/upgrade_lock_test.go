package repo_test

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

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
	ctx, env := repotesting.NewEnvironment(t, content.FormatVersion1, repotesting.Options{OpenOptions: func(opts *repo.Options) {
		// nolint:goconst
		opts.UpgradeOwnerID = "upgrade-owner"
	}})
	formatBlockCacheDuration := env.Repository.ClientOptions().FormatBlobCacheDuration

	l := &content.UpgradeLock{
		CreationTime:           env.Repository.Time(),
		AdvanceNoticeDuration:  15 * time.Hour,
		IODrainTimeout:         formatBlockCacheDuration * 2,
		StatusPollInterval:     formatBlockCacheDuration,
		Message:                "upgrading from format version 2 -> 3",
		MaxPermittedClockDrift: formatBlockCacheDuration / 3,
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
	}

	_, err := env.RepositoryWriter.SetUpgradeLockIntent(ctx, *l)
	require.EqualError(t, err, fmt.Sprintf("repository is using version %d, and version %d is the maximum",
		content.MaxFormatVersion, content.MaxFormatVersion))
}

func TestFormatUpgradeCommit(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, content.FormatVersion1, repotesting.Options{OpenOptions: func(opts *repo.Options) {
		opts.UpgradeOwnerID = "upgrade-owner"
	}})
	formatBlockCacheDuration := env.Repository.ClientOptions().FormatBlobCacheDuration

	l := &content.UpgradeLock{
		OwnerID:                "upgrade-owner",
		CreationTime:           env.Repository.Time(),
		AdvanceNoticeDuration:  0,
		IODrainTimeout:         formatBlockCacheDuration * 2,
		StatusPollInterval:     formatBlockCacheDuration,
		Message:                "upgrading from format version 2 -> 3",
		MaxPermittedClockDrift: formatBlockCacheDuration / 3,
	}

	require.EqualError(t, env.RepositoryWriter.CommitUpgrade(ctx), "no upgrade in progress")

	_, err := env.RepositoryWriter.SetUpgradeLockIntent(ctx, *l)
	require.NoError(t, err)

	require.NoError(t, env.RepositoryWriter.CommitUpgrade(ctx))

	// verify that rollback after commit fails
	require.EqualError(t, env.RepositoryWriter.RollbackUpgrade(ctx), "no upgrade in progress")
}

func TestFormatUpgradeRollback(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, content.FormatVersion1, repotesting.Options{OpenOptions: func(opts *repo.Options) {
		opts.UpgradeOwnerID = "upgrade-owner"
	}})
	formatBlockCacheDuration := env.Repository.ClientOptions().FormatBlobCacheDuration

	l := &content.UpgradeLock{
		OwnerID:                "upgrade-owner",
		CreationTime:           env.Repository.Time(),
		AdvanceNoticeDuration:  0,
		IODrainTimeout:         formatBlockCacheDuration * 2,
		StatusPollInterval:     formatBlockCacheDuration,
		Message:                "upgrading from format version 2 -> 3",
		MaxPermittedClockDrift: formatBlockCacheDuration / 3,
	}

	_, err := env.RepositoryWriter.SetUpgradeLockIntent(ctx, *l)
	require.NoError(t, err)

	require.NoError(t, env.RepositoryWriter.RollbackUpgrade(ctx))

	// reopen the repo because we still have the lock in-memory
	env.MustReopen(t)

	// verify that commit after rollback fails
	require.EqualError(t, env.RepositoryWriter.CommitUpgrade(ctx), "no upgrade in progress")
}

func TestFormatUpgradeMultipleLocksRollback(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, content.FormatVersion1, repotesting.Options{OpenOptions: func(opts *repo.Options) {
		opts.UpgradeOwnerID = "upgrade-owner"
	}})
	formatBlockCacheDuration := env.Repository.ClientOptions().FormatBlobCacheDuration

	l := &content.UpgradeLock{
		OwnerID:                "upgrade-owner",
		CreationTime:           env.Repository.Time(),
		AdvanceNoticeDuration:  0,
		IODrainTimeout:         formatBlockCacheDuration * 2,
		StatusPollInterval:     formatBlockCacheDuration,
		Message:                "upgrading from format version 2 -> 3",
		MaxPermittedClockDrift: formatBlockCacheDuration / 3,
	}

	secondWriter := env.MustOpenAnother(t, func(opts *repo.Options) {
		opts.UpgradeOwnerID = "upgrade-owner"
	})

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
	env.MustReopen(t, func(opts *repo.Options) {
		opts.UpgradeOwnerID = "another-upgrade-owner"
	})
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

	r, err := repo.Open(testlogging.Context(t), configFile, "password", &repo.Options{UpgradeOwnerID: "allowed-upgrade-owner"})
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

func TestFormatUpgradeDuringOngoingWriteSessions(t *testing.T) {
	curTime := clock.Now()
	ctx, env := repotesting.NewEnvironment(t, content.FormatVersion1, repotesting.Options{
		// new environment with controlled time
		OpenOptions: func(opts *repo.Options) {
			opts.TimeNowFunc = func() time.Time {
				return curTime
			}
		},
	})

	rep := env.Repository // read-only

	lw := rep.(repo.RepositoryWriter)

	// w1, w2, w3 are indepdendent sessions.
	_, w1, err := rep.NewWriter(ctx, repo.WriteSessionOptions{Purpose: "writer1"})
	require.NoError(t, err)

	defer w1.Close(ctx)

	_, w2, err := rep.NewWriter(ctx, repo.WriteSessionOptions{Purpose: "writer2"})
	require.NoError(t, err)

	defer w2.Close(ctx)

	_, w3, err := rep.NewWriter(ctx, repo.WriteSessionOptions{Purpose: "writer3"})
	require.NoError(t, err)

	defer w3.Close(ctx)

	o1Data := []byte{1, 2, 3}
	o2Data := []byte{2, 3, 4}
	o3Data := []byte{3, 4, 5}
	o4Data := []byte{4, 5, 6}

	writeObject(ctx, t, w1, o1Data, "o1")
	writeObject(ctx, t, w2, o2Data, "o2")
	writeObject(ctx, t, w3, o3Data, "o3")
	writeObject(ctx, t, lw, o4Data, "o4")

	formatBlockCacheDuration := env.Repository.ClientOptions().FormatBlobCacheDuration
	l := content.UpgradeLock{
		OwnerID:                "upgrade-owner",
		CreationTime:           env.Repository.Time(),
		AdvanceNoticeDuration:  0,
		IODrainTimeout:         formatBlockCacheDuration * 2,
		StatusPollInterval:     formatBlockCacheDuration,
		Message:                "upgrading from format version 2 -> 3",
		MaxPermittedClockDrift: formatBlockCacheDuration / 3,
	}

	_, err = env.RepositoryWriter.SetUpgradeLockIntent(ctx, l)
	require.NoError(t, err)

	// ongoing writes should NOT get interrupted because the upgrade lock
	// monitor could not have noticed the lock yet
	require.NoError(t, w1.Flush(ctx))
	require.NoError(t, w2.Flush(ctx))
	require.NoError(t, w3.Flush(ctx))
	require.NoError(t, lw.Flush(ctx))

	o5Data := []byte{7, 8, 9}
	o6Data := []byte{10, 11, 12}
	o7Data := []byte{13, 14, 15}
	o8Data := []byte{16, 17, 18}

	writeObject(ctx, t, w1, o5Data, "o5")
	writeObject(ctx, t, w2, o6Data, "o6")
	writeObject(ctx, t, w3, o7Data, "o7")
	writeObject(ctx, t, lw, o8Data, "o8")

	// move time forward by the lock refresh interval
	curTime = curTime.Add(formatBlockCacheDuration + time.Second)

	// ongoing writes should get interrupted this time
	require.ErrorIs(t, w1.Flush(ctx), repo.ErrRepositoryUnavailableDueToUpgrageInProgress)
	require.ErrorIs(t, w2.Flush(ctx), repo.ErrRepositoryUnavailableDueToUpgrageInProgress)
	require.ErrorIs(t, w3.Flush(ctx), repo.ErrRepositoryUnavailableDueToUpgrageInProgress)
	require.ErrorIs(t, lw.Flush(ctx), repo.ErrRepositoryUnavailableDueToUpgrageInProgress)
}
