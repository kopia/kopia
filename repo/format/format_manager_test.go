package format_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/epoch"
	"github.com/kopia/kopia/internal/faketime"
	"github.com/kopia/kopia/internal/feature"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/kopia/kopia/repo/format"
	"github.com/kopia/kopia/repo/hashing"
)

var (
	errSomeError = errors.New("some error")

	cf = format.ContentFormat{
		MutableParameters: format.MutableParameters{
			Version:         format.FormatVersion1,
			EpochParameters: epoch.DefaultParameters(),
			MaxPackSize:     20e6,
			IndexVersion:    2,
		},
		Hash:       hashing.DefaultAlgorithm,
		Encryption: encryption.DefaultAlgorithm,
		HMACSecret: []byte{1, 2, 3, 4, 5},
	}

	uli = &format.UpgradeLockIntent{
		OwnerID: "foo@bar",
	}

	rc = &format.RepositoryConfig{
		ContentFormat: cf,
		UpgradeLock:   uli,
	}

	cacheDuration = 10 * time.Minute
)

func TestFormatManager(t *testing.T) {
	ctx := testlogging.Context(t)

	startTime := time.Date(2020, 1, 1, 12, 0, 0, 0, time.UTC)
	ta := faketime.NewTimeAdvance(startTime)
	nowFunc := ta.NowFunc()
	blobCache := format.NewMemoryBlobCache(nowFunc)

	st := blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, nil)
	fst := blobtesting.NewFaultyStorage(st)
	require.NoError(t, format.Initialize(ctx, fst, &format.KopiaRepositoryJSON{}, rc, format.BlobStorageConfiguration{}, "some-password"))

	rawBytes := mustGetBytes(t, st, "kopia.repository")

	mgr, err := format.NewManagerWithCache(ctx, fst, cacheDuration, "some-password", nowFunc, blobCache)
	require.NoError(t, err)

	require.Equal(t, cf.HMACSecret, mgr.GetHmacSecret())
	require.Equal(t, cf.Encryption, mgr.GetEncryptionAlgorithm())
	require.Equal(t, cf.Hash, mgr.GetHashFunction())
	require.NotNil(t, mgr.HashFunc())
	require.NotNil(t, mgr.Encryptor())
	require.Equal(t, cf.MasterKey, mgr.GetMasterKey())
	require.False(t, mgr.SupportsPasswordChange())
	require.Equal(t, startTime, mgr.LoadedTime())
	require.Equal(t, cf.MutableParameters, mustGetMutableParameters(t, mgr))
	require.True(t, bytes.Contains(mustGetRepositoryFormatBytes(t, mgr), rawBytes))
	require.Equal(t, uli, mustGetUpgradeLockIntent(t, mgr))

	// move time to be 1ns shy of when the cache expires
	fst.AddFault(blobtesting.MethodGetBlob).ErrorInstead(errSomeError)
	ta.Advance(cacheDuration - 1)

	// despite the failure, we still trust the cache
	mustGetMutableParameters(t, mgr)

	// now move the final nanosecond, this will trigger a load and storage errors
	ta.Advance(1)

	// error on first read, subsequent reads are ok
	require.ErrorIs(t, expectMutableParametersError(t, mgr), errSomeError)
	mustGetMutableParameters(t, mgr)
	mustGetMutableParameters(t, mgr)

	n := mgr.LoadedTime()

	require.Equal(t, 2, mgr.RefreshCount())

	// open another manager when cache is still valid, it will reuse old cached time
	ta.Advance(5)

	mgr2, err := format.NewManagerWithCache(ctx, fst, cacheDuration, "some-password", nowFunc, blobCache)
	require.NoError(t, err)

	mustGetMutableParameters(t, mgr2)

	require.Equal(t, n, mgr2.LoadedTime())
	// open another manager when cache has already expired
	ta.Advance(2 * cacheDuration)

	n = ta.NowFunc()()

	mgr3, err := format.NewManagerWithCache(ctx, fst, cacheDuration, "some-password", nowFunc, blobCache)
	require.NoError(t, err)

	// make sure we're using current time
	require.Equal(t, n, mgr3.LoadedTime())

	// update using mgr3
	mp := mustGetMutableParameters(t, mgr3)
	bc2 := mustGetBlobStorageConfiguration(t, mgr3)
	rf2 := mustGetRequiredFeatures(t, mgr3)

	// make some changes
	mp.MaxPackSize++

	require.NoError(t, mgr3.SetParameters(ctx, mp, bc2, rf2))

	// enough time has passed since last read, so mgr will notice the update immediately
	require.Equal(t, mp, mustGetMutableParameters(t, mgr))

	// update again
	oldmp := mp
	mp.MaxPackSize++
	require.NoError(t, mgr3.SetParameters(ctx, mp, bc2, rf2))

	// mgr still sees old mp
	require.Equal(t, oldmp, mustGetMutableParameters(t, mgr))

	// advance time, the now update is now visible
	ta.Advance(cacheDuration)
	require.Equal(t, mp, mustGetMutableParameters(t, mgr))
}

func TestInitialize(t *testing.T) {
	ctx := testlogging.Context(t)

	st := blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, nil)
	fst := blobtesting.NewFaultyStorage(st)

	// error fetching first blob - kopia.repository
	fst.AddFault(blobtesting.MethodGetBlob).ErrorInstead(errSomeError)
	require.ErrorIs(t,
		format.Initialize(ctx, fst, &format.KopiaRepositoryJSON{}, rc, format.BlobStorageConfiguration{}, "some-password"),
		errSomeError)

	// error fetching second blob - kopia.blobcfg
	fst.AddFault(blobtesting.MethodGetBlob)
	fst.AddFault(blobtesting.MethodGetBlob).ErrorInstead(errSomeError)
	require.ErrorIs(t,
		format.Initialize(ctx, fst, &format.KopiaRepositoryJSON{}, rc, format.BlobStorageConfiguration{}, "some-password"),
		errSomeError)

	// success
	require.NoError(t, format.Initialize(ctx, fst, &format.KopiaRepositoryJSON{}, rc, format.BlobStorageConfiguration{}, "some-password"))

	// already initialized
	require.ErrorIs(t,
		format.Initialize(ctx, fst, &format.KopiaRepositoryJSON{}, rc, format.BlobStorageConfiguration{}, "some-password"),
		format.ErrAlreadyInitialized)
}

func TestInitializeWithRetention(t *testing.T) {
	ctx := testlogging.Context(t)

	mode := blob.Governance
	period := time.Hour * 48

	ta := faketime.NewClockTimeWithOffset(0)
	nowFunc := ta.NowFunc()
	earliestExpiry := nowFunc().Add(period)

	st := blobtesting.NewVersionedMapStorage(nowFunc)
	blobCache := format.NewMemoryBlobCache(nowFunc)

	// success
	require.NoError(t, format.Initialize(
		ctx,
		st,
		&format.KopiaRepositoryJSON{},
		rc,
		format.BlobStorageConfiguration{
			RetentionMode:   mode,
			RetentionPeriod: period,
		},
		"some-password",
	))

	mgr, err := format.NewManagerWithCache(ctx, st, cacheDuration, "some-password", nowFunc, blobCache)
	require.NoError(t, err, "getting format manager")

	// New retention parameters should be available from the format manager.
	blobCfg := mustGetBlobStorageConfiguration(t, mgr)
	assert.Equal(t, mode, blobCfg.RetentionMode)
	assert.Equal(t, period, blobCfg.RetentionPeriod)

	// Get the retention configuration that was added to the blob. Allow up to a
	// minute difference between the expected and returned values since that
	// should be large enough to avoid test flakes.
	gotMode, expiry, err := st.GetRetention(ctx, format.KopiaRepositoryBlobID)
	require.NoError(t, err, "getting repo blob retention info")

	assert.Equal(t, mode, gotMode)
	assert.WithinDuration(t, earliestExpiry, expiry, time.Minute)

	gotMode, expiry, err = st.GetRetention(ctx, format.KopiaBlobCfgBlobID)
	require.NoError(t, err, "getting storage blob config retention info")

	assert.Equal(t, mode, gotMode)
	assert.WithinDuration(t, earliestExpiry, expiry, time.Minute)
}

func TestUpdateRetention(t *testing.T) {
	ctx := testlogging.Context(t)

	mode := blob.Governance
	period := time.Hour * 48

	ta := faketime.NewClockTimeWithOffset(0)
	nowFunc := ta.NowFunc()
	earliestExpiry := nowFunc().Add(period)

	st := blobtesting.NewVersionedMapStorage(nowFunc)
	blobCache := format.NewMemoryBlobCache(nowFunc)

	// success
	require.NoError(t, format.Initialize(ctx, st, &format.KopiaRepositoryJSON{}, rc, format.BlobStorageConfiguration{}, "some-password"))

	mgr, err := format.NewManagerWithCache(ctx, st, cacheDuration, "some-password", nowFunc, blobCache)
	require.NoError(t, err, "getting format manager")

	mp := mustGetMutableParameters(t, mgr)
	rf := mustGetRequiredFeatures(t, mgr)

	err = mgr.SetParameters(
		ctx,
		mp,
		format.BlobStorageConfiguration{
			RetentionMode:   mode,
			RetentionPeriod: period,
		},
		rf,
	)
	require.NoError(t, err, "setting repo parameters")

	// New retention parameters should be available from the format manager.
	blobCfg := mustGetBlobStorageConfiguration(t, mgr)
	assert.Equal(t, mode, blobCfg.RetentionMode)
	assert.Equal(t, period, blobCfg.RetentionPeriod)

	// Get the retention configuration that was added to the blob. Allow up to a
	// minute difference between the expected and returned values since that
	// should be large enough to avoid test flakes.
	gotMode, expiry, err := st.GetRetention(ctx, format.KopiaRepositoryBlobID)
	require.NoError(t, err, "getting repo blob retention info")

	assert.Equal(t, mode, gotMode)
	assert.WithinDuration(t, earliestExpiry, expiry, time.Minute)

	gotMode, expiry, err = st.GetRetention(ctx, format.KopiaBlobCfgBlobID)
	require.NoError(t, err, "getting storage blob config retention info")

	assert.Equal(t, mode, gotMode)
	assert.WithinDuration(t, earliestExpiry, expiry, time.Minute)
}

func TestUpdateRetentionNegativeValue(t *testing.T) {
	ctx := testlogging.Context(t)

	startTime := time.Date(2020, 1, 1, 12, 0, 0, 0, time.UTC)
	ta := faketime.NewTimeAdvance(startTime)
	nowFunc := ta.NowFunc()

	st := blobtesting.NewVersionedMapStorage(nowFunc)
	blobCache := format.NewMemoryBlobCache(nowFunc)
	mode := blob.Governance
	period := -time.Hour * 48

	// success
	require.NoError(t, format.Initialize(ctx, st, &format.KopiaRepositoryJSON{}, rc, format.BlobStorageConfiguration{}, "some-password"))

	mgr, err := format.NewManagerWithCache(ctx, st, cacheDuration, "some-password", nowFunc, blobCache)
	require.NoError(t, err, "getting format manager")

	mp := mustGetMutableParameters(t, mgr)
	rf := mustGetRequiredFeatures(t, mgr)

	err = mgr.SetParameters(
		ctx,
		mp,
		format.BlobStorageConfiguration{
			RetentionMode:   mode,
			RetentionPeriod: period,
		},
		rf,
	)
	require.Error(t, err, "setting repo parameters")

	// Old retention parameters should be available from the format manager.
	blobCfg := mustGetBlobStorageConfiguration(t, mgr)
	assert.Empty(t, blobCfg.RetentionMode)
	assert.Zero(t, blobCfg.RetentionPeriod)

	// Retention wasn't set so everything should be zero/empty.
	gotMode, expiry, err := st.GetRetention(ctx, format.KopiaRepositoryBlobID)
	require.NoError(t, err, "getting repo blob retention info")

	assert.Empty(t, gotMode)
	assert.Zero(t, expiry)

	gotMode, expiry, err = st.GetRetention(ctx, format.KopiaBlobCfgBlobID)
	require.NoError(t, err, "getting storage blob config retention info")

	assert.Empty(t, gotMode)
	assert.Zero(t, expiry)
}

func TestChangePassword(t *testing.T) {
	ctx := testlogging.Context(t)

	startTime := time.Date(2020, 1, 1, 12, 0, 0, 0, time.UTC)
	ta := faketime.NewTimeAdvance(startTime)
	nowFunc := ta.NowFunc()
	blobCache := format.NewMemoryBlobCache(nowFunc)

	cf2 := cf
	cf2.Version = format.FormatVersion3
	cf2.EnablePasswordChange = true

	rc = &format.RepositoryConfig{
		ContentFormat: cf2,
		UpgradeLock:   uli,
	}

	st := blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, nil)
	fst := blobtesting.NewFaultyStorage(st)
	require.NoError(t, format.Initialize(ctx, fst, &format.KopiaRepositoryJSON{}, rc, format.BlobStorageConfiguration{}, "some-password"))

	mgr, err := format.NewManagerWithCache(ctx, fst, cacheDuration, "some-password", nowFunc, blobCache)
	require.NoError(t, err)

	mgr2, err := format.NewManagerWithCache(ctx, fst, cacheDuration, "some-password", nowFunc, blobCache)
	require.NoError(t, err)

	require.NoError(t, mgr2.ChangePassword(ctx, "new-password"))

	// immediately after changing the password, both managers can still read the repo
	mustGetMutableParameters(t, mgr)
	mustGetMutableParameters(t, mgr2)

	ta.Advance(cacheDuration)

	require.ErrorIs(t, expectMutableParametersError(t, mgr), format.ErrInvalidPassword)
	mustGetMutableParameters(t, mgr2)

	_, err = format.NewManagerWithCache(ctx, fst, cacheDuration, "some-password", nowFunc, blobCache)
	require.ErrorIs(t, err, format.ErrInvalidPassword)
}

func TestFormatManagerValidDuration(t *testing.T) {
	cases := map[time.Duration]time.Duration{
		-1:               15 * time.Minute,
		time.Second:      time.Second,
		30 * time.Minute: 15 * time.Minute,
		10 * time.Minute: 10 * time.Minute,
	}

	for requestedCacheDuration, actualCacheDuration := range cases {
		ctx := testlogging.Context(t)

		startTime := time.Date(2020, 1, 1, 12, 0, 0, 0, time.UTC)
		ta := faketime.NewTimeAdvance(startTime)
		nowFunc := ta.NowFunc()
		blobCache := format.NewMemoryBlobCache(nowFunc)

		st := blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, nil)
		fst := blobtesting.NewFaultyStorage(st)
		require.NoError(t, format.Initialize(ctx, fst, &format.KopiaRepositoryJSON{}, rc, format.BlobStorageConfiguration{}, "some-password"))

		if requestedCacheDuration < 0 {
			// plant a malformed cache entry to ensure it's not being used
			blobCache.Put(ctx, "kopia.repository", []byte("malformed"))
		}

		mgr, err := format.NewManagerWithCache(ctx, fst, requestedCacheDuration, "some-password", nowFunc, blobCache)
		require.NoError(t, err)

		require.Equal(t, actualCacheDuration, mgr.ValidCacheDuration())
	}
}

func mustGetMutableParameters(t *testing.T, mgr *format.Manager) format.MutableParameters {
	t.Helper()

	mp, err := mgr.GetMutableParameters(testlogging.Context(t))
	require.NoError(t, err)

	return mp
}

func mustGetUpgradeLockIntent(t *testing.T, mgr *format.Manager) *format.UpgradeLockIntent {
	t.Helper()

	uli, err := mgr.GetUpgradeLockIntent(testlogging.Context(t))
	require.NoError(t, err)

	return uli
}

func mustGetRepositoryFormatBytes(t *testing.T, mgr *format.Manager) []byte {
	t.Helper()

	b, err := mgr.RepositoryFormatBytes(testlogging.Context(t))
	require.NoError(t, err)

	return b
}

func mustGetRequiredFeatures(t *testing.T, mgr *format.Manager) []feature.Required {
	t.Helper()

	rf, err := mgr.RequiredFeatures(testlogging.Context(t))
	require.NoError(t, err)

	return rf
}

func mustGetBlobStorageConfiguration(t *testing.T, mgr *format.Manager) format.BlobStorageConfiguration {
	t.Helper()

	cfg, err := mgr.BlobCfgBlob(testlogging.Context(t))
	require.NoError(t, err)

	return cfg
}

func expectMutableParametersError(t *testing.T, mgr *format.Manager) error {
	t.Helper()

	_, err := mgr.GetMutableParameters(testlogging.Context(t))
	require.Error(t, err)

	return err
}

func mustGetBytes(t *testing.T, st blob.Storage, blobID blob.ID) []byte {
	t.Helper()

	var tmp gather.WriteBuffer
	defer tmp.Close()

	require.NoError(t, st.GetBlob(testlogging.Context(t), blobID, 0, -1, &tmp))

	return tmp.ToByteSlice()
}
