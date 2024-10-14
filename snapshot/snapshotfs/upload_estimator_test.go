package snapshotfs_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kopia/kopia/internal/mockfs"
	vsi "github.com/kopia/kopia/internal/volumesizeinfo"
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/snapshot/policy"
	"github.com/kopia/kopia/snapshot/snapshotfs"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var errSimulated = errors.New("simulated error")

type mockLogger struct{}

func (w *mockLogger) Write(p []byte) (int, error) {
	return len(p), nil
}

func (w *mockLogger) Sync() error {
	return nil
}

func getMockLogger() logging.Logger {
	ml := &mockLogger{}
	return zap.New(
		zapcore.NewCore(
			zapcore.NewConsoleEncoder(zapcore.EncoderConfig{
				// Keys can be anything except the empty string.
				TimeKey:        zapcore.OmitKey,
				LevelKey:       zapcore.OmitKey,
				NameKey:        zapcore.OmitKey,
				CallerKey:      zapcore.OmitKey,
				FunctionKey:    zapcore.OmitKey,
				MessageKey:     "M",
				StacktraceKey:  "S",
				LineEnding:     zapcore.DefaultLineEnding,
				EncodeLevel:    zapcore.CapitalLevelEncoder,
				EncodeTime:     zapcore.ISO8601TimeEncoder,
				EncodeDuration: zapcore.StringDurationEncoder,
				EncodeCaller:   zapcore.ShortCallerEncoder,
			}),
			ml,
			zapcore.DebugLevel,
		),
	).Sugar()
}

// withFailedVolumeSizeInfo returns EstimatorOption which ensures that GetVolumeSizeInfo will fail with provided error.
// Purposed for tests.
func withFailedVolumeSizeInfo(err error) snapshotfs.EstimatorOption {
	return snapshotfs.WithVolumeSizeInfoFn(func(_ string) (vsi.VolumeSizeInfo, error) {
		return vsi.VolumeSizeInfo{}, err
	})
}

// withVolumeSizeInfo returns EstimatorOption which provides fake volume size.
func withVolumeSizeInfo(filesCount, usedFileSize, totalFileSize uint64) snapshotfs.EstimatorOption {
	return snapshotfs.WithVolumeSizeInfoFn(func(_ string) (vsi.VolumeSizeInfo, error) {
		return vsi.VolumeSizeInfo{
			TotalSize:  totalFileSize,
			UsedSize:   usedFileSize,
			FilesCount: filesCount,
		}, nil
	})
}

func expectSuccessfulEstimation(
	ctx context.Context,
	t *testing.T,
	estimator snapshotfs.Estimator,
	expectedNumberOfFiles,
	expectedDataSize int64,
) {
	t.Helper()
	var filesCount, totalFileSize int64

	done := make(chan struct{})
	go func() {
		defer close(done)
		estimator.StartEstimation(ctx, func(fc, ts int64) {
			filesCount = fc
			totalFileSize = ts
		})

		estimator.Wait()
	}()

	select {
	case <-done:
		require.Equal(t, expectedNumberOfFiles, filesCount)
		require.Equal(t, expectedDataSize, totalFileSize)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for estimation")
	}
}

func TestUploadEstimator(t *testing.T) {
	dir1 := mockfs.NewDirectory()

	file1Content := []byte{1, 2, 3}
	file2Content := []byte{4, 5, 6, 7}
	file3Content := []byte{8, 9, 10, 11, 12}

	dir1.AddFile("file1", file1Content, 0o644)
	dir1.AddFile("file2", file2Content, 0o644)
	dir1.AddFile("file3", file3Content, 0o644)

	expectedNumberOfFiles := int64(3)
	expectedDataSize := int64(len(file1Content) + len(file2Content) + len(file3Content))

	t.Run("Classic estimation", func(t *testing.T) {
		logger := getMockLogger()

		policyTree := policy.BuildTree(nil, policy.DefaultPolicy)
		estimator := snapshotfs.NewEstimator(dir1, policyTree, snapshotfs.EstimationParameters{Type: snapshotfs.EstimationTypeClassic}, logger)

		estimationCtx := context.Background()
		expectSuccessfulEstimation(estimationCtx, t, estimator, expectedNumberOfFiles, expectedDataSize)
	})
	t.Run("Rough estimation", func(t *testing.T) {
		logger := getMockLogger()

		expectedNumberOfFiles := int64(1000)
		expectedDataSize := int64(2000)

		policyTree := policy.BuildTree(nil, policy.DefaultPolicy)
		estimator := snapshotfs.NewEstimator(
			dir1, policyTree, snapshotfs.EstimationParameters{Type: snapshotfs.EstimationTypeRough}, logger,
			withVolumeSizeInfo(uint64(expectedNumberOfFiles), uint64(expectedDataSize), 3000))

		estimationCtx := context.Background()

		expectSuccessfulEstimation(estimationCtx, t, estimator, expectedNumberOfFiles, expectedDataSize)
	})
	t.Run("Rough estimation - GetVolumeSizeInfo failed", func(t *testing.T) {
		logger := getMockLogger()

		policyTree := policy.BuildTree(nil, policy.DefaultPolicy)
		estimator := snapshotfs.NewEstimator(
			dir1, policyTree, snapshotfs.EstimationParameters{Type: snapshotfs.EstimationTypeRough}, logger,
			withFailedVolumeSizeInfo(errSimulated))

		estimationCtx := context.Background()

		// We expect that estimation will succeed even when GetVolumeSizeInfo will fail
		// fallback to classical estimation should handle this case
		expectSuccessfulEstimation(estimationCtx, t, estimator, expectedNumberOfFiles, expectedDataSize)
	})
	t.Run("Adaptive estimation - rough estimation path", func(t *testing.T) {
		logger := getMockLogger()

		expectedNumberOfFiles := int64(1000)
		expectedDataSize := int64(2000)

		policyTree := policy.BuildTree(nil, policy.DefaultPolicy)
		estimator := snapshotfs.NewEstimator(
			dir1, policyTree,
			snapshotfs.EstimationParameters{Type: snapshotfs.EstimationTypeAdaptive, AdaptiveThreshold: 100}, logger,
			withVolumeSizeInfo(uint64(expectedNumberOfFiles), uint64(expectedDataSize), 3000))

		estimationCtx := context.Background()

		expectSuccessfulEstimation(estimationCtx, t, estimator, expectedNumberOfFiles, expectedDataSize)
	})
	t.Run("Adaptive estimation - classic estimation path", func(t *testing.T) {
		logger := getMockLogger()

		policyTree := policy.BuildTree(nil, policy.DefaultPolicy)
		estimator := snapshotfs.NewEstimator(
			dir1, policyTree,
			snapshotfs.EstimationParameters{Type: snapshotfs.EstimationTypeAdaptive, AdaptiveThreshold: 10000}, logger,
			withVolumeSizeInfo(uint64(1000), uint64(2000), 3000))

		estimationCtx := context.Background()

		expectSuccessfulEstimation(estimationCtx, t, estimator, expectedNumberOfFiles, expectedDataSize)
	})
	t.Run("Adaptive estimation - getVolumeSizeInfo failed", func(t *testing.T) {
		logger := getMockLogger()

		policyTree := policy.BuildTree(nil, policy.DefaultPolicy)
		estimator := snapshotfs.NewEstimator(
			dir1, policyTree, snapshotfs.EstimationParameters{Type: snapshotfs.EstimationTypeAdaptive, AdaptiveThreshold: 1}, logger,
			withFailedVolumeSizeInfo(errSimulated))

		estimationCtx := context.Background()

		// We expect that estimation will succeed even when getVolumeSizeInfo will fail
		// fallback to classical estimation should handle this case
		expectSuccessfulEstimation(estimationCtx, t, estimator, expectedNumberOfFiles, expectedDataSize)
	})

	t.Run("Classic estimation stops on context cancel", func(t *testing.T) {
		testCtx, cancel := context.WithCancel(context.Background())
		dir2 := mockfs.NewDirectory()

		dir2.AddFile("file1", file1Content, 0o644)
		dir2.AddFile("file2", file2Content, 0o644)
		dir2.AddFile("file3", file3Content, 0o644)
		dir2.AddDir("d1", 0o777)

		dir2.Subdir("d1").OnReaddir(func() {
			cancel()
		})

		logger := getMockLogger()
		policyTree := policy.BuildTree(nil, policy.DefaultPolicy)
		estimator := snapshotfs.NewEstimator(dir2, policyTree, snapshotfs.EstimationParameters{Type: snapshotfs.EstimationTypeRough}, logger)

		// In case of canceled context, we should get zeroes instead of estimated numbers
		expectSuccessfulEstimation(testCtx, t, estimator, 0, 0)
	})
	t.Run("EstimationStarter stops on request", func(t *testing.T) {
		dir2 := mockfs.NewDirectory()

		dir2.AddFile("file1", file1Content, 0o644)
		dir2.AddFile("file2", file2Content, 0o644)
		dir2.AddFile("file3", file3Content, 0o644)
		dir2.AddDir("d1", 0o777)

		logger := getMockLogger()
		policyTree := policy.BuildTree(nil, policy.DefaultPolicy)
		estimator := snapshotfs.NewEstimator(dir2, policyTree, snapshotfs.EstimationParameters{Type: snapshotfs.EstimationTypeClassic}, logger)

		dir2.Subdir("d1").OnReaddir(func() {
			estimator.Cancel()
		})

		// In case interrupted estimation, we should get zeroes instead of estimated numbers
		expectSuccessfulEstimation(context.Background(), t, estimator, 0, 0)
	})
	t.Run("Classic estimation respects ignores from policy tree", func(t *testing.T) {
		policyTree := policy.BuildTree(map[string]*policy.Policy{
			".": {
				FilesPolicy: policy.FilesPolicy{
					IgnoreRules: []string{"file1"},
				},
			},
		}, policy.DefaultPolicy)

		logger := getMockLogger()
		estimator := snapshotfs.NewEstimator(dir1, policyTree, snapshotfs.EstimationParameters{Type: snapshotfs.EstimationTypeClassic}, logger)

		expectSuccessfulEstimation(context.Background(), t, estimator, expectedNumberOfFiles-1, expectedDataSize-int64(len(file1Content)))
	})
}
