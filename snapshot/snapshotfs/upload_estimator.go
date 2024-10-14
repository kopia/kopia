package snapshotfs

import (
	"context"
	"sync"

	"github.com/kopia/kopia/fs"
	vsi "github.com/kopia/kopia/internal/volumesizeinfo"
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/snapshot/policy"

	"github.com/pkg/errors"
)

// EstimationDoneFn represents the signature of the callback function which will be invoked when an estimation is done.
type EstimationDoneFn func(int64, int64)

// EstimationStarter defines an interface that is used to start an estimation of the size of data to be uploaded.
type EstimationStarter interface {
	StartEstimation(ctx context.Context, cb EstimationDoneFn)
}

// EstimationController defines an interface which has to be used to cancel or wait for running estimation.
type EstimationController interface {
	Cancel()
	Wait()
}

// Estimator interface combines EstimationStarter and EstimationController interfaces.
// It represents the objects that can both initiate and control an estimation process.
type Estimator interface {
	EstimationStarter
	EstimationController
}

// NoOpEstimationController is a default implementation of the EstimationController interface.
// It's used in cases where no estimation operation is running and hence, its methods are no-ops.
type NoOpEstimationController struct{}

// Cancel is a no-op function to satisfy the EstimationController interface.
func (c *NoOpEstimationController) Cancel() {}

// Wait is a no-op function to satisfy the EstimationController interface.
func (c *NoOpEstimationController) Wait() {}

// noOpEstimationCtrl is an instance of NoOpEstimationController.
// It's a singleton instance used to handle operations when no estimation is running.
var noOpEstimationCtrl EstimationController = &NoOpEstimationController{} //nolint:gochecknoglobals

type estimator struct {
	estimationParameters EstimationParameters
	logger               logging.Logger
	entry                fs.Directory
	policyTree           *policy.Tree

	scanWG              sync.WaitGroup
	cancelCtx           context.CancelFunc
	getVolumeSizeInfoFn func(string) (vsi.VolumeSizeInfo, error)
}

// EstimatorOption is an option which could be used to customize estimator behavior.
type EstimatorOption func(Estimator)

// VolumeSizeInfoFn represents a function type which is used to retrieve volume size information.
type VolumeSizeInfoFn func(string) (vsi.VolumeSizeInfo, error)

// WithVolumeSizeInfoFn returns EstimatorOption which allows to pass custom GetVolumeSizeInfo implementation.
func WithVolumeSizeInfoFn(fn VolumeSizeInfoFn) EstimatorOption {
	return func(e Estimator) {
		roughEst, _ := e.(*estimator)
		roughEst.getVolumeSizeInfoFn = fn
	}
}

// NewEstimator returns instance of estimator.
func NewEstimator(
	entry fs.Directory,
	policyTree *policy.Tree,
	estimationParams EstimationParameters,
	logger logging.Logger,
	options ...EstimatorOption,
) Estimator {
	est := &estimator{
		estimationParameters: estimationParams,
		logger:               logger,
		entry:                entry,
		policyTree:           policyTree,
		getVolumeSizeInfoFn:  vsi.GetVolumeSizeInfo,
	}

	for _, option := range options {
		option(est)
	}

	return est
}

// StartEstimation starts estimation of data to be uploaded.
// Terminates early as soon as the provided context is canceled.
func (e *estimator) StartEstimation(ctx context.Context, cb EstimationDoneFn) {
	if e.cancelCtx != nil {
		return // Estimation already started, do nothing
	}

	scanCtx, cancelScan := context.WithCancel(ctx)

	e.cancelCtx = cancelScan
	e.scanWG.Add(1)

	go func() {
		defer e.scanWG.Done()

		logger := estimateLog(ctx)

		var filesCount, totalFileSize int64

		var err error

		et := e.estimationParameters.Type
		useClassic := false

		if et == EstimationTypeAdaptive || et == EstimationTypeRough {
			filesCount, totalFileSize, err = e.doRoughEstimation()
			if err != nil {
				logger.Debugf("Unable to do rough estimation, fallback to classic one. %v", err)

				useClassic = true
			}

			if et == EstimationTypeAdaptive && filesCount < e.estimationParameters.AdaptiveThreshold {
				logger.Debugf("Small number of files (%d) on volume, falling back to classic estimation.", filesCount)

				useClassic = true
			}
		}

		if useClassic || et == EstimationTypeClassic {
			filesCount, totalFileSize, err = e.doClassicEstimation(scanCtx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					logger.Debugf("Estimation has been interrupted")
				} else {
					logger.Debugf("Estimation failed: %v", err)
					logger.Warn("Unable to estimate")
				}
			}
		}

		cb(filesCount, totalFileSize)
	}()
}

func (e *estimator) Wait() {
	e.scanWG.Wait()
	e.cancelCtx = nil
}

func (e *estimator) Cancel() {
	if e.cancelCtx != nil {
		e.cancelCtx()
		e.cancelCtx = nil
	}
}

func (e *estimator) doRoughEstimation() (filesCount, totalFileSize int64, err error) {
	volumeSizeInfo, err := e.getVolumeSizeInfoFn(e.entry.LocalFilesystemPath())
	if err != nil {
		return 0, 0, errors.Wrap(err, "Unable to get volume size info")
	}

	return int64(volumeSizeInfo.FilesCount), int64(volumeSizeInfo.UsedSize), nil //nolint:gosec
}

func (e *estimator) doClassicEstimation(ctx context.Context) (filesCount, totalFileSize int64, err error) {
	var res scanResults

	err = Estimate(ctx, e.entry, e.policyTree, &res, 1)
	if err != nil {
		return 0, 0, errors.Wrap(err, "Unable to scan directory")
	}

	return int64(res.numFiles), res.totalFileSize, nil
}
