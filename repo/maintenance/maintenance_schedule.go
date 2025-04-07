package maintenance

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
)

const (
	maintenanceScheduleKeySize = 32
	maintenanceScheduleBlobID  = "kopia.maintenance"
)

//nolint:gochecknoglobals
var (
	maintenanceScheduleKeyPurpose    = []byte("maintenance schedule")
	maintenanceScheduleAEADExtraData = []byte("maintenance")
)

// maxRetainedRunInfoPerRunType the maximum number of retained RunInfo entries per run type.
const maxRetainedRunInfoPerRunType = 5

// RunInfo represents information about a single run of a maintenance task.
type RunInfo struct {
	Start   time.Time `json:"start"`
	End     time.Time `json:"end"`
	Success bool      `json:"success,omitempty"`
	Error   string    `json:"error,omitempty"`
}

// Schedule keeps track of scheduled maintenance times.
type Schedule struct {
	NextFullMaintenanceTime  time.Time `json:"nextFullMaintenance"`
	NextQuickMaintenanceTime time.Time `json:"nextQuickMaintenance"`

	Runs map[TaskType][]RunInfo `json:"runs"`
}

// ReportRun adds the provided run information to the history and discards oldest entried.
func (s *Schedule) ReportRun(taskType TaskType, info RunInfo) {
	if s.Runs == nil {
		s.Runs = map[TaskType][]RunInfo{}
	}

	// insert as first item
	history := append([]RunInfo{info}, s.Runs[taskType]...)

	if len(history) > maxRetainedRunInfoPerRunType {
		history = history[0:maxRetainedRunInfoPerRunType]
	}

	s.Runs[taskType] = history
}

func getAES256GCM(rep repo.DirectRepository) (cipher.AEAD, error) {
	c, err := aes.NewCipher(rep.DeriveKey(maintenanceScheduleKeyPurpose, maintenanceScheduleKeySize))
	if err != nil {
		return nil, errors.Wrap(err, "unable to create AES-256 cipher")
	}

	//nolint:wrapcheck
	return cipher.NewGCM(c)
}

// TimeToAttemptNextMaintenance returns the time when we should attempt next maintenance.
// if the maintenance is not owned by this user, returns time.Time{}.
func TimeToAttemptNextMaintenance(ctx context.Context, rep repo.DirectRepository) (time.Time, error) {
	mp, err := GetParams(ctx, rep)
	if err != nil {
		return time.Time{}, errors.Wrap(err, "unable to get maintenance parameters")
	}

	// if maintenance is not owned by this user, do not run maintenance here.
	if !mp.isOwnedByByThisUser(rep) {
		return time.Time{}, nil
	}

	ms, err := GetSchedule(ctx, rep)
	if err != nil {
		return time.Time{}, errors.Wrap(err, "unable to get maintenance schedule")
	}

	var nextMaintenanceTime time.Time

	if mp.FullCycle.Enabled {
		nextMaintenanceTime = ms.NextFullMaintenanceTime
		if nextMaintenanceTime.IsZero() {
			nextMaintenanceTime = clock.Now()
		}
	}

	if mp.QuickCycle.Enabled {
		if nextMaintenanceTime.IsZero() || ms.NextQuickMaintenanceTime.Before(nextMaintenanceTime) {
			nextMaintenanceTime = ms.NextQuickMaintenanceTime
			if nextMaintenanceTime.IsZero() {
				nextMaintenanceTime = clock.Now()
			}
		}
	}

	return nextMaintenanceTime, nil
}

// GetSchedule gets the scheduled maintenance times.
func GetSchedule(ctx context.Context, rep repo.DirectRepository) (*Schedule, error) {
	var tmp gather.WriteBuffer
	defer tmp.Close()

	// read
	err := rep.BlobReader().GetBlob(ctx, maintenanceScheduleBlobID, 0, -1, &tmp)
	if errors.Is(err, blob.ErrBlobNotFound) {
		return &Schedule{}, nil
	}

	if err != nil {
		return nil, errors.Wrap(err, "error reading schedule blob")
	}

	// decrypt
	c, err := getAES256GCM(rep)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get cipher")
	}

	v := tmp.ToByteSlice()

	if len(v) < c.NonceSize() {
		return nil, errors.New("invalid schedule blob")
	}

	j, err := c.Open(nil, v[0:c.NonceSize()], v[c.NonceSize():], maintenanceScheduleAEADExtraData)
	if err != nil {
		return nil, errors.Wrap(err, "unable to decrypt schedule blob")
	}

	// parse JSON
	s := &Schedule{}
	if err := json.Unmarshal(j, s); err != nil {
		return nil, errors.Wrap(err, "malformed schedule blob")
	}

	return s, nil
}

// SetSchedule updates scheduled maintenance times.
func SetSchedule(ctx context.Context, rep repo.DirectRepositoryWriter, s *Schedule) error {
	// encode JSON
	v, err := json.Marshal(s)
	if err != nil {
		return errors.Wrap(err, "unable to serialize JSON")
	}

	// encrypt with AES-256-GCM and random nonce
	c, err := getAES256GCM(rep)
	if err != nil {
		return errors.Wrap(err, "unable to get cipher")
	}

	// generate random nonce
	nonce := make([]byte, c.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return errors.Wrap(err, "unable to initialize nonce")
	}

	result := append([]byte(nil), nonce...)
	ciphertext := c.Seal(result, nonce, v, maintenanceScheduleAEADExtraData)

	//nolint:wrapcheck
	return rep.BlobStorage().PutBlob(ctx, maintenanceScheduleBlobID, gather.FromSlice(ciphertext), blob.PutOptions{})
}

// ReportRun reports timing of a maintenance run and persists it in repository.
func ReportRun(ctx context.Context, rep repo.DirectRepositoryWriter, taskType TaskType, s *Schedule, run func() error) error {
	if s == nil {
		var err error

		s, err = GetSchedule(ctx, rep)
		if err != nil {
			return errors.Wrap(err, "unable to get maintenance schedule")
		}
	}

	ri := RunInfo{
		Start: rep.Time(),
	}

	runErr := run()

	ri.End = rep.Time()

	if runErr != nil {
		ri.Error = runErr.Error()
	} else {
		ri.Success = true
	}

	s.ReportRun(taskType, ri)

	if err := SetSchedule(ctx, rep, s); err != nil {
		log(ctx).Errorf("unable to report run: %v", err)
	}

	return runErr
}
