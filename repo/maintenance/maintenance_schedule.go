package maintenance

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
)

const (
	maintenanceScheduleKeySize = 32
	maintenanceScheduleBlobID  = "kopia.maintenance"
)

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

	Runs map[string][]RunInfo `json:"runs"`
}

// ReportRun adds the provided run information to the history and discards oldest entried.
func (s *Schedule) ReportRun(runType string, info RunInfo) {
	if s.Runs == nil {
		s.Runs = map[string][]RunInfo{}
	}

	// insert as first item
	history := append([]RunInfo{info}, s.Runs[runType]...)

	if len(history) > maxRetainedRunInfoPerRunType {
		history = history[0:maxRetainedRunInfoPerRunType]
	}

	s.Runs[runType] = history
}

func getAES256GCM(rep MaintainableRepository) (cipher.AEAD, error) {
	c, err := aes.NewCipher(rep.DeriveKey(maintenanceScheduleKeyPurpose, maintenanceScheduleKeySize))
	if err != nil {
		return nil, errors.Wrap(err, "unable to create AES-256 cipher")
	}

	return cipher.NewGCM(c)
}

// GetSchedule gets the scheduled maintenance times.
func GetSchedule(ctx context.Context, rep MaintainableRepository) (*Schedule, error) {
	// read
	v, err := rep.BlobStorage().GetBlob(ctx, maintenanceScheduleBlobID, 0, -1)
	if err == blob.ErrBlobNotFound {
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

	if len(v) < c.NonceSize() {
		return nil, errors.Wrap(err, "invalid schedule blob")
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
func SetSchedule(ctx context.Context, rep MaintainableRepository, s *Schedule) error {
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

	return rep.BlobStorage().PutBlob(ctx, maintenanceScheduleBlobID, gather.FromSlice(ciphertext))
}

// ReportRun reports timing of a maintenance run and persists it in repository.
func ReportRun(ctx context.Context, rep MaintainableRepository, runType string, run func() error) error {
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

	s, err := GetSchedule(ctx, rep)
	if err != nil {
		log(ctx).Warningf("unable to get schedule")
	}

	s.ReportRun(runType, ri)

	if err := SetSchedule(ctx, rep, s); err != nil {
		log(ctx).Warningf("unable to report run: %v", err)
	}

	return runErr
}
