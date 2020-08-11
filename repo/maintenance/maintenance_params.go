package maintenance

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/manifest"
)

var manifestLabels = map[string]string{
	"type": "maintenance",
}

// Params is a JSON-serialized maintenance configuration stored in a repository.
type Params struct {
	Owner string `json:"owner"`

	QuickCycle CycleParams `json:"quick"`
	FullCycle  CycleParams `json:"full"`

	SnapshotGC SnapshotGCParams `json:"snapshotGC"`
}

// SnapshotGCParams contains parameters for Snapshot Garbage Collection
// NOTE: Due to layering, the implementation of Snapshot GC is outside of repository package
// but for simplicity we store it here.
type SnapshotGCParams struct {
	MinContentAge time.Duration `json:"minAge"`
}

// DefaultParams represents default values of maintenance parameters.
func DefaultParams() Params {
	return Params{
		FullCycle: CycleParams{
			// TODO: enable this when ready for public consumption
			// Enabled:  true,
			Interval: 7 * 24 * time.Hour,
		},
		QuickCycle: CycleParams{
			Enabled:  true,
			Interval: 1 * time.Hour,
		},
		SnapshotGC: SnapshotGCParams{
			MinContentAge: 24 * time.Hour, //nolint:gomnd
		},
	}
}

// CycleParams specifies parameters for a maintenance cycle (quick or full).
type CycleParams struct {
	Enabled  bool          `json:"enabled"`
	Interval time.Duration `json:"interval"`
}

// HasParams determines whether repository-wide maintenance parameters have been set.
func HasParams(ctx context.Context, rep MaintainableRepository) (bool, error) {
	md, err := manifestIDs(ctx, rep)
	if err != nil {
		return false, err
	}

	return len(md) > 0, nil
}

// GetParams returns repository-wide maintenance parameters.
func GetParams(ctx context.Context, rep MaintainableRepository) (*Params, error) {
	md, err := manifestIDs(ctx, rep)
	if err != nil {
		return nil, err
	}

	if len(md) == 0 {
		// not found, return empty params
		p := DefaultParams()
		return &p, nil
	}

	// arbitrality pick first pick ID to return in case there's more than one
	// this is possible when two repository clients independently create manifests at approximately the same time
	// so it should not really matter which one we pick.
	// see https://github.com/kopia/kopia/issues/391
	manifestID := md[0].ID

	p := &Params{}
	if _, err := rep.GetManifest(ctx, manifestID, p); err != nil {
		return nil, errors.Wrap(err, "error loading manifest")
	}

	return p, nil
}

// SetParams sets the maintenance parameters.
func SetParams(ctx context.Context, rep MaintainableRepository, par *Params) error {
	md, err := manifestIDs(ctx, rep)
	if err != nil {
		return err
	}

	if _, err := rep.PutManifest(ctx, manifestLabels, par); err != nil {
		return errors.Wrap(err, "put manifest")
	}

	for _, m := range md {
		if err := rep.DeleteManifest(ctx, m.ID); err != nil {
			return errors.Wrap(err, "delete manifest")
		}
	}

	return nil
}

func manifestIDs(ctx context.Context, rep MaintainableRepository) ([]*manifest.EntryMetadata, error) {
	md, err := rep.FindManifests(ctx, manifestLabels)
	if err != nil {
		return nil, errors.Wrap(err, "error looking for maintenance manifest")
	}

	return md, err
}
