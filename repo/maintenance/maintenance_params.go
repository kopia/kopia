package maintenance

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
)

//nolint:gochecknoglobals
var manifestLabels = map[string]string{
	"type": "maintenance",
}

// Params is a JSON-serialized maintenance configuration stored in a repository.
type Params struct {
	Owner string `json:"owner"`

	QuickCycle CycleParams `json:"quick"`
	FullCycle  CycleParams `json:"full"`

	LogRetention LogRetentionOptions `json:"logRetention"`

	ExtendObjectLocks bool `json:"extendObjectLocks"`

	ListParallelism int `json:"listParallelism"`
}

// isOwnedByByThisUser determines whether current user is the maintenance owner.
func (p *Params) isOwnedByByThisUser(rep repo.Repository) bool {
	return p.Owner == rep.ClientOptions().UsernameAtHost()
}

// DefaultParams represents default values of maintenance parameters.
func DefaultParams() Params {
	return Params{
		FullCycle: CycleParams{
			Enabled:  true,
			Interval: 24 * time.Hour, //nolint:mnd
		},
		QuickCycle: CycleParams{
			Enabled:  true,
			Interval: 1 * time.Hour,
		},
		LogRetention: defaultLogRetention(),
		// Don't attempt to extend object locks by default. This option may not be
		// supported by all storage providers or blob implementations (currently
		// supported by S3 backend) and may cause data to be kept longer than
		// desired if the retention period is relatively long.
		ExtendObjectLocks: false,
	}
}

// CycleParams specifies parameters for a maintenance cycle (quick or full).
type CycleParams struct {
	Enabled  bool          `json:"enabled"`
	Interval time.Duration `json:"interval"`
}

// HasParams determines whether repository-wide maintenance parameters have been set.
func HasParams(ctx context.Context, rep repo.Repository) (bool, error) {
	md, err := manifestIDs(ctx, rep)
	if err != nil {
		return false, err
	}

	return len(md) > 0, nil
}

// IsOwnedByThisUser determines whether current user is the maintenance owner.
func IsOwnedByThisUser(ctx context.Context, rep repo.Repository) (bool, error) {
	p, err := GetParams(ctx, rep)
	if err != nil {
		return false, errors.Wrap(err, "error getting maintenance params")
	}

	return p.isOwnedByByThisUser(rep), nil
}

// GetParams returns repository-wide maintenance parameters.
func GetParams(ctx context.Context, rep repo.Repository) (*Params, error) {
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
	manifestID := manifest.PickLatestID(md)

	p := &Params{}
	if _, err := rep.GetManifest(ctx, manifestID, p); err != nil {
		return nil, errors.Wrap(err, "error loading manifest")
	}

	return p, nil
}

// SetParams sets the maintenance parameters.
func SetParams(ctx context.Context, rep repo.RepositoryWriter, par *Params) error {
	if _, err := rep.ReplaceManifests(ctx, manifestLabels, par); err != nil {
		return errors.Wrap(err, "put manifest")
	}

	return nil
}

func manifestIDs(ctx context.Context, rep repo.Repository) ([]*manifest.EntryMetadata, error) {
	md, err := rep.FindManifests(ctx, manifestLabels)
	if err != nil {
		return nil, errors.Wrap(err, "error looking for maintenance manifest")
	}

	return md, nil
}
