//go:build darwin || (linux && amd64)

package engine

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/tests/robustness"
)

const (
	engineStatsStoreKey = "cumulative-engine-stats"
	engineLogsStoreKey  = "engine-logs"
	snapIDIndexStoreKey = "checker-snapID-index"
)

// saveLog saves the engine Log in the metadata store.
func (e *Engine) saveLog(ctx context.Context) error {
	b, err := json.Marshal(e.EngineLog)
	if err != nil {
		return err
	}

	return e.MetaStore.Store(ctx, engineLogsStoreKey, b)
}

// loadLog loads the engine log from the metadata store.
func (e *Engine) loadLog(ctx context.Context) error {
	b, err := e.MetaStore.Load(ctx, engineLogsStoreKey)
	if err != nil {
		if errors.Is(err, robustness.ErrKeyNotFound) {
			// Swallow key-not-found error. May not have historical logs
			return nil
		}

		return err
	}

	err = json.Unmarshal(b, &e.EngineLog)
	if err != nil {
		return err
	}

	e.EngineLog.runOffset = len(e.EngineLog.Log)

	return err
}

// saveStats saves the engine Stats in the metadata store.
func (e *Engine) saveStats(ctx context.Context) error {
	cumulStatRaw, err := json.Marshal(e.CumulativeStats)
	if err != nil {
		return err
	}

	return e.MetaStore.Store(ctx, engineStatsStoreKey, cumulStatRaw)
}

// loadStats loads the engine Stats from the metadata store.
func (e *Engine) loadStats(ctx context.Context) error {
	b, err := e.MetaStore.Load(ctx, engineStatsStoreKey)
	if err != nil {
		if errors.Is(err, robustness.ErrKeyNotFound) {
			// Swallow key-not-found error. We may not have historical
			// stats data. Initialize the action map for the cumulative stats
			e.CumulativeStats.PerActionStats = make(map[ActionKey]*ActionStats)
			e.CumulativeStats.CreationTime = clock.Now()

			return nil
		}

		return err
	}

	return json.Unmarshal(b, &e.CumulativeStats)
}

// saveSnapIDIndex saves the Checker's snapshot ID index in the metadata store.
func (e *Engine) saveSnapIDIndex(ctx context.Context) error {
	snapIDIdxRaw, err := json.Marshal(e.Checker.SnapIDIndex)
	if err != nil {
		return err
	}

	return e.MetaStore.Store(ctx, snapIDIndexStoreKey, snapIDIdxRaw)
}

// loadSnapIDIndex loads the Checker's snapshot ID index from the metadata store.
func (e *Engine) loadSnapIDIndex(ctx context.Context) error {
	b, err := e.MetaStore.Load(ctx, snapIDIndexStoreKey)
	if err != nil {
		if errors.Is(err, robustness.ErrKeyNotFound) {
			// Swallow key-not-found error. We may not have historical
			// index data.
			return nil
		}

		return err
	}

	return json.Unmarshal(b, &e.Checker.SnapIDIndex)
}
