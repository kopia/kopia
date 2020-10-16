// +build darwin,amd64 linux,amd64

package engine

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/kopia/kopia/tests/robustness/snapmeta"
)

const (
	engineStatsStoreKey = "cumulative-engine-stats"
	engineLogsStoreKey  = "engine-logs"
)

// SaveLog saves the engine Log in the metadata store.
func (e *Engine) SaveLog() error {
	b, err := json.Marshal(e.EngineLog)
	if err != nil {
		return err
	}

	return e.MetaStore.Store(engineLogsStoreKey, b)
}

// LoadLog loads the engine log from the metadata store.
func (e *Engine) LoadLog() error {
	b, err := e.MetaStore.Load(engineLogsStoreKey)
	if err != nil {
		if errors.Is(err, snapmeta.ErrKeyNotFound) {
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

// SaveStats saves the engine Stats in the metadata store.
func (e *Engine) SaveStats() error {
	cumulStatRaw, err := json.Marshal(e.CumulativeStats)
	if err != nil {
		return err
	}

	return e.MetaStore.Store(engineStatsStoreKey, cumulStatRaw)
}

// LoadStats loads the engine Stats from the metadata store.
func (e *Engine) LoadStats() error {
	b, err := e.MetaStore.Load(engineStatsStoreKey)
	if err != nil {
		if errors.Is(err, snapmeta.ErrKeyNotFound) {
			// Swallow key-not-found error. We may not have historical
			// stats data. Initialize the action map for the cumulative stats
			e.CumulativeStats.PerActionStats = make(map[ActionKey]*ActionStats)
			e.CumulativeStats.CreationTime = time.Now()

			return nil
		}

		return err
	}

	return json.Unmarshal(b, &e.CumulativeStats)
}
