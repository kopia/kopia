package main

import (
	"encoding/json"
	"fmt"

	"github.com/kopia/kopia/fs/repofs"
	"github.com/kopia/kopia/vault"
)

func loadBackupManifest(vlt *vault.Vault, manifestID string) (*repofs.Snapshot, error) {
	b, err := vlt.Get(manifestID)
	if err != nil {
		return nil, fmt.Errorf("error loading previous backup: %v", err)
	}

	var m repofs.Snapshot
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("invalid previous backup manifest: %v", err)
	}

	return &m, nil
}

func saveBackupManifest(vlt *vault.Vault, manifestID string, m *repofs.Snapshot) error {
	b, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("cannot marshal backup manifest to JSON: %v", err)
	}

	return vlt.Put(manifestID, b)
}
