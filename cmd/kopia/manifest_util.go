package main

import (
	"encoding/json"
	"fmt"

	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/vault"
)

func saveBackupManifest(vlt *vault.Vault, manifestID string, m *snapshot.Manifest) error {
	b, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("cannot marshal backup manifest to JSON: %v", err)
	}

	return vlt.Put(manifestID, b)
}
