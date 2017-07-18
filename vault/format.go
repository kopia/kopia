package vault

// VaultFormat describes the format of a Vault.
// Contents of this structure are serialized in plain text in the Vault storage.
type VaultFormat struct {
	Version             string `json:"version"`
	UniqueID            []byte `json:"uniqueID"`
	KeyAlgorithm        string `json:"keyAlgo"`
	EncryptionAlgorithm string `json:"encryption"`
}
