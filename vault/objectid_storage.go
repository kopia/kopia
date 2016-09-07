package vault

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/kopia/kopia/repo"
)

const (
	// StoredObjectIDPrefix is the name prefix for all vault items storing repository object IDs.
	StoredObjectIDPrefix = "v"

	storedObjectIDLengthBytes = 8
)

// SaveObjectID stores the given object ID in an encrypted vault item and returns a unique ID.
func (vlt *Vault) SaveObjectID(oid repo.ObjectID) (string, error) {
	h := hmac.New(sha256.New, vlt.format.UniqueID)
	bytes, err := json.Marshal(&oid)
	if err != nil {
		return "", err
	}
	h.Write(bytes)
	sum := h.Sum(nil)
	for i := storedObjectIDLengthBytes; i < len(sum); i++ {
		sum[i%storedObjectIDLengthBytes] ^= sum[i]
	}
	sum = sum[0:storedObjectIDLengthBytes]
	key := StoredObjectIDPrefix + hex.EncodeToString(sum)

	if err := vlt.Put(key, bytes); err != nil {
		return "", err
	}

	return key, nil
}

// GetObjectID retrieves stored object ID from the vault item with a given ID and return it.
func (vlt *Vault) GetObjectID(id string) (repo.ObjectID, error) {
	matches, err := vlt.List(id)
	if err != nil {
		return repo.NullObjectID, err
	}

	switch len(matches) {
	case 0:
		return repo.NullObjectID, ErrItemNotFound
	case 1:
		b, err := vlt.Get(matches[0])
		if err != nil {
			return repo.NullObjectID, err
		}

		var oid repo.ObjectID
		if err := json.Unmarshal(b, &oid); err != nil {
			return repo.NullObjectID, err
		}

		return oid, nil

	default:
		return repo.NullObjectID, fmt.Errorf("ambiguous object ID: %v", id)
	}
}
