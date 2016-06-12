package vault

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/kopia/kopia/repo"
)

const (
	// StoredObjectIDPrefix is the name prefix for all vault items storing repository object IDs.
	StoredObjectIDPrefix = "v"

	storedObjectIDLengthBytes = 8
)

type objectIDData struct {
	ObjectID string `json:"objectID"`
}

// SaveObjectID stores the given object ID in an encrypted vault item and returns a unique ID.
func (vlt *Vault) SaveObjectID(oid repo.ObjectID) (string, error) {
	h := hmac.New(sha256.New, vlt.format.UniqueID)
	h.Write([]byte(oid))
	sum := h.Sum(nil)
	for i := storedObjectIDLengthBytes; i < len(sum); i++ {
		sum[i%storedObjectIDLengthBytes] ^= sum[i]
	}
	sum = sum[0:storedObjectIDLengthBytes]
	key := StoredObjectIDPrefix + hex.EncodeToString(sum)

	var d objectIDData
	d.ObjectID = string(oid)

	if err := vlt.putJSON(key, &d); err != nil {
		return "", err
	}

	return key, nil
}

// GetObjectID retrieves stored object ID from the vault item with a given ID and return it.
func (vlt *Vault) GetObjectID(id string) (repo.ObjectID, error) {
	matches, err := vlt.List(id)
	if err != nil {
		return "", err
	}

	switch len(matches) {
	case 0:
		return "", ErrItemNotFound
	case 1:
		var d objectIDData
		if err := vlt.getJSON(matches[0], &d); err != nil {
			return "", err
		}
		return repo.ParseObjectID(d.ObjectID)

	default:
		return "", fmt.Errorf("ambiguous object ID: %v", id)
	}
}
