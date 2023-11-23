package repo

import (
	"encoding/base64"
	"encoding/json"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
)

type tokenInfo struct {
	Version  string              `json:"version"`
	Storage  blob.ConnectionInfo `json:"storage"`
	Password string              `json:"password,omitempty"`
}

// Token returns an opaque token that contains repository connection information
// and optionally the provided password.
func (r *directRepository) Token(password string) (string, error) {
	return EncodeToken(password, r.blobs.ConnectionInfo())
}

// EncodeToken returns an opaque token that contains the given connection information
// and optionally the provided password.
func EncodeToken(password string, ci blob.ConnectionInfo) (string, error) {
	ti := &tokenInfo{
		Version:  "1",
		Storage:  ci,
		Password: password,
	}

	v, err := json.Marshal(ti)
	if err != nil {
		return "", errors.Wrap(err, "marshal token")
	}

	return base64.RawURLEncoding.EncodeToString(v), nil
}

// DecodeToken decodes the provided token and returns connection info and password if persisted.
func DecodeToken(token string) (blob.ConnectionInfo, string, error) {
	t := &tokenInfo{}

	v, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return blob.ConnectionInfo{}, "", errors.New("unable to decode token")
	}

	if err := json.Unmarshal(v, t); err != nil {
		return blob.ConnectionInfo{}, "", errors.New("unable to decode token")
	}

	if t.Version != "1" {
		return blob.ConnectionInfo{}, "", errors.New("unsupported token version")
	}

	return t.Storage, t.Password, nil
}
